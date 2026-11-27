// Screen share (Go Live) viewer: RTP video packets -> H.264 frames -> DAVE decrypt -> decode -> picture.
//
// The patched songbird (vendor/songbird/src/video.rs) hands us every transport-decrypted video
// packet from a stream connection. Here we put the packets of one frame back together
// (RFC 6184 H.264 packetisation), remove DAVE end-to-end encryption, decode with OpenH264,
// and publish the newest picture for the UI.

use eframe::egui;
use openh264::decoder::Decoder;
use openh264::formats::YUVSource;
use songbird::davey::MediaType;
use songbird::video::{RawVideo, H264_PT};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type FrameSlot = Arc<Mutex<Option<egui::ColorImage>>>;
/// User id of the person whose stream we're watching (needed for DAVE decryption).
pub type Streamer = Arc<Mutex<Option<u64>>>;

const START_CODE: [u8; 4] = [0, 0, 0, 1];
const DAVE_MAGIC: [u8; 2] = [0xFA, 0xFA];

pub fn spawn(slot: FrameSlot, streamer: Streamer, ctx: egui::Context) {
    let (tx, rx) = channel();
    songbird::video::set_sink(tx);
    std::thread::spawn(move || run(rx, slot, streamer, ctx));
}

/// Rebuild an Annex-B H.264 frame from its RTP payloads (single NAL, STAP-A, FU-A).
fn depacketize(payloads: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for p in payloads {
        if p.is_empty() {
            continue;
        }
        match p[0] & 0x1F {
            1..=23 => {
                out.extend_from_slice(&START_CODE);
                out.extend_from_slice(p);
            }
            24 => {
                let mut i = 1;
                while i + 2 <= p.len() {
                    let size = u16::from_be_bytes([p[i], p[i + 1]]) as usize;
                    i += 2;
                    if i + size > p.len() {
                        return None;
                    }
                    out.extend_from_slice(&START_CODE);
                    out.extend_from_slice(&p[i..i + size]);
                    i += size;
                }
            }
            28 => {
                if p.len() < 2 {
                    return None;
                }
                let (indicator, header) = (p[0], p[1]);
                if header & 0x80 != 0 {
                    out.extend_from_slice(&START_CODE);
                    out.push((indicator & 0xE0) | (header & 0x1F));
                }
                out.extend_from_slice(&p[2..]);
            }
            _ => return None,
        }
    }
    (!out.is_empty()).then_some(out)
}

fn run(rx: Receiver<RawVideo>, slot: FrameSlot, streamer: Streamer, ctx: egui::Context) {
    let mut decoder = Decoder::new().ok();
    let mut frame_ts: Option<u32> = None;
    let mut parts: Vec<(u16, Vec<u8>)> = Vec::new();
    let mut pt_counts: HashMap<u8, u32> = HashMap::new();
    let (mut frames, mut decoded, mut dave_fail, mut gaps) = (0u32, 0u32, 0u32, 0u32);
    let mut last_report = Instant::now();

    for pkt in rx {
        *pt_counts.entry(pkt.payload_type).or_default() += 1;
        if last_report.elapsed() > Duration::from_secs(5) {
            tracing::info!(
                "stream: packets by type {pt_counts:?}; frames {frames}, shown {decoded}, dave failures {dave_fail}, incomplete {gaps}"
            );
            last_report = Instant::now();
        }
        // Lock onto the busiest video payload type (retransmissions are rarer); prefer the
        // one we asked for when it's present.
        let video_pt = if pt_counts.contains_key(&H264_PT) {
            H264_PT
        } else {
            match pt_counts.iter().filter(|(pt, _)| **pt != songbird::video::OPUS_PT).max_by_key(|(_, c)| **c) {
                Some((pt, _)) => *pt,
                None => continue,
            }
        };
        if pkt.payload_type != video_pt {
            continue;
        }
        if frames == 0 && parts.is_empty() {
            tracing::info!("stream: using video payload type {video_pt}, first byte {:02x?}", pkt.body.first());
        }
        if frame_ts != Some(pkt.timestamp) {
            if !parts.is_empty() {
                gaps += 1; // previous frame never got its last packet
            }
            parts.clear();
            frame_ts = Some(pkt.timestamp);
        }
        parts.push((pkt.seq, pkt.body));
        if !pkt.marker {
            continue;
        }

        // Last packet of the frame: order by sequence number and check nothing is missing.
        let first = parts[0].0;
        parts.sort_by_key(|(seq, _)| seq.wrapping_sub(first) as i16);
        let complete = parts.windows(2).all(|w| w[1].0 == w[0].0.wrapping_add(1));
        let payloads: Vec<Vec<u8>> = parts.drain(..).map(|(_, b)| b).collect();
        frame_ts = None;
        frames += 1;
        if !complete {
            gaps += 1;
            continue;
        }
        let Some(frame) = depacketize(&payloads) else {
            gaps += 1;
            continue;
        };

        // Remove DAVE end-to-end encryption if the frame carries it.
        let plain = if frame.ends_with(&DAVE_MAGIC) {
            let Some(uid) = *streamer.lock().unwrap() else { continue };
            let mut guard = pkt.dave.write().unwrap();
            let Some(session) = guard.as_mut() else {
                dave_fail += 1;
                continue;
            };
            match session.decrypt(uid, MediaType::VIDEO, &frame) {
                Ok(p) => p,
                Err(e) => {
                    dave_fail += 1;
                    if dave_fail <= 3 {
                        tracing::warn!("stream: DAVE video decrypt failed: {e:?}");
                    }
                    continue;
                }
            }
        } else {
            frame
        };

        let Some(dec) = decoder.as_mut() else { continue };
        match dec.decode(&plain) {
            Ok(Some(yuv)) => {
                let (w, h) = yuv.dimensions();
                let mut rgba = vec![0u8; w * h * 4];
                yuv.write_rgba8(&mut rgba);
                *slot.lock().unwrap() = Some(egui::ColorImage::from_rgba_unmultiplied([w, h], &rgba));
                decoded += 1;
                ctx.request_repaint();
            }
            Ok(None) => {}
            Err(e) => {
                if decoded == 0 && frames % 30 == 1 {
                    tracing::debug!("stream: waiting for a key frame ({e})");
                }
            }
        }
    }
}
