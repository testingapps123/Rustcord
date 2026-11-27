// Screen share (Go Live) viewer: RTP video packets -> H.264 frames -> DAVE decrypt -> decode -> picture.
//
// The patched songbird (vendor/songbird/src/video.rs) hands us every transport-decrypted video
// packet from a stream connection. Here we put the packets of one frame back together
// (RFC 6184 H.264 packetisation), remove DAVE end-to-end encryption, decode with OpenH264,
// and publish the newest picture for the UI.
//
// Smoothness:
// * we ask the streamer for a full picture (RTCP PLI) when a stream starts, when we switch,
//   and whenever packets go missing, so the picture appears/recovers quickly;
// * every frame is decoded (later frames depend on it) but frames that arrive late are not
//   converted or shown, so a slow moment never turns into growing delay.

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

/// (width, height, frames per second) of what we're showing, for the "720p 30FPS" badge.
static STATS: Mutex<(usize, usize, f32)> = Mutex::new((0, 0, 0.0));

pub fn stats() -> (usize, usize, f32) {
    *STATS.lock().unwrap()
}

const START_CODE: [u8; 4] = [0, 0, 0, 1];
const DAVE_MAGIC: [u8; 2] = [0xFA, 0xFA];
/// Show a frame only if it is less than this far behind real time.
const MAX_LATE: f64 = 0.25;
/// Don't convert more often than this (about 60 fps).
const MIN_GAP: Duration = Duration::from_millis(15);

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

/// Does this Annex-B frame contain a key frame (IDR slice)?
fn has_idr(frame: &[u8]) -> bool {
    frame.windows(5).any(|w| w[..4] == START_CODE && (w[4] & 0x1F) == 5)
}

struct Watch {
    ssrc: u32,
    decoder: Option<Decoder>,
    have_picture: bool,
    /// RTP timestamp (90 kHz) and wall time of the reference point for lateness
    base: Option<(u32, Instant)>,
}

impl Watch {
    fn new(ssrc: u32) -> Self {
        songbird::video::request_keyframe(ssrc);
        Self { ssrc, decoder: Decoder::new().ok(), have_picture: false, base: None }
    }

    /// Seconds this frame is behind real time.
    fn lateness(&mut self, ts: u32) -> f64 {
        let now = Instant::now();
        let (bts, bt) = *self.base.get_or_insert((ts, now));
        let media = ts.wrapping_sub(bts) as i32 as f64 / 90_000.0;
        let wall = now.duration_since(bt).as_secs_f64();
        let late = wall - media;
        if late < 0.0 {
            // frame is "early": move the reference so lateness stays meaningful
            self.base = Some((ts, now));
            return 0.0;
        }
        late
    }
}

fn run(rx: Receiver<RawVideo>, slot: FrameSlot, streamer: Streamer, ctx: egui::Context) {
    let mut watch: Option<Watch> = None;
    let mut frame_ts: Option<u32> = None;
    let mut parts: Vec<(u16, Vec<u8>)> = Vec::new();
    let mut pt_counts: HashMap<u8, u32> = HashMap::new();
    let (mut frames, mut shown, mut skipped, mut dave_fail, mut gaps) = (0u32, 0u32, 0u32, 0u32, 0u32);
    let mut last_report = Instant::now();
    let mut last_shown = Instant::now() - MIN_GAP;
    let mut fps_window: (Instant, u32) = (Instant::now(), 0);
    let mut last_streamer: Option<u64> = None;

    for pkt in rx {
        *pt_counts.entry(pkt.payload_type).or_default() += 1;
        if last_report.elapsed() > Duration::from_secs(5) {
            tracing::info!(
                "stream: packets {pt_counts:?}; frames {frames}, shown {shown}, skipped (late) {skipped}, dave failures {dave_fail}, incomplete {gaps}"
            );
            last_report = Instant::now();
        }

        // Lock onto the busiest video payload type (retransmissions are rarer).
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

        // New stream (or switched to someone else): start fresh and ask for a full picture.
        let who = *streamer.lock().unwrap();
        if watch.as_ref().map_or(true, |w| w.ssrc != pkt.ssrc) || who != last_streamer {
            tracing::info!("stream: now decoding SSRC {} (payload type {video_pt})", pkt.ssrc);
            watch = Some(Watch::new(pkt.ssrc));
            last_streamer = who;
            parts.clear();
            frame_ts = None;
            *STATS.lock().unwrap() = (0, 0, 0.0);
        }
        let w = watch.as_mut().unwrap();

        if frame_ts != Some(pkt.timestamp) {
            if !parts.is_empty() {
                gaps += 1; // previous frame never got its last packet
                songbird::video::request_keyframe(w.ssrc);
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
        let complete = parts.windows(2).all(|p| p[1].0 == p[0].0.wrapping_add(1));
        let payloads: Vec<Vec<u8>> = parts.drain(..).map(|(_, b)| b).collect();
        frame_ts = None;
        frames += 1;
        let Some(frame) = complete.then(|| depacketize(&payloads)).flatten() else {
            gaps += 1;
            songbird::video::request_keyframe(w.ssrc);
            continue;
        };

        // Remove DAVE end-to-end encryption if the frame carries it.
        let plain = if frame.ends_with(&DAVE_MAGIC) {
            let Some(uid) = who else { continue };
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

        // Until the first full picture arrives, P-frames can't be decoded: wait for a key frame.
        if !w.have_picture && !has_idr(&plain) {
            songbird::video::request_keyframe(w.ssrc);
            continue;
        }
        let late = w.lateness(pkt.timestamp);
        let Some(dec) = w.decoder.as_mut() else { continue };
        match dec.decode(&plain) {
            Ok(Some(yuv)) => {
                w.have_picture = true;
                // Decode always; only convert + show when we're keeping up.
                if late > MAX_LATE || last_shown.elapsed() < MIN_GAP {
                    skipped += 1;
                    continue;
                }
                let (width, height) = yuv.dimensions();
                let mut rgba = vec![0u8; width * height * 4];
                yuv.write_rgba8(&mut rgba);
                *slot.lock().unwrap() = Some(egui::ColorImage::from_rgba_unmultiplied([width, height], &rgba));
                last_shown = Instant::now();
                shown += 1;
                fps_window.1 += 1;
                let secs = fps_window.0.elapsed().as_secs_f32();
                if secs >= 1.0 {
                    *STATS.lock().unwrap() = (width, height, fps_window.1 as f32 / secs);
                    fps_window = (Instant::now(), 0);
                } else if STATS.lock().unwrap().0 == 0 {
                    *STATS.lock().unwrap() = (width, height, 0.0);
                }
                ctx.request_repaint();
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!("stream: decode error ({e}), asking for a key frame");
                w.have_picture = false;
                w.decoder = Decoder::new().ok();
                songbird::video::request_keyframe(w.ssrc);
            }
        }
    }
}
