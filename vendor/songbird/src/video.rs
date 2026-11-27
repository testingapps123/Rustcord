//! Rustcord addition: hand video RTP packets from Go Live (screen share) connections to the app.
//!
//! Stream connections are marked by their RTC server id (passed as the "guild id" when
//! connecting). For those, the identify/select-protocol messages announce video support,
//! and every non-Opus RTP packet is transport-decrypted and passed to `SINK` together with
//! the connection's DAVE session (DAVE video decryption happens per frame, after depacketising).

#![allow(missing_docs)]

use std::sync::{mpsc::Sender, Arc, Mutex, RwLock};

/// Discord's RTP payload type for Opus audio.
pub const OPUS_PT: u8 = 120;
/// Payload type we ask for H.264 video (RTX = +1).
pub const H264_PT: u8 = 101;

pub struct RawVideo {
    pub ssrc: u32,
    pub seq: u16,
    pub timestamp: u32,
    pub marker: bool,
    pub payload_type: u8,
    pub body: Vec<u8>,
    pub dave: Arc<RwLock<Option<davey::DaveSession>>>,
}

static SINK: Mutex<Option<Sender<RawVideo>>> = Mutex::new(None);
static STREAM_SERVERS: Mutex<Vec<u64>> = Mutex::new(Vec::new());

pub fn set_sink(tx: Sender<RawVideo>) {
    *SINK.lock().unwrap() = Some(tx);
}

pub fn add_stream_server(id: u64) {
    let mut v = STREAM_SERVERS.lock().unwrap();
    if !v.contains(&id) {
        v.push(id);
    }
}

pub fn remove_stream_server(id: u64) {
    STREAM_SERVERS.lock().unwrap().retain(|x| *x != id);
}

pub fn is_stream_server(id: u64) -> bool {
    STREAM_SERVERS.lock().unwrap().contains(&id)
}

static OUR_SSRC: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
/// (media SSRC, when last sent)
static KEYFRAME: Mutex<(Option<u32>, Option<std::time::Instant>)> = Mutex::new((None, None));

pub(crate) fn set_our_ssrc(ssrc: u32) {
    OUR_SSRC.store(ssrc, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn our_ssrc() -> u32 {
    OUR_SSRC.load(std::sync::atomic::Ordering::Relaxed)
}

/// Ask the streamer for a full picture (sent as RTCP PLI with the next received packet).
pub fn request_keyframe(media_ssrc: u32) {
    KEYFRAME.lock().unwrap().0 = Some(media_ssrc);
}

/// At most one request every 700 ms.
pub(crate) fn take_keyframe_request() -> Option<u32> {
    let mut k = KEYFRAME.lock().unwrap();
    let due = k.1.map_or(true, |t| t.elapsed() >= std::time::Duration::from_millis(700));
    if due && k.0.is_some() {
        k.1 = Some(std::time::Instant::now());
        return k.0.take();
    }
    None
}

pub(crate) fn deliver(pkt: RawVideo) {
    if let Some(tx) = SINK.lock().unwrap().as_ref() {
        let _ = tx.send(pkt);
    }
}
