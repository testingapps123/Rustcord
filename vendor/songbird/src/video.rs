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

// ---------- cameras (video on the voice connection) ----------

/// camera video SSRC -> user id, from the voice server's op 12 messages
static CAMERAS: Mutex<Option<std::collections::HashMap<u32, u64>>> = Mutex::new(None);

pub(crate) fn set_camera(user: u64, video_ssrc: u32) {
    let mut g = CAMERAS.lock().unwrap();
    let m = g.get_or_insert_with(Default::default);
    m.retain(|_, u| *u != user);
    if video_ssrc != 0 {
        m.insert(video_ssrc, user);
    }
}

pub(crate) fn remove_camera_user(user: u64) {
    if let Some(m) = CAMERAS.lock().unwrap().as_mut() {
        m.retain(|_, u| *u != user);
    }
}

/// Whose camera is this video SSRC? (None: it's screen-share video)
pub fn camera_user(ssrc: u32) -> Option<u64> {
    CAMERAS.lock().unwrap().as_ref().and_then(|m| m.get(&ssrc).copied())
}

/// Our audio SSRC on the voice connection (camera video goes out as +1).
static VOICE_SSRC: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

pub(crate) fn set_voice_ssrc(ssrc: u32) {
    VOICE_SSRC.store(ssrc, std::sync::atomic::Ordering::Relaxed);
}

pub fn voice_ssrc() -> u32 {
    VOICE_SSRC.load(std::sync::atomic::Ordering::Relaxed)
}

/// Does this user have their camera on?
pub fn has_camera(user: u64) -> bool {
    CAMERAS.lock().unwrap().as_ref().map_or(false, |m| m.values().any(|u| *u == user))
}

pub fn clear_cameras() {
    *CAMERAS.lock().unwrap() = None;
}

static OUR_SSRC: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
/// media SSRC -> (wanted, when last sent)
static KEYFRAME: Mutex<Option<std::collections::HashMap<u32, (bool, Option<std::time::Instant>)>>> = Mutex::new(None);

pub(crate) fn set_our_ssrc(ssrc: u32) {
    OUR_SSRC.store(ssrc, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn our_ssrc() -> u32 {
    OUR_SSRC.load(std::sync::atomic::Ordering::Relaxed)
}

/// Ask the sender of `media_ssrc` for a full picture (RTCP PLI, sent on the connection that
/// carries that video when its next packet arrives).
pub fn request_keyframe(media_ssrc: u32) {
    KEYFRAME.lock().unwrap().get_or_insert_with(Default::default).entry(media_ssrc).or_insert((false, None)).0 = true;
}

/// Should a PLI for `ssrc` go out now? At most one every 700 ms per video.
pub(crate) fn take_keyframe_request(ssrc: u32) -> bool {
    let mut g = KEYFRAME.lock().unwrap();
    let Some(e) = g.as_mut().and_then(|m| m.get_mut(&ssrc)) else { return false };
    if e.0 && e.1.map_or(true, |t| t.elapsed() >= std::time::Duration::from_millis(700)) {
        *e = (false, Some(std::time::Instant::now()));
        return true;
    }
    false
}

// ---------- Drover-style proxy for the voice websocket ----------

#[derive(Clone)]
pub struct ProxyCfg {
    pub socks: bool,
    pub host: String,
    pub port: u16,
}

static PROXY: Mutex<Option<ProxyCfg>> = Mutex::new(None);

/// Route voice-server websockets through an HTTP (CONNECT) or SOCKS5 proxy.
pub fn set_proxy(socks: bool, host: &str, port: u16) {
    *PROXY.lock().unwrap() = Some(ProxyCfg { socks, host: host.to_string(), port });
}

pub(crate) fn proxy() -> Option<ProxyCfg> {
    PROXY.lock().unwrap().clone()
}

pub(crate) async fn proxy_connect(p: &ProxyCfg, host: &str, port: u16) -> std::io::Result<tokio::net::TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let bad = |m: String| std::io::Error::new(std::io::ErrorKind::Other, m);
    let mut s = tokio::net::TcpStream::connect((p.host.as_str(), p.port)).await?;
    if p.socks {
        s.write_all(&[5, 1, 0]).await?;
        let mut r = [0u8; 2];
        s.read_exact(&mut r).await?;
        if r != [5, 0] {
            return Err(bad("SOCKS5 proxy needs a login".into()));
        }
        let mut req = vec![5, 1, 0, 3, host.len() as u8];
        req.extend_from_slice(host.as_bytes());
        req.extend_from_slice(&port.to_be_bytes());
        s.write_all(&req).await?;
        let mut head = [0u8; 4];
        s.read_exact(&mut head).await?;
        if head[1] != 0 {
            return Err(bad(format!("SOCKS5 proxy refused (code {})", head[1])));
        }
        let skip = match head[3] {
            1 => 4,
            4 => 16,
            3 => {
                let mut l = [0u8; 1];
                s.read_exact(&mut l).await?;
                l[0] as usize
            },
            _ => return Err(bad("bad SOCKS5 reply".into())),
        };
        let mut rest = vec![0u8; skip + 2];
        s.read_exact(&mut rest).await?;
    } else {
        s.write_all(format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n").as_bytes()).await?;
        let mut reply = Vec::new();
        let mut b = [0u8; 1];
        while !reply.ends_with(b"\r\n\r\n") {
            if s.read(&mut b).await? == 0 || reply.len() > 8192 {
                return Err(bad("proxy closed the connection".into()));
            }
            reply.push(b[0]);
        }
        let head = String::from_utf8_lossy(&reply);
        if head.split_whitespace().nth(1) != Some("200") {
            return Err(bad(format!("proxy refused: {}", head.lines().next().unwrap_or(""))));
        }
    }
    Ok(s)
}

// ---------- going live (sending our own screen) ----------

/// One encoded H.264 frame (Annex-B) to send, with its 90 kHz RTP timestamp.
pub struct OutFrame {
    pub annexb: Vec<u8>,
    pub ts90k: u32,
}

static SENDING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static FORMAT: Mutex<(u32, u32, u32, u32)> = Mutex::new((1280, 720, 30, 2_500_000));
static OUT: Mutex<Option<std::sync::mpsc::SyncSender<OutFrame>>> = Mutex::new(None);
static WANT_KEYFRAME: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Largest RTP payload we send (keeps packets under typical MTU).
const MAX_PAYLOAD: usize = 1100;

/// Call before connecting the stream: we are the streamer, with this format.
pub fn set_sending(on: bool, width: u32, height: u32, fps: u32, bitrate: u32) {
    *FORMAT.lock().unwrap() = (width, height, fps, bitrate);
    SENDING.store(on, std::sync::atomic::Ordering::Relaxed);
    if !on {
        *OUT.lock().unwrap() = None;
    }
}

pub fn is_sending() -> bool {
    SENDING.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn send_format() -> (u32, u32, u32) {
    let f = *FORMAT.lock().unwrap();
    (f.0, f.1, f.2)
}

pub(crate) fn send_bitrate() -> u32 {
    FORMAT.lock().unwrap().3
}

pub(crate) fn keyframe_wanted() {
    WANT_KEYFRAME.store(true, std::sync::atomic::Ordering::Relaxed);
    // (the request's target SSRC is encrypted, so ask both our video senders)
    CAM_WANT_KEYFRAME.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Did a viewer ask for a full picture since we last checked?
pub fn take_keyframe_wanted() -> bool {
    WANT_KEYFRAME.swap(false, std::sync::atomic::Ordering::Relaxed)
}

/// Is the stream connection ready to take frames?
pub fn sender_ready() -> bool {
    OUT.lock().unwrap().is_some()
}

/// Queue a frame (dropped if the sender is busy or not connected yet).
pub fn send_frame(frame: OutFrame) -> bool {
    match OUT.lock().unwrap().as_ref() {
        Some(tx) => tx.try_send(frame).is_ok(),
        None => false,
    }
}

// ---------- our camera (video on the voice connection) ----------

static CAM_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static CAM_FORMAT: Mutex<(u32, u32, u32, u32)> = Mutex::new((1280, 720, 30, 2_500_000));
static CAM_OUT: Mutex<Option<std::sync::mpsc::SyncSender<OutFrame>>> = Mutex::new(None);
static CAM_WANT_KEYFRAME: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static VOICE_WS: Mutex<Option<flume::Sender<crate::driver::tasks::message::WsMessage>>> = Mutex::new(None);

pub(crate) fn set_voice_ws(tx: flume::Sender<crate::driver::tasks::message::WsMessage>) {
    *VOICE_WS.lock().unwrap() = Some(tx);
}

/// Tell the voice server whether our camera is on (op 12), like the official client.
pub(crate) fn announce_camera() {
    let ssrc = voice_ssrc();
    if ssrc == 0 {
        return;
    }
    let on = CAM_ON.load(std::sync::atomic::Ordering::Relaxed);
    let (w, h, fps, bitrate) = *CAM_FORMAT.lock().unwrap();
    let msg = serde_json::json!({ "op": 12, "d": {
        "audio_ssrc": ssrc,
        "video_ssrc": if on { ssrc + 1 } else { 0 },
        "rtx_ssrc": if on { ssrc + 2 } else { 0 },
        "streams": [{
            "type": "video", "rid": "100", "ssrc": ssrc + 1, "active": on, "quality": 100,
            "rtx_ssrc": ssrc + 2, "max_bitrate": bitrate, "max_framerate": fps,
            "max_resolution": { "type": "fixed", "width": w, "height": h },
        }],
    }});
    if let Some(tx) = VOICE_WS.lock().unwrap().as_ref() {
        let _ = tx.send(crate::driver::tasks::message::WsMessage::SendRaw(msg));
    }
}

/// Turn our camera on/off in the call (the app also sets self_video in its voice state).
pub fn set_camera_on(on: bool, width: u32, height: u32, fps: u32, bitrate: u32) {
    *CAM_FORMAT.lock().unwrap() = (width, height, fps, bitrate);
    CAM_ON.store(on, std::sync::atomic::Ordering::Relaxed);
    if on {
        CAM_WANT_KEYFRAME.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    announce_camera();
}

/// Can camera frames be sent right now?
pub fn camera_ready() -> bool {
    CAM_ON.load(std::sync::atomic::Ordering::Relaxed) && CAM_OUT.lock().unwrap().is_some()
}

pub fn camera_send_frame(frame: OutFrame) -> bool {
    match CAM_OUT.lock().unwrap().as_ref() {
        Some(tx) => tx.try_send(frame).is_ok(),
        None => false,
    }
}

/// Did someone ask for a full picture of our camera?
pub fn camera_take_keyframe() -> bool {
    CAM_WANT_KEYFRAME.swap(false, std::sync::atomic::Ordering::Relaxed)
}

/// Split Annex-B into NAL units (without start codes).
fn split_nals(data: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= data.len() {
        if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
            starts.push(i + 3);
            i += 3;
        } else {
            i += 1;
        }
    }
    let mut out = Vec::new();
    for (k, &s) in starts.iter().enumerate() {
        let mut e = starts.get(k + 1).map_or(data.len(), |n| n - 3);
        while e > s && data[e - 1] == 0 && starts.get(k + 1).is_some() {
            e -= 1; // trailing zero of a 4-byte start code
        }
        if e > s {
            out.push(&data[s..e]);
        }
    }
    out
}

/// RFC 6184 packetisation: single NAL units, or FU-A fragments for big ones.
fn packetize(nals: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    for nal in nals {
        if nal.len() <= MAX_PAYLOAD {
            packets.push(nal.to_vec());
            continue;
        }
        let header = nal[0];
        let indicator = (header & 0xE0) | 28;
        let body = &nal[1..];
        let chunks: Vec<&[u8]> = body.chunks(MAX_PAYLOAD - 2).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let mut fu = header & 0x1F;
            if i == 0 {
                fu |= 0x80;
            }
            if i == chunks.len() - 1 {
                fu |= 0x40;
            }
            let mut p = Vec::with_capacity(chunk.len() + 2);
            p.push(indicator);
            p.push(fu);
            p.extend_from_slice(chunk);
            packets.push(p);
        }
    }
    packets
}

pub(crate) fn start_sender(
    sock: std::net::UdpSocket,
    cipher: crate::driver::crypto::Cipher,
    video_ssrc: u32,
    dave: Arc<RwLock<Option<davey::DaveSession>>>,
    dave_version: Arc<std::sync::atomic::AtomicU16>,
    camera: bool,
) {
    let (tx, rx) = std::sync::mpsc::sync_channel::<OutFrame>(2);
    if camera {
        *CAM_OUT.lock().unwrap() = Some(tx);
    } else {
        *OUT.lock().unwrap() = Some(tx);
    }
    keyframe_wanted();
    let label = if camera { "camera" } else { "go live" };
    std::thread::spawn(move || {
        let mut seq: u16 = rand::random();
        let mut counter: u32 = rand::random();
        let (mut frames, mut dave_skips, mut started) = (0u64, 0u64, std::time::Instant::now());
        for f in rx {
            if started.elapsed() >= std::time::Duration::from_secs(5) {
                tracing::info!(
                    "[rustcord] {label}: sent {frames} frames, {dave_skips} held back (end-to-end encryption not ready: nobody else in the call yet?)"
                );
                frames = 0;
                dave_skips = 0;
                started = std::time::Instant::now();
            }
            // DAVE end-to-end encryption of the whole frame (keeps NAL headers readable).
            // Version 0 = the call isn't end-to-end encrypted: send as is, like the official client.
            let e2ee = dave_version.load(std::sync::atomic::Ordering::Relaxed) != 0;
            let payload: Vec<u8> = {
                let mut guard = dave.write().unwrap();
                match guard.as_mut() {
                    Some(s) if e2ee && s.is_ready() => match s.encrypt(davey::MediaType::VIDEO, davey::Codec::H264, &f.annexb) {
                        Ok(c) => c.into_owned(),
                        Err(_) => {
                            dave_skips += 1;
                            continue;
                        },
                    },
                    Some(_) if e2ee => {
                        dave_skips += 1; // group not ready yet
                        continue;
                    },
                    _ => f.annexb.clone(),
                }
            };
            let nals = split_nals(&payload);
            let packets = packetize(&nals);
            let last = packets.len().saturating_sub(1);
            for (i, p) in packets.iter().enumerate() {
                let mut header = [0u8; 12];
                header[0] = 0x80;
                header[1] = H264_PT | if i == last { 0x80 } else { 0 };
                header[2..4].copy_from_slice(&seq.to_be_bytes());
                header[4..8].copy_from_slice(&f.ts90k.to_be_bytes());
                header[8..12].copy_from_slice(&video_ssrc.to_be_bytes());
                seq = seq.wrapping_add(1);
                counter = counter.wrapping_add(1);
                if let Ok(pkt) = cipher.seal_rtcp(&header, p, counter) {
                    let _ = sock.send(&pkt);
                }
                // Pace big frames (key frames can be 100+ packets): sending them in one burst
                // makes routers/VPNs drop packets, which shows up as stutter for viewers.
                if packets.len() > 12 && i % 8 == 7 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
            frames += 1;
        }
        tracing::info!("[rustcord] {label}: sender stopped");
    });
}

pub(crate) fn deliver(pkt: RawVideo) {
    if let Some(tx) = SINK.lock().unwrap().as_ref() {
        let _ = tx.send(pkt);
    }
}
