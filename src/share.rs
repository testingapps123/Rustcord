// Sharing your own screen (Go Live): capture -> scale -> H.264 encode -> patched songbird sender.
//
// Whole screens use xcap's DXGI recorder (efficient); single windows are grabbed per frame.
// Frames go to `songbird::video::send_frame`, which DAVE-encrypts, packetises and sends them
// on the stream connection. A key frame is made at the start and whenever a viewer asks.

use eframe::egui;
use openh264::encoder::{BitRate, Complexity, Encoder, EncoderConfig, FrameRate, IntraFramePeriod, RateControlMode, UsageType};
use openh264::formats::YUVBuffer;
use openh264::OpenH264API;
use songbird::video::OutFrame;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, PartialEq)]
pub enum Source {
    Monitor(u32),
    Window(u32),
}

#[derive(Clone)]
pub struct SourceInfo {
    pub source: Source,
    pub label: String,
    pub width: u32,
    pub height: u32,
}

/// Screens first, then app windows worth sharing.
pub fn sources() -> Vec<SourceInfo> {
    let mut out = Vec::new();
    if let Ok(mons) = xcap::Monitor::all() {
        for (i, m) in mons.iter().enumerate() {
            let (Ok(id), Ok(w), Ok(h)) = (m.id(), m.width(), m.height()) else { continue };
            let primary = if m.is_primary().unwrap_or(false) { " (main)" } else { "" };
            out.push(SourceInfo { source: Source::Monitor(id), label: format!("Screen {}{primary}", i + 1), width: w, height: h });
        }
    }
    if let Ok(wins) = xcap::Window::all() {
        for w in wins {
            let (Ok(id), Ok(title), Ok(width), Ok(height)) = (w.id(), w.title(), w.width(), w.height()) else { continue };
            if title.trim().is_empty() || width < 200 || height < 150 || w.is_minimized().unwrap_or(true) {
                continue;
            }
            let app = w.app_name().unwrap_or_default();
            let label = if app.is_empty() || title.contains(&app) { title.clone() } else { format!("{app} — {title}") };
            out.push(SourceInfo { source: Source::Window(id), label, width, height });
        }
    }
    out
}

/// Output size: at most `max_h` tall, same shape, even numbers (H.264 needs them).
pub fn target_size(src_w: u32, src_h: u32, max_h: u32) -> (u32, u32) {
    let h = src_h.min(max_h).max(2);
    let w = ((src_w as f64 * h as f64 / src_h.max(1) as f64).round() as u32).max(2);
    (w & !1, h & !1)
}

/// A sensible bitrate for screen content.
pub fn bitrate_for(w: u32, h: u32, fps: u32) -> u32 {
    let bits = (w as f64 * h as f64 * fps as f64 * 0.09) as u32;
    bits.clamp(800_000, 8_000_000)
}

/// Running share; dropping it stops capture.
pub struct Sharer {
    stop: Arc<AtomicBool>,
}

impl Drop for Sharer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

pub type Preview = Arc<Mutex<Option<egui::ColorImage>>>;

pub fn start(source: Source, width: u32, height: u32, fps: u32, preview: Preview, ctx: egui::Context) -> Sharer {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    std::thread::spawn(move || {
        if let Err(e) = run(source, width, height, fps, preview, ctx, flag) {
            tracing::warn!("share: stopped: {e}");
        }
    });
    Sharer { stop }
}

/// Nearest-neighbour scale of RGBA pixels (fast, good enough for screen content).
fn scale_rgba(src: &[u8], sw: u32, sh: u32, dw: u32, dh: u32) -> Vec<u8> {
    if sw == dw && sh == dh {
        return src.to_vec();
    }
    let mut out = vec![0u8; (dw * dh * 4) as usize];
    let xs: Vec<usize> = (0..dw).map(|x| ((x as u64 * sw as u64 / dw as u64) as usize).min(sw as usize - 1)).collect();
    for y in 0..dh as usize {
        let sy = ((y as u64 * sh as u64 / dh as u64) as usize).min(sh as usize - 1);
        let srow = &src[sy * sw as usize * 4..(sy + 1) * sw as usize * 4];
        let drow = &mut out[y * dw as usize * 4..(y + 1) * dw as usize * 4];
        for (x, &sx) in xs.iter().enumerate() {
            drow[x * 4..x * 4 + 4].copy_from_slice(&srow[sx * 4..sx * 4 + 4]);
        }
    }
    out
}

/// Scale + convert RGBA straight to I420 (the encoder's input) in one pass on 4 threads.
/// Replaces a full-size rescale plus a single-threaded colour conversion.
fn rgba_to_i420(src: &[u8], sw: u32, sh: u32, dw: u32, dh: u32) -> Vec<u8> {
    let (sw, sh, dw, dh) = (sw as usize, sh as usize, dw as usize, dh as usize);
    let xs: Vec<usize> = (0..dw).map(|x| (x * sw / dw).min(sw - 1)).collect();
    let row_of = |y: usize| (y * sh / dh).min(sh - 1);
    let mut out = vec![0u8; dw * dh * 3 / 2];
    let (ybuf, uv) = out.split_at_mut(dw * dh);
    let (ubuf, vbuf) = uv.split_at_mut((dw / 2) * (dh / 2));
    let px = |sy: usize, sx: usize| {
        let i = (sy * sw + sx) * 4;
        (src[i] as i32, src[i + 1] as i32, src[i + 2] as i32)
    };
    let threads = 4;
    let rows = dh.div_ceil(threads).max(1);
    std::thread::scope(|s| {
        for (t, ychunk) in ybuf.chunks_mut(rows * dw).enumerate() {
            let xs = &xs;
            s.spawn(move || {
                for (r, row) in ychunk.chunks_mut(dw).enumerate() {
                    let sy = row_of(t * rows + r);
                    for (x, out) in row.iter_mut().enumerate() {
                        let (r, g, b) = px(sy, xs[x]);
                        *out = (((66 * r + 129 * g + 25 * b + 128) >> 8) + 16) as u8;
                    }
                }
            });
        }
        let (cw, ch) = (dw / 2, dh / 2);
        let crows = ch.div_ceil(threads).max(1);
        for (t, (uc, vc)) in ubuf.chunks_mut(crows * cw).zip(vbuf.chunks_mut(crows * cw)).enumerate() {
            let xs = &xs;
            s.spawn(move || {
                for (r, (urow, vrow)) in uc.chunks_mut(cw).zip(vc.chunks_mut(cw)).enumerate() {
                    let sy = row_of((t * crows + r) * 2);
                    for x in 0..cw {
                        let (r, g, b) = px(sy, xs[x * 2]);
                        urow[x] = (((-38 * r - 74 * g + 112 * b + 128) >> 8) + 128).clamp(0, 255) as u8;
                        vrow[x] = (((112 * r - 94 * g - 18 * b + 128) >> 8) + 128).clamp(0, 255) as u8;
                    }
                }
            });
        }
    });
    out
}

fn make_encoder(w: u32, h: u32, fps: u32) -> Result<Encoder, String> {
    let cfg = EncoderConfig::new()
        .usage_type(UsageType::ScreenContentRealTime)
        .rate_control_mode(RateControlMode::Bitrate)
        .bitrate(BitRate::from_bps(bitrate_for(w, h, fps)))
        .max_frame_rate(FrameRate::from_hz(fps as f32))
        .intra_frame_period(IntraFramePeriod::from_num_frames(fps * 4))
        .skip_frames(true)
        // Use several CPU cores and the fastest preset so encoding keeps up in real time.
        .num_threads(4)
        .complexity(Complexity::Low);
    Encoder::with_api_config(OpenH264API::from_source(), cfg).map_err(|e| e.to_string())
}

fn run(source: Source, w: u32, h: u32, fps: u32, preview: Preview, ctx: egui::Context, stop: Arc<AtomicBool>) -> Result<(), String> {
    let (mut w, mut h, mut fps) = (w, h, fps);
    let mut encoder = make_encoder(w, h, fps)?;

    // Capture setup.
    let mut recorder = None;
    let mut frames_rx = None;
    let mut window = None;
    match &source {
        Source::Monitor(id) => {
            let m = xcap::Monitor::all().map_err(|e| e.to_string())?.into_iter().find(|m| m.id().ok() == Some(*id)).ok_or("screen not found")?;
            let (rec, rx) = m.video_recorder().map_err(|e| e.to_string())?;
            rec.start().map_err(|e| e.to_string())?;
            recorder = Some(rec);
            frames_rx = Some(rx);
        }
        Source::Window(id) => {
            window = Some(xcap::Window::all().map_err(|e| e.to_string())?.into_iter().find(|x| x.id().ok() == Some(*id)).ok_or("window not found")?);
        }
    }
    tracing::info!("share: capturing at {w}x{h} {fps}fps");

    let mut frame_gap = Duration::from_secs_f64(1.0 / fps.max(1) as f64);
    let started = Instant::now();
    let mut last_preview = Instant::now() - Duration::from_secs(1);
    let mut last = Instant::now() - frame_gap;
    let mut last_rgba: Option<(u32, u32, Vec<u8>)> = None;
    let mut stats: (u32, f64) = (0, 0.0);
    let mut stats_since = Instant::now();
    let mut slow_windows = 0u32;
    while !stop.load(Ordering::Relaxed) {
        // Grab the newest picture.
        let grabbed = if let Some(rx) = &frames_rx {
            let mut newest = rx.recv_timeout(Duration::from_millis(250)).ok();
            while let Ok(f) = rx.try_recv() {
                newest = Some(f);
            }
            newest.map(|f| (f.width, f.height, f.raw))
        } else if let Some(win) = &window {
            win.capture_image().ok().map(|img| (img.width(), img.height(), img.into_raw()))
        } else {
            None
        };
        // Screens only send new frames when something changes: repeat the last one for viewers.
        let (sw, sh, rgba) = match grabbed.or_else(|| last_rgba.clone()) {
            Some(f) => f,
            None => continue,
        };
        if sw < 2 || sh < 2 || rgba.len() < (sw * sh * 4) as usize {
            continue;
        }
        let wait = frame_gap.saturating_sub(last.elapsed());
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
        last = Instant::now();
        if last_preview.elapsed() >= Duration::from_millis(250) {
            let pw = 320u32;
            let ph = (pw as u64 * sh as u64 / sw.max(1) as u64).max(2) as u32;
            let small = scale_rgba(&rgba, sw, sh, pw, ph);
            *preview.lock().unwrap() = Some(egui::ColorImage::from_rgba_unmultiplied([pw as usize, ph as usize], &small));
            ctx.request_repaint();
            last_preview = Instant::now();
        }
        if !songbird::video::sender_ready() {
            continue; // stream connection not up yet
        }
        if songbird::video::take_keyframe_wanted() {
            encoder.force_intra_frame();
        }
        let t_enc = Instant::now();
        let yuv = YUVBuffer::from_vec(rgba_to_i420(&rgba, sw, sh, w, h), w as usize, h as usize);
        last_rgba = Some((sw, sh, rgba));
        let encoded = encoder.encode(&yuv).map(|bs| bs.to_vec());
        stats.0 += 1;
        stats.1 += t_enc.elapsed().as_secs_f64() * 1000.0;
        if stats_since.elapsed() >= Duration::from_secs(5) {
            let secs = stats_since.elapsed().as_secs_f64();
            let per = stats.1 / stats.0.max(1) as f64;
            let budget = 1000.0 / fps.max(1) as f64;
            tracing::info!("share: {:.1} fps encoded at {w}x{h}, {per:.1} ms per frame (budget {budget:.1} ms)", stats.0 as f64 / secs);
            // Can't keep up for 10 s in a row: step down (60 -> 30 fps, then 1080p -> 720p -> 480p)
            // instead of stuttering. One slow moment (a busy game scene) isn't enough.
            slow_windows = if per > budget * 1.15 { slow_windows + 1 } else { 0 };
            if slow_windows >= 2 {
                slow_windows = 0;
                let before = (w, h, fps);
                if fps > 30 {
                    fps = 30;
                } else if h > 720 {
                    (w, h) = target_size(w, h, 720);
                } else if h > 480 {
                    (w, h) = target_size(w, h, 480);
                }
                if (w, h, fps) != before {
                    tracing::info!("share: PC too slow for {}x{}@{}, switching to {w}x{h}@{fps}", before.0, before.1, before.2);
                    encoder = make_encoder(w, h, fps)?;
                    frame_gap = Duration::from_secs_f64(1.0 / fps as f64);
                }
            }
            stats = (0, 0.0);
            stats_since = Instant::now();
        }
        let data = match encoded {
            Ok(d) => d,
            Err(e) => {
                tracing::debug!("share: encode error {e}");
                continue;
            }
        };
        if data.is_empty() {
            continue; // rate control skipped this frame
        }
        let ts90k = (started.elapsed().as_secs_f64() * 90_000.0) as u64 as u32;
        songbird::video::send_frame(OutFrame { annexb: data, ts90k });
    }
    if let Some(rec) = recorder {
        let _ = rec.stop();
    }
    tracing::info!("share: capture stopped");
    Ok(())
}
