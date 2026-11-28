// Your camera in calls: webcam (Windows Media Foundation) -> H.264 (openh264) -> the patched
// songbird's camera sender on the voice connection, plus a small preview for your own tile.

use eframe::egui;
use openh264::encoder::{BitRate, Complexity, Encoder, EncoderConfig, FrameRate, IntraFramePeriod, RateControlMode, UsageType};
use openh264::formats::YUVBuffer;
use openh264::OpenH264API;
use songbird::video::OutFrame;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use windows::core::PWSTR;
use windows::Win32::Media::MediaFoundation::*;
use windows::Win32::System::Com::{CoInitializeEx, CoTaskMemFree, CoUninitialize, COINIT_MULTITHREADED};

pub const WIDTH: u32 = 1280;
pub const HEIGHT: u32 = 720;
pub const FPS: u32 = 30;
pub const BITRATE: u32 = 2_500_000;

/// Running camera; dropping it turns the webcam off.
pub struct Camera {
    stop: Arc<AtomicBool>,
}

impl Drop for Camera {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Start the first webcam. `me` is our user id (the preview goes into our own tile).
/// `failed` is called (from the camera thread) with a message if it can't start.
pub fn start(me: u64, ctx: egui::Context, failed: impl FnOnce(String) + Send + 'static) -> Camera {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    std::thread::spawn(move || {
        if let Err(e) = unsafe { run(me, &ctx, &flag) } {
            tracing::warn!("camera: {e}");
            failed(e);
        }
    });
    Camera { stop }
}

unsafe fn run(me: u64, ctx: &egui::Context, stop: &AtomicBool) -> Result<(), String> {
    let _ = CoInitializeEx(None, COINIT_MULTITHREADED);
    MFStartup(MF_VERSION, MFSTARTUP_FULL).map_err(|e| format!("Media Foundation: {e}"))?;
    let result = capture(me, ctx, stop);
    let _ = MFShutdown();
    CoUninitialize();
    result
}

unsafe fn capture(me: u64, ctx: &egui::Context, stop: &AtomicBool) -> Result<(), String> {
    let err = |what: &str, e: windows::core::Error| format!("{what}: {e}");

    // Find the webcams.
    let mut attrs: Option<IMFAttributes> = None;
    MFCreateAttributes(&mut attrs, 1).map_err(|e| err("attributes", e))?;
    let attrs = attrs.ok_or("no attributes")?;
    attrs
        .SetGUID(&MF_DEVSOURCE_ATTRIBUTE_SOURCE_TYPE, &MF_DEVSOURCE_ATTRIBUTE_SOURCE_TYPE_VIDCAP_GUID)
        .map_err(|e| err("attributes", e))?;
    let mut list: *mut Option<IMFActivate> = std::ptr::null_mut();
    let mut count = 0u32;
    MFEnumDeviceSources(&attrs, &mut list, &mut count).map_err(|e| err("listing cameras", e))?;
    if count == 0 || list.is_null() {
        return Err("No camera found. Plug one in (or check Windows camera privacy settings) and try again.".into());
    }
    let devices: Vec<Option<IMFActivate>> = std::slice::from_raw_parts_mut(list, count as usize).iter_mut().map(|d| d.take()).collect();
    CoTaskMemFree(Some(list as *const _));
    let device = devices.into_iter().flatten().next().ok_or("No camera found.")?;
    let mut name = PWSTR::null();
    let mut len = 0u32;
    if device.GetAllocatedString(&MF_DEVSOURCE_ATTRIBUTE_FRIENDLY_NAME, &mut name, &mut len).is_ok() {
        tracing::info!("camera: using {}", name.to_string().unwrap_or_default());
        CoTaskMemFree(Some(name.0 as *const _));
    }

    // Open it, asking Windows to deliver 720p NV12 (it converts/scales if the camera can't).
    let source: IMFMediaSource = device
        .ActivateObject()
        .map_err(|e| format!("Couldn't open the camera (is another app using it, or is camera access off in Windows privacy settings?): {e}"))?;
    let mut rattrs: Option<IMFAttributes> = None;
    MFCreateAttributes(&mut rattrs, 1).map_err(|e| err("attributes", e))?;
    let rattrs = rattrs.ok_or("no attributes")?;
    rattrs.SetUINT32(&MF_SOURCE_READER_ENABLE_ADVANCED_VIDEO_PROCESSING, 1).map_err(|e| err("attributes", e))?;
    let reader = MFCreateSourceReaderFromMediaSource(&source, &rattrs).map_err(|e| err("camera reader", e))?;
    let stream = MF_SOURCE_READER_FIRST_VIDEO_STREAM.0 as u32;
    let mt = MFCreateMediaType().map_err(|e| err("media type", e))?;
    mt.SetGUID(&MF_MT_MAJOR_TYPE, &MFMediaType_Video).map_err(|e| err("media type", e))?;
    mt.SetGUID(&MF_MT_SUBTYPE, &MFVideoFormat_NV12).map_err(|e| err("media type", e))?;
    mt.SetUINT64(&MF_MT_FRAME_SIZE, ((WIDTH as u64) << 32) | HEIGHT as u64).map_err(|e| err("media type", e))?;
    mt.SetUINT64(&MF_MT_FRAME_RATE, ((FPS as u64) << 32) | 1).map_err(|e| err("media type", e))?;
    reader.SetCurrentMediaType(stream, None, &mt).map_err(|e| err("camera format", e))?;
    let actual = reader.GetCurrentMediaType(stream).map_err(|e| err("camera format", e))?;
    let size = actual.GetUINT64(&MF_MT_FRAME_SIZE).unwrap_or(((WIDTH as u64) << 32) | HEIGHT as u64);
    let (w, h) = (((size >> 32) as u32) & !1, (size as u32) & !1);
    tracing::info!("camera: capturing {w}x{h}");

    let cfg = EncoderConfig::new()
        .usage_type(UsageType::CameraVideoRealTime)
        .rate_control_mode(RateControlMode::Bitrate)
        .bitrate(BitRate::from_bps(BITRATE))
        .max_frame_rate(FrameRate::from_hz(FPS as f32))
        .intra_frame_period(IntraFramePeriod::from_num_frames(FPS * 4))
        .skip_frames(true)
        .num_threads(2)
        .complexity(Complexity::Low);
    let mut encoder = Encoder::with_api_config(OpenH264API::from_source(), cfg).map_err(|e| e.to_string())?;

    let started = Instant::now();
    let mut last_preview = Instant::now() - Duration::from_secs(1);
    let mut sent = 0u32;
    let mut since = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let mut flags = 0u32;
        let mut sample: Option<IMFSample> = None;
        reader
            .ReadSample(stream, 0, None, Some(&mut flags), None, Some(&mut sample))
            .map_err(|e| err("reading the camera", e))?;
        if flags & (MF_SOURCE_READERF_ENDOFSTREAM.0 as u32) != 0 || flags & (MF_SOURCE_READERF_ERROR.0 as u32) != 0 {
            return Err("The camera stopped.".into());
        }
        let Some(sample) = sample else { continue };
        let buf = sample.ConvertToContiguousBuffer().map_err(|e| err("camera frame", e))?;
        let mut ptr: *mut u8 = std::ptr::null_mut();
        let mut cur = 0u32;
        buf.Lock(&mut ptr, None, Some(&mut cur)).map_err(|e| err("camera frame", e))?;
        let need = (w * h * 3 / 2) as usize;
        let nv12 = if !ptr.is_null() && cur as usize >= need { Some(std::slice::from_raw_parts(ptr, need).to_vec()) } else { None };
        let _ = buf.Unlock();
        let Some(nv12) = nv12 else { continue };

        // Our own tile shows what we send (small, ~15 fps).
        if last_preview.elapsed() >= Duration::from_millis(66) {
            let img = nv12_preview(&nv12, w as usize, h as usize, 4);
            crate::stream::put_camera_frame(me, img);
            ctx.request_repaint();
            last_preview = Instant::now();
        }
        if !songbird::video::camera_ready() {
            continue; // not in a call / voice connection not up
        }
        if songbird::video::camera_take_keyframe() {
            encoder.force_intra_frame();
        }
        let yuv = YUVBuffer::from_vec(nv12_to_i420(&nv12, w as usize, h as usize), w as usize, h as usize);
        let Ok(data) = encoder.encode(&yuv).map(|bs| bs.to_vec()) else { continue };
        if data.is_empty() {
            continue;
        }
        let ts90k = (started.elapsed().as_secs_f64() * 90_000.0) as u64 as u32;
        songbird::video::camera_send_frame(OutFrame { annexb: data, ts90k });
        sent += 1;
        if since.elapsed() >= Duration::from_secs(10) {
            tracing::info!("camera: {:.1} fps sent at {w}x{h}", sent as f64 / since.elapsed().as_secs_f64());
            sent = 0;
            since = Instant::now();
        }
    }
    tracing::info!("camera: off");
    Ok(())
}

/// NV12 (Y plane + interleaved UV) -> I420 (Y, U, V planes), which the encoder takes.
fn nv12_to_i420(nv12: &[u8], w: usize, h: usize) -> Vec<u8> {
    let ysize = w * h;
    let csize = ysize / 4;
    let mut out = vec![0u8; ysize + 2 * csize];
    out[..ysize].copy_from_slice(&nv12[..ysize]);
    let (u, v) = out[ysize..].split_at_mut(csize);
    for (i, uv) in nv12[ysize..ysize + 2 * csize].chunks_exact(2).enumerate() {
        u[i] = uv[0];
        v[i] = uv[1];
    }
    out
}

/// Small RGBA picture from NV12 (every `step`-th pixel).
fn nv12_preview(nv12: &[u8], w: usize, h: usize, step: usize) -> egui::ColorImage {
    let (ow, oh) = (w / step, h / step);
    let mut rgba = vec![0u8; ow * oh * 4];
    let uv = &nv12[w * h..];
    for y in 0..oh {
        for x in 0..ow {
            let (sx, sy) = (x * step, y * step);
            let yy = nv12[sy * w + sx] as i32 - 16;
            let ci = (sy / 2) * w + (sx / 2) * 2;
            let (u, v) = (uv[ci] as i32 - 128, uv[ci + 1] as i32 - 128);
            let c = 298 * yy;
            let p = &mut rgba[(y * ow + x) * 4..(y * ow + x) * 4 + 4];
            p[0] = ((c + 409 * v + 128) >> 8).clamp(0, 255) as u8;
            p[1] = ((c - 100 * u - 208 * v + 128) >> 8).clamp(0, 255) as u8;
            p[2] = ((c + 516 * u + 128) >> 8).clamp(0, 255) as u8;
            p[3] = 255;
        }
    }
    egui::ColorImage::from_rgba_unmultiplied([ow, oh], &rgba)
}
