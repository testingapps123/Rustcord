// Native voice: songbird (voice gateway + UDP + encryption + DAVE end-to-end encryption via davey),
// Opus audio, and cpal for the microphone and speakers. No web engine involved.
//
// Flow: the UI asks the main gateway to join (op 4). Discord answers with VOICE_STATE_UPDATE
// (our session id) and VOICE_SERVER_UPDATE (voice server + token); the gateway forwards both here,
// and once we have everything we connect songbird's driver.

use crate::Evt;
use async_trait::async_trait;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use eframe::egui;
use songbird::{
    driver::{Channels, DecodeConfig, DecodeMode, SampleRate},
    events::{
        context_data::{DisconnectKind, DisconnectReason},
        CoreEvent, Event, EventContext, EventHandler,
    },
    id::{ChannelId, GuildId, UserId},
    input::{Input, RawAdapter},
    Config, ConnectionInfo, Driver,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use symphonia_core::io::MediaSource;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// Mic level (0..1) above which we treat you as talking.
const VOICE_GATE: f32 = 0.015;
/// Keep sending this long after you stop talking, so word endings aren't cut off.
const GATE_HANGOVER: Duration = Duration::from_millis(450);
/// Shown when the voice server never answers our UDP packets (common on networks that block VoIP).
const BLOCKED: &str = "Voice is blocked on this network. Turn on your VPN, then join again.";

// ---------- Voice & Video settings ----------

/// Settings shared by the settings screen and the voice engine (saved to settings.json).
#[derive(Clone, PartialEq)]
pub struct VoiceCfg {
    pub input_dev: Option<String>,
    pub output_dev: Option<String>,
    /// 0.0 ..= 2.0
    pub in_vol: f32,
    pub out_vol: f32,
    /// mic level that counts as talking (Voice Activity mode)
    pub gate: f32,
    pub ptt: bool,
    /// Windows virtual-key code for Push to Talk
    pub ptt_key: u16,
    pub denoise: bool,
    /// per-person volume, user id -> 0.0 ..= 2.0
    pub user_vol: HashMap<u64, f32>,
}

impl Default for VoiceCfg {
    fn default() -> Self {
        Self {
            input_dev: None,
            output_dev: None,
            in_vol: 1.0,
            out_vol: 1.0,
            gate: VOICE_GATE,
            ptt: false,
            ptt_key: 0xC0, // the ` key
            denoise: true,
            user_vol: HashMap::new(),
        }
    }
}

pub type SharedCfg = Arc<Mutex<VoiceCfg>>;

impl VoiceCfg {
    pub fn load(path: &std::path::Path) -> Self {
        let mut c = Self::default();
        let Ok(text) = std::fs::read_to_string(path) else { return c };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { return c };
        c.input_dev = v["input_dev"].as_str().map(String::from);
        c.output_dev = v["output_dev"].as_str().map(String::from);
        c.in_vol = v["in_vol"].as_f64().unwrap_or(1.0) as f32;
        c.out_vol = v["out_vol"].as_f64().unwrap_or(1.0) as f32;
        c.gate = v["gate"].as_f64().unwrap_or(VOICE_GATE as f64) as f32;
        c.ptt = v["ptt"].as_bool().unwrap_or(false);
        c.ptt_key = v["ptt_key"].as_u64().unwrap_or(0xC0) as u16;
        c.denoise = v["denoise"].as_bool().unwrap_or(true);
        if let Some(m) = v["user_vol"].as_object() {
            for (k, val) in m {
                if let (Ok(id), Some(vol)) = (k.parse::<u64>(), val.as_f64()) {
                    c.user_vol.insert(id, vol as f32);
                }
            }
        }
        c
    }

    pub fn save(&self, path: &std::path::Path) {
        let users: serde_json::Map<String, serde_json::Value> =
            self.user_vol.iter().map(|(k, v)| (k.to_string(), serde_json::json!(v))).collect();
        let v = serde_json::json!({
            "input_dev": self.input_dev, "output_dev": self.output_dev,
            "in_vol": self.in_vol, "out_vol": self.out_vol, "gate": self.gate,
            "ptt": self.ptt, "ptt_key": self.ptt_key, "denoise": self.denoise,
            "user_vol": users,
        });
        let _ = std::fs::write(path, serde_json::to_string_pretty(&v).unwrap_or_default());
    }
}

/// Names of the microphones and speakers Windows knows about.
pub fn list_devices() -> (Vec<String>, Vec<String>) {
    let host = cpal::default_host();
    let ins = host.input_devices().map(|d| d.filter_map(|x| x.name().ok()).collect()).unwrap_or_default();
    let outs = host.output_devices().map(|d| d.filter_map(|x| x.name().ok()).collect()).unwrap_or_default();
    (ins, outs)
}

/// Is this key / mouse button held right now? Works even while another app (a game) is focused,
/// by reading the key state; nothing is hooked or injected.
pub fn key_down(vk: u16) -> bool {
    unsafe { (windows_sys::Win32::UI::Input::KeyboardAndMouse::GetAsyncKeyState(vk as i32) as u16) & 0x8000 != 0 }
}

/// For "record keybind": the first key or mouse button currently held (left click excluded).
pub fn held_key() -> Option<u16> {
    (0x02u16..=0xFE).find(|vk| !matches!(vk, 0x10..=0x12) && key_down(*vk))
}

pub fn key_name(vk: u16) -> String {
    match vk {
        0x02 => "Right Click".into(),
        0x04 => "Middle Click".into(),
        0x05 => "Mouse 4".into(),
        0x06 => "Mouse 5".into(),
        0x08 => "Backspace".into(),
        0x09 => "Tab".into(),
        0x0D => "Enter".into(),
        0x14 => "Caps Lock".into(),
        0x20 => "Space".into(),
        0x30..=0x39 | 0x41..=0x5A => ((vk as u8) as char).to_string(),
        0x60..=0x69 => format!("Num {}", vk - 0x60),
        0x70..=0x87 => format!("F{}", vk - 0x6F),
        0xA0 => "Left Shift".into(),
        0xA1 => "Right Shift".into(),
        0xA2 => "Left Ctrl".into(),
        0xA3 => "Right Ctrl".into(),
        0xA4 => "Left Alt".into(),
        0xA5 => "Right Alt".into(),
        0xC0 => "`".into(),
        0xBA => ";".into(),
        0xDE => "'".into(),
        0xDB => "[".into(),
        0xDD => "]".into(),
        0xDC => "\\".into(),
        0xBC => ",".into(),
        0xBE => ".".into(),
        0xBF => "/".into(),
        _ => format!("Key {vk:#04x}"),
    }
}

pub enum VoiceMsg {
    Join { guild: Option<String>, channel: String },
    Leave,
    Mute(bool),
    Deafen(bool),
    Me(String),
    MyState { session_id: String, channel_id: Option<String> },
    Server { token: String, endpoint: Option<String> },
    /// Main gateway session id (from READY); stream connections reuse it.
    Session(String),
    WatchStream { key: String, streamer: String },
    StopStream,
    StreamCreate { key: String, rtc_server_id: String },
    StreamServer { key: String, endpoint: Option<String>, token: String },
    StreamGone(String),
    /// devices changed in settings: reopen mic/speakers
    RestartAudio,
    /// settings 'Let's Check': hear your own mic
    MicTest(bool),
}

pub enum VoiceUi {
    Connecting,
    Connected,
    Ended(String),
    RemoteSpeaking(Vec<String>),
    SelfSpeaking(bool),
    StreamConnecting,
    StreamLive,
    StreamEnded(String),
}

/// Reports when a stream connection drops.
#[derive(Clone)]
struct StreamHandler {
    out: Sender<Evt>,
    ctx: egui::Context,
}

#[async_trait]
impl EventHandler for StreamHandler {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::DriverDisconnect(d) = ctx {
            if !matches!(d.reason, Some(DisconnectReason::Requested)) && !matches!(d.kind, DisconnectKind::Connect) {
                tracing::warn!("stream: disconnected kind={:?} reason={:?}", d.kind, d.reason);
                ui(&self.out, &self.ctx, VoiceUi::StreamEnded("The stream connection dropped".into()));
            }
        }
        None
    }
}

async fn connect_stream(
    out: &Sender<Evt>,
    ctx: &egui::Context,
    rtc_server_id: &str,
    endpoint: String,
    token: String,
    session_id: String,
    user: &str,
) -> Result<(Driver, u64), String> {
    let rtc = nz(rtc_server_id).ok_or("bad stream server id")?;
    let user_id = nz(user).ok_or("bad user id")?;
    songbird::video::add_stream_server(rtc.get());
    // Decrypt (for the video hook) but don't decode the stream's audio.
    let mut driver = Driver::new(Config::default().decode_mode(DecodeMode::Decrypt));
    driver.add_global_event(Event::Core(CoreEvent::DriverDisconnect), StreamHandler { out: out.clone(), ctx: ctx.clone() });
    let info = ConnectionInfo {
        channel_id: ChannelId::from(rtc),
        endpoint,
        guild_id: GuildId::from(rtc),
        session_id,
        token,
        user_id: UserId::from(user_id),
    };
    match tokio::time::timeout(Duration::from_secs(20), driver.connect(info)).await {
        Ok(Ok(())) => Ok((driver, rtc.get())),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("stream server didn't answer".into()),
    }
}

pub fn spawn(
    out: Sender<Evt>,
    ctx: egui::Context,
    streamer: crate::stream::Streamer,
    cfg: SharedCfg,
    meter: Arc<AtomicU32>,
) -> UnboundedSender<VoiceMsg> {
    let (tx, rx) = unbounded_channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("could not start voice runtime");
        rt.block_on(run(rx, out, ctx, streamer, cfg, meter));
    });
    tx
}

fn nz(id: &str) -> Option<NonZeroU64> {
    id.parse::<u64>().ok().and_then(NonZeroU64::new)
}

fn ui(out: &Sender<Evt>, ctx: &egui::Context, v: VoiceUi) {
    let _ = out.send(Evt::Voice(v));
    ctx.request_repaint();
}

// ---------- audio devices ----------

struct Shared {
    mic: Arc<(Mutex<VecDeque<f32>>, Condvar)>,
    /// peak mic level since the voice task last looked (voice activity)
    mic_level: Arc<AtomicU32>,
    /// peak mic level since the settings screen last looked (meter)
    meter: Arc<AtomicU32>,
    speaker: Arc<Mutex<VecDeque<f32>>>,
    deaf: Arc<AtomicBool>,
    cfg: SharedCfg,
    out_rate: Arc<AtomicU32>,
    out_channels: Arc<AtomicU32>,
}

struct AudioIo {
    _input: cpal::Stream,
    _output: cpal::Stream,
    mic_rate: u32,
    mic_channels: u16,
    out_rate: u32,
    out_channels: u16,
}

fn store_max(a: &AtomicU32, v: f32) {
    if v > f32::from_bits(a.load(Ordering::Relaxed)) {
        a.store(v.to_bits(), Ordering::Relaxed);
    }
}

/// Microphone processing in the audio callback: volume, noise suppression, level meters.
struct MicProc {
    buf: Arc<(Mutex<VecDeque<f32>>, Condvar)>,
    level: Arc<AtomicU32>,
    meter: Arc<AtomicU32>,
    cfg: SharedCfg,
    cap: usize,
    channels: usize,
    rate: u32,
    denoiser: Box<nnnoiseless::DenoiseState<'static>>,
    mono: Vec<f32>,
}

impl MicProc {
    fn push(&mut self, samples: impl Iterator<Item = f32>) {
        let (gain, denoise) = match self.cfg.try_lock() {
            Ok(c) => (c.in_vol, c.denoise && self.rate == 48_000),
            Err(_) => (1.0, false),
        };
        let samples: Vec<f32> = samples.map(|s| s * gain).collect();
        let processed: Vec<f32> = if denoise {
            // RNNoise works on 480-sample mono frames at 48 kHz.
            for frame in samples.chunks(self.channels.max(1)) {
                self.mono.push(frame.iter().sum::<f32>() / frame.len() as f32);
            }
            let mut out = Vec::with_capacity(samples.len());
            const N: usize = nnnoiseless::DenoiseState::FRAME_SIZE;
            while self.mono.len() >= N {
                let input: Vec<f32> = self.mono.drain(..N).map(|s| s * 32768.0).collect();
                let mut clean = [0.0f32; N];
                self.denoiser.process_frame(&mut clean, &input);
                for s in clean {
                    for _ in 0..self.channels.max(1) {
                        out.push(s / 32768.0);
                    }
                }
            }
            out
        } else {
            samples
        };
        let mut q = self.buf.0.lock().unwrap();
        let mut peak = 0.0f32;
        for s in processed {
            peak = peak.max(s.abs());
            q.push_back(s);
        }
        let extra = q.len().saturating_sub(self.cap);
        if extra > 0 {
            q.drain(..extra);
        }
        drop(q);
        self.buf.1.notify_one();
        store_max(&self.level, peak);
        store_max(&self.meter, peak);
    }
}

fn pull_speaker(buf: &Mutex<VecDeque<f32>>, out: &mut dyn Iterator<Item = &mut f32>) {
    let mut q = buf.lock().unwrap();
    for s in out {
        *s = q.pop_front().unwrap_or(0.0);
    }
}

fn pick_device(devices: Option<impl Iterator<Item = cpal::Device>>, want: &Option<String>) -> Option<cpal::Device> {
    let want = want.as_ref()?;
    devices?.find(|d| d.name().ok().as_ref() == Some(want))
}

fn start_audio(sh: &Shared) -> Result<AudioIo, String> {
    let host = cpal::default_host();
    let (want_in, want_out) = {
        let c = sh.cfg.lock().unwrap();
        (c.input_dev.clone(), c.output_dev.clone())
    };
    let mic = pick_device(host.input_devices().ok(), &want_in)
        .or_else(|| host.default_input_device())
        .ok_or("no microphone found")?;
    let spk = pick_device(host.output_devices().ok(), &want_out)
        .or_else(|| host.default_output_device())
        .ok_or("no speakers or headphones found")?;

    let in_cfg = mic.default_input_config().map_err(|e| e.to_string())?;
    let out_cfg = spk.default_output_config().map_err(|e| e.to_string())?;
    let (in_fmt, out_fmt) = (in_cfg.sample_format(), out_cfg.sample_format());
    let in_sc: cpal::StreamConfig = in_cfg.into();
    let out_sc: cpal::StreamConfig = out_cfg.into();
    let err = |e: cpal::StreamError| eprintln!("audio error: {e}");

    let mut proc = MicProc {
        buf: sh.mic.clone(),
        level: sh.mic_level.clone(),
        meter: sh.meter.clone(),
        cfg: sh.cfg.clone(),
        cap: (in_sc.sample_rate.0 as usize * in_sc.channels as usize) / 2, // 0.5 s max backlog
        channels: in_sc.channels as usize,
        rate: in_sc.sample_rate.0,
        denoiser: nnnoiseless::DenoiseState::new(),
        mono: Vec::new(),
    };
    let input = match in_fmt {
        cpal::SampleFormat::F32 => mic.build_input_stream(&in_sc, move |d: &[f32], _: &_| proc.push(d.iter().copied()), err, None),
        cpal::SampleFormat::I16 => mic.build_input_stream(&in_sc, move |d: &[i16], _: &_| proc.push(d.iter().map(|s| *s as f32 / 32768.0)), err, None),
        cpal::SampleFormat::U16 => mic.build_input_stream(&in_sc, move |d: &[u16], _: &_| proc.push(d.iter().map(|s| (*s as f32 - 32768.0) / 32768.0)), err, None),
        f => return Err(format!("unsupported microphone format {f:?}")),
    }
    .map_err(|e| e.to_string())?;

    let s1 = sh.speaker.clone();
    let output = match out_fmt {
        cpal::SampleFormat::F32 => spk.build_output_stream(&out_sc, move |d: &mut [f32], _: &_| pull_speaker(&s1, &mut d.iter_mut()), err, None),
        cpal::SampleFormat::I16 => spk.build_output_stream(
            &out_sc,
            move |d: &mut [i16], _: &_| {
                let mut tmp = vec![0.0f32; d.len()];
                pull_speaker(&s1, &mut tmp.iter_mut());
                for (o, s) in d.iter_mut().zip(tmp) {
                    *o = (s.clamp(-1.0, 1.0) * 32767.0) as i16;
                }
            },
            err,
            None,
        ),
        f => return Err(format!("unsupported speaker format {f:?}")),
    }
    .map_err(|e| e.to_string())?;

    input.play().map_err(|e| e.to_string())?;
    output.play().map_err(|e| e.to_string())?;
    sh.out_rate.store(out_sc.sample_rate.0, Ordering::Relaxed);
    sh.out_channels.store(out_sc.channels as u32, Ordering::Relaxed);
    tracing::info!(
        "audio: mic '{}' {}Hz x{} / speakers '{}' {}Hz x{}",
        mic.name().unwrap_or_default(), in_sc.sample_rate.0, in_sc.channels,
        spk.name().unwrap_or_default(), out_sc.sample_rate.0, out_sc.channels
    );
    Ok(AudioIo {
        _input: input,
        _output: output,
        mic_rate: in_sc.sample_rate.0,
        mic_channels: in_sc.channels,
        out_rate: out_sc.sample_rate.0,
        out_channels: out_sc.channels,
    })
}

/// Live microphone as a songbird input: raw interleaved f32 PCM.
struct MicSource {
    buf: Arc<(Mutex<VecDeque<f32>>, Condvar)>,
}

impl Read for MicSource {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let want = out.len() / 4;
        if want == 0 {
            return Ok(0);
        }
        let (lock, cv) = &*self.buf;
        let mut q = lock.lock().unwrap();
        if q.len() < want {
            q = cv.wait_timeout(q, Duration::from_millis(25)).unwrap().0;
        }
        for i in 0..want {
            let s = q.pop_front().unwrap_or(0.0);
            out[i * 4..i * 4 + 4].copy_from_slice(&s.to_le_bytes());
        }
        Ok(want * 4)
    }
}

impl Seek for MicSource {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "live microphone"))
    }
}

impl MediaSource for MicSource {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}

// ---------- incoming audio + driver events ----------

struct Inner {
    out: Sender<Evt>,
    ctx: egui::Context,
    speaker: Arc<Mutex<VecDeque<f32>>>,
    deaf: Arc<AtomicBool>,
    out_rate: Arc<AtomicU32>,
    out_channels: Arc<AtomicU32>,
    cfg: SharedCfg,
    ssrc_user: Mutex<HashMap<u32, u64>>,
    last_speaking: Mutex<Vec<String>>,
}

#[derive(Clone)]
struct Handler(Arc<Inner>);

impl Inner {
    /// Mix everyone's 20 ms of 48 kHz stereo audio (each at their own volume) for the speakers.
    fn play(&self, streams: Vec<(&Vec<i16>, Option<u64>)>) {
        const FRAMES: usize = 960;
        let (master, vols) = {
            let c = self.cfg.lock().unwrap();
            let vols: Vec<f32> = streams.iter().map(|(_, u)| u.and_then(|u| c.user_vol.get(&u).copied()).unwrap_or(1.0)).collect();
            (c.out_vol, vols)
        };
        let mut mix = vec![0.0f32; FRAMES * 2];
        for ((pcm, _), vol) in streams.iter().zip(vols) {
            let g = vol * master / 32768.0;
            for (m, s) in mix.iter_mut().zip(pcm.iter()) {
                *m += *s as f32 * g;
            }
        }
        let out_rate = self.out_rate.load(Ordering::Relaxed).max(8_000);
        let ratio = out_rate as f64 / 48_000.0;
        let n_out = (FRAMES as f64 * ratio).round() as usize;
        let ch = self.out_channels.load(Ordering::Relaxed).max(1) as usize;
        let mut buf = self.speaker.lock().unwrap();
        for j in 0..n_out {
            let pos = j as f64 / ratio;
            let i0 = (pos.floor() as usize).min(FRAMES - 1);
            let i1 = (i0 + 1).min(FRAMES - 1);
            let f = (pos - i0 as f64) as f32;
            let l = mix[i0 * 2] * (1.0 - f) + mix[i1 * 2] * f;
            let r = mix[i0 * 2 + 1] * (1.0 - f) + mix[i1 * 2 + 1] * f;
            for c in 0..ch {
                buf.push_back(match (ch, c % 2) {
                    (1, _) => (l + r) * 0.5,
                    (_, 0) => l,
                    _ => r,
                });
            }
        }
        let cap = out_rate as usize * ch * 3 / 10; // 0.3 s max latency
        let extra = buf.len().saturating_sub(cap);
        if extra > 0 {
            buf.drain(..extra);
        }
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        let me = &self.0;
        match ctx {
            EventContext::SpeakingStateUpdate(s) => {
                if let Some(u) = s.user_id {
                    me.ssrc_user.lock().unwrap().insert(s.ssrc, u.0);
                }
            }
            EventContext::VoiceTick(tick) => {
                let mut streams = Vec::new();
                let mut talking = Vec::new();
                {
                    let map = me.ssrc_user.lock().unwrap();
                    for (ssrc, data) in &tick.speaking {
                        if let Some(pcm) = &data.decoded_voice {
                            let user = map.get(ssrc).copied();
                            streams.push((pcm, user));
                            if let Some(u) = user {
                                talking.push(u.to_string());
                            }
                        }
                    }
                }
                if !me.deaf.load(Ordering::Relaxed) && !streams.is_empty() {
                    me.play(streams);
                }
                talking.sort();
                let mut last = me.last_speaking.lock().unwrap();
                if *last != talking {
                    *last = talking.clone();
                    ui(&me.out, &me.ctx, VoiceUi::RemoteSpeaking(talking));
                }
            }
            EventContext::DriverConnect(_) | EventContext::DriverReconnect(_) => ui(&me.out, &me.ctx, VoiceUi::Connected),
            EventContext::DriverDisconnect(d) => {
                // Our own leave/switch, or a failed first attempt (the voice task reports those).
                if matches!(d.reason, Some(DisconnectReason::Requested)) || matches!(d.kind, DisconnectKind::Connect) {
                    return None;
                }
                tracing::warn!("voice: driver disconnected kind={:?} reason={:?}", d.kind, d.reason);
                let why = match &d.reason {
                    Some(r) => format!("Voice disconnected: {r:?}"),
                    None => "Voice disconnected".into(),
                };
                ui(&me.out, &me.ctx, VoiceUi::Ended(why));
            }
            _ => {}
        }
        None
    }
}

// ---------- the voice task ----------

struct Live {
    driver: Driver,
    _audio: AudioIo,
}

async fn run(
    mut rx: UnboundedReceiver<VoiceMsg>,
    out: Sender<Evt>,
    ctx: egui::Context,
    streamer: crate::stream::Streamer,
    cfg: SharedCfg,
    meter: Arc<AtomicU32>,
) {
    let mut mic_test: Option<AudioIo> = None;
    let mut gw_session: Option<String> = None;
    // (stream key, streamer id) we want to watch, plus what Discord has told us about it so far
    let mut want_stream: Option<(String, String)> = None;
    let mut stream_rtc: Option<String> = None;
    let mut stream_server: Option<(String, String)> = None;
    let mut stream_live: Option<(Driver, u64)> = None;
    let shared = Shared {
        mic: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
        mic_level: Arc::new(AtomicU32::new(0)),
        meter,
        cfg,
        out_rate: Arc::new(AtomicU32::new(48_000)),
        out_channels: Arc::new(AtomicU32::new(2)),
        speaker: Arc::new(Mutex::new(VecDeque::new())),
        deaf: Arc::new(AtomicBool::new(false)),
    };
    let mut me: Option<String> = None;
    let mut pending: Option<(Option<String>, String)> = None;
    let mut session: Option<String> = None;
    let mut server: Option<(String, String)> = None;
    let mut live: Option<Live> = None;
    let mut muted = false;
    let mut gate_open = false;
    let mut last_loud = Instant::now() - GATE_HANGOVER;
    let mut tick = tokio::time::interval(Duration::from_millis(40));

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match &msg {
                    VoiceMsg::Me(id) => tracing::info!("voice: my user id {id}"),
                    VoiceMsg::Join { guild, channel } => tracing::info!("voice: join requested guild={guild:?} channel={channel}"),
                    VoiceMsg::Leave => tracing::info!("voice: leave requested"),
                    VoiceMsg::Mute(m) => tracing::info!("voice: mute={m}"),
                    VoiceMsg::Deafen(d) => tracing::info!("voice: deafen={d}"),
                    VoiceMsg::MyState { channel_id, .. } => tracing::info!("voice: got my VOICE_STATE_UPDATE channel={channel_id:?}"),
                    VoiceMsg::Server { endpoint, .. } => tracing::info!("voice: got VOICE_SERVER_UPDATE endpoint={endpoint:?}"),
                    VoiceMsg::Session(_) => {}
                    VoiceMsg::WatchStream { key, .. } => tracing::info!("stream: watch requested {key}"),
                    VoiceMsg::StopStream => tracing::info!("stream: stop requested"),
                    VoiceMsg::StreamCreate { key, rtc_server_id } => tracing::info!("stream: STREAM_CREATE {key} rtc={rtc_server_id}"),
                    VoiceMsg::StreamServer { key, endpoint, .. } => tracing::info!("stream: STREAM_SERVER_UPDATE {key} endpoint={endpoint:?}"),
                    VoiceMsg::StreamGone(key) => tracing::info!("stream: STREAM_DELETE {key}"),
                    VoiceMsg::RestartAudio => tracing::info!("audio: devices changed"),
                    VoiceMsg::MicTest(on) => tracing::info!("audio: mic test {on}"),
                }
                let stop_stream = |live: &mut Option<(Driver, u64)>| {
                    if let Some((mut d, rtc)) = live.take() {
                        d.leave();
                        songbird::video::remove_stream_server(rtc);
                    }
                };
                match msg {
                    VoiceMsg::Me(id) => me = Some(id),
                    VoiceMsg::Join { guild, channel } => {
                        if let Some(mut l) = live.take() {
                            l.driver.leave();
                        }
                        pending = Some((guild, channel));
                        session = None;
                        server = None;
                        ui(&out, &ctx, VoiceUi::Connecting);
                    }
                    VoiceMsg::Leave => {
                        if let Some(mut l) = live.take() {
                            l.driver.leave();
                        }
                        if want_stream.take().is_some() || stream_live.is_some() {
                            stop_stream(&mut stream_live);
                            ui(&out, &ctx, VoiceUi::StreamEnded(String::new()));
                        }
                        pending = None;
                        ui(&out, &ctx, VoiceUi::Ended(String::new()));
                    }
                    VoiceMsg::Mute(m) => muted = m,
                    VoiceMsg::Deafen(d) => shared.deaf.store(d, Ordering::Relaxed),
                    VoiceMsg::RestartAudio => {
                        if let Some(l) = live.as_mut() {
                            match start_audio(&shared) {
                                Ok(a) => l._audio = a,
                                Err(e) => tracing::warn!("audio: couldn't switch devices: {e}"),
                            }
                        }
                        if mic_test.is_some() {
                            mic_test = start_audio(&shared).ok();
                        }
                    }
                    VoiceMsg::MicTest(on) => {
                        mic_test = None;
                        if on && live.is_none() {
                            shared.speaker.lock().unwrap().clear();
                            mic_test = start_audio(&shared).ok();
                        }
                    }
                    VoiceMsg::MyState { session_id, channel_id } => match channel_id {
                        Some(_) => session = Some(session_id),
                        None => {
                            if let Some(mut l) = live.take() {
                                l.driver.leave();
                                ui(&out, &ctx, VoiceUi::Ended("You were disconnected from voice".into()));
                            }
                        }
                    },
                    VoiceMsg::Server { token, endpoint } => {
                        if let Some(ep) = endpoint {
                            server = Some((token, ep));
                        }
                    }
                    VoiceMsg::Session(id) => gw_session = Some(id),
                    VoiceMsg::WatchStream { key, streamer: who } => {
                        stop_stream(&mut stream_live);
                        *streamer.lock().unwrap() = who.parse().ok();
                        want_stream = Some((key, who));
                        stream_rtc = None;
                        stream_server = None;
                        ui(&out, &ctx, VoiceUi::StreamConnecting);
                    }
                    VoiceMsg::StopStream => {
                        stop_stream(&mut stream_live);
                        want_stream = None;
                        ui(&out, &ctx, VoiceUi::StreamEnded(String::new()));
                    }
                    VoiceMsg::StreamCreate { key, rtc_server_id } => {
                        if want_stream.as_ref().map_or(false, |w| w.0 == key) {
                            stream_rtc = Some(rtc_server_id);
                        }
                    }
                    VoiceMsg::StreamServer { key, endpoint, token } => {
                        if let (true, Some(ep)) = (want_stream.as_ref().map_or(false, |w| w.0 == key), endpoint) {
                            stream_server = Some((ep, token));
                        }
                    }
                    VoiceMsg::StreamGone(key) => {
                        if want_stream.as_ref().map_or(false, |w| w.0 == key) {
                            stop_stream(&mut stream_live);
                            want_stream = None;
                            ui(&out, &ctx, VoiceUi::StreamEnded("The stream ended".into()));
                        }
                    }
                }

                // Everything needed to watch the stream has arrived?
                if stream_live.is_none() {
                    if let (Some(_), Some(rtc), Some((ep, token)), Some(sid), Some(uid)) =
                        (want_stream.as_ref(), stream_rtc.clone(), stream_server.clone(), gw_session.clone(), me.clone())
                    {
                        stream_server = None;
                        tracing::info!("stream: connecting to {ep}");
                        match connect_stream(&out, &ctx, &rtc, ep, token, sid, &uid).await {
                            Ok(live) => {
                                tracing::info!("stream: connected");
                                stream_live = Some(live);
                                ui(&out, &ctx, VoiceUi::StreamLive);
                            }
                            Err(e) => {
                                tracing::warn!("stream: connect failed: {e}");
                                want_stream = None;
                                ui(&out, &ctx, VoiceUi::StreamEnded(format!("Couldn't open the stream: {e}")));
                            }
                        }
                    }
                }

                // Everything needed to connect has arrived?
                if let (Some((guild, channel)), Some(sid), Some((token, endpoint)), Some(uid)) =
                    (pending.clone(), session.clone(), server.clone(), me.clone())
                {
                    pending = None;
                    tracing::info!("voice: have session + server, connecting to {endpoint}");
                    mic_test = None;
                    match tokio::time::timeout(Duration::from_secs(20), connect(&shared, &out, &ctx, guild, channel, sid, token, endpoint, uid)).await {
                        Ok(Ok(l)) => {
                            tracing::info!("voice: connected");
                            ui(&out, &ctx, VoiceUi::Connected);
                            live = Some(l);
                        }
                        Ok(Err(e)) if e.contains("timed out") => {
                            tracing::warn!("voice: connect failed: {e}");
                            ui(&out, &ctx, VoiceUi::Ended(BLOCKED.into()));
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("voice: connect failed: {e}");
                            ui(&out, &ctx, VoiceUi::Ended(format!("Couldn't join voice: {e}")));
                        }
                        Err(_) => {
                            tracing::warn!("voice: connect timed out");
                            ui(&out, &ctx, VoiceUi::Ended(BLOCKED.into()));
                        }
                    }
                }
            }
            _ = tick.tick() => {
                if let Some(t) = &mic_test {
                    // Mic test: play your own (processed) mic straight back.
                    let mut mic = shared.mic.0.lock().unwrap();
                    let samples: Vec<f32> = mic.drain(..).collect();
                    drop(mic);
                    if t.mic_rate == t.out_rate {
                        let (ic, oc) = (t.mic_channels.max(1) as usize, t.out_channels.max(1) as usize);
                        let mut spk = shared.speaker.lock().unwrap();
                        for frame in samples.chunks(ic) {
                            for c in 0..oc {
                                spk.push_back(frame[c.min(frame.len() - 1)]);
                            }
                        }
                    }
                }
                let Some(l) = live.as_mut() else { continue };
                let level = f32::from_bits(shared.mic_level.swap(0, Ordering::Relaxed));
                let (ptt, key, gate) = {
                    let c = shared.cfg.lock().unwrap();
                    (c.ptt, c.ptt_key, c.gate)
                };
                let open = if muted {
                    false
                } else if ptt {
                    if key_down(key) {
                        last_loud = Instant::now();
                    }
                    last_loud.elapsed() < Duration::from_millis(150)
                } else {
                    if level > gate {
                        last_loud = Instant::now();
                    }
                    last_loud.elapsed() < GATE_HANGOVER
                };
                if open != gate_open {
                    gate_open = open;
                    l.driver.mute(!open);
                    if !open {
                        // throw away stale mic audio so the next word starts fresh
                        shared.mic.0.lock().unwrap().clear();
                    }
                    ui(&out, &ctx, VoiceUi::SelfSpeaking(open));
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect(
    shared: &Shared,
    out: &Sender<Evt>,
    ctx: &egui::Context,
    guild: Option<String>,
    channel: String,
    session_id: String,
    token: String,
    endpoint: String,
    user: String,
) -> Result<Live, String> {
    let channel_id = nz(&channel).ok_or("bad channel id")?;
    // Direct-message calls use the channel id where a server id would go.
    let guild_id = nz(guild.as_deref().unwrap_or(&channel)).ok_or("bad server id")?;
    let user_id = nz(&user).ok_or("bad user id")?;

    shared.speaker.lock().unwrap().clear();
    shared.mic.0.lock().unwrap().clear();
    let audio = start_audio(shared)?;
    tracing::info!(
        "voice: audio ready mic {}Hz x{} / speakers {}Hz x{}",
        audio.mic_rate, audio.mic_channels, audio.out_rate, audio.out_channels
    );

    let config = Config::default().decode_mode(DecodeMode::Decode(DecodeConfig::new(Channels::Stereo, SampleRate::Hz48000)));
    let mut driver = Driver::new(config);
    let handler = Handler(Arc::new(Inner {
        out: out.clone(),
        ctx: ctx.clone(),
        speaker: shared.speaker.clone(),
        deaf: shared.deaf.clone(),
        out_rate: shared.out_rate.clone(),
        out_channels: shared.out_channels.clone(),
        cfg: shared.cfg.clone(),
        ssrc_user: Mutex::new(HashMap::new()),
        last_speaking: Mutex::new(Vec::new()),
    }));
    for ev in [CoreEvent::VoiceTick, CoreEvent::SpeakingStateUpdate, CoreEvent::DriverConnect, CoreEvent::DriverReconnect, CoreEvent::DriverDisconnect] {
        driver.add_global_event(Event::Core(ev), handler.clone());
    }

    let mic: Input = RawAdapter::new(MicSource { buf: shared.mic.clone() }, audio.mic_rate, audio.mic_channels as u32).into();
    driver.play_only_input(mic);
    driver.mute(true); // the voice gate opens it when you talk

    let info = ConnectionInfo {
        channel_id: ChannelId::from(channel_id),
        endpoint,
        guild_id: GuildId::from(guild_id),
        session_id,
        token,
        user_id: UserId::from(user_id),
    };
    driver.connect(info).await.map_err(|e| e.to_string())?;
    Ok(Live { driver, _audio: audio })
}

/// Used by the UI to show who is talking.
pub fn speaking_set(remote: &[String], me: Option<&str>, self_speaking: bool) -> HashSet<String> {
    let mut s: HashSet<String> = remote.iter().cloned().collect();
    if let (true, Some(id)) = (self_speaking, me) {
        s.insert(id.to_string());
    }
    s
}
