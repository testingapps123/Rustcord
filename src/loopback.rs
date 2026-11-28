// Go Live sound: capture everything your PC is playing EXCEPT Rustcord itself (the call's
// voices, the web chat), like Discord's stream audio. Uses Windows' process loopback capture
// (Windows 10 2004+) in "exclude this process tree" mode, so people in the call never hear
// themselves echoed back through your stream.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;
use windows::core::{implement, Interface, Result as WinResult, HRESULT, IUnknown};
use windows::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
use windows::Win32::Media::Audio::*;
use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_MULTITHREADED};
use windows::Win32::System::Threading::{CreateEventW, WaitForSingleObject};

pub type Pcm = Arc<(Mutex<VecDeque<f32>>, Condvar)>;

#[implement(IActivateAudioInterfaceCompletionHandler)]
struct Done(mpsc::Sender<()>);

impl IActivateAudioInterfaceCompletionHandler_Impl for Done_Impl {
    fn ActivateCompleted(&self, _op: Option<&IActivateAudioInterfaceAsyncOperation>) -> WinResult<()> {
        let _ = self.0.send(());
        Ok(())
    }
}

/// PROPVARIANT holding a VT_BLOB (same memory layout as the Windows struct).
#[repr(C)]
struct BlobVariant {
    vt: u16,
    r1: u16,
    r2: u16,
    r3: u16,
    size: u32,
    data: *const u8,
}

const VT_BLOB: u16 = 65;

/// Start capturing (48 kHz stereo f32 into `out`) until `stop` is set.
pub fn start(out: Pcm, stop: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        if let Err(e) = unsafe { run(&out, &stop) } {
            tracing::warn!("live sound: capture stopped: {e}");
        }
    });
}

unsafe fn run(out: &Pcm, stop: &AtomicBool) -> WinResult<()> {
    let _ = CoInitializeEx(None, COINIT_MULTITHREADED);
    let params = AUDIOCLIENT_ACTIVATION_PARAMS {
        ActivationType: AUDIOCLIENT_ACTIVATION_TYPE_PROCESS_LOOPBACK,
        Anonymous: AUDIOCLIENT_ACTIVATION_PARAMS_0 {
            ProcessLoopbackParams: AUDIOCLIENT_PROCESS_LOOPBACK_PARAMS {
                TargetProcessId: std::process::id(),
                ProcessLoopbackMode: PROCESS_LOOPBACK_MODE_EXCLUDE_TARGET_PROCESS_TREE,
            },
        },
    };
    let pv = BlobVariant {
        vt: VT_BLOB,
        r1: 0,
        r2: 0,
        r3: 0,
        size: std::mem::size_of::<AUDIOCLIENT_ACTIVATION_PARAMS>() as u32,
        data: &params as *const _ as *const u8,
    };
    let (tx, rx) = mpsc::channel();
    let handler: IActivateAudioInterfaceCompletionHandler = Done(tx).into();
    let op = ActivateAudioInterfaceAsync(
        VIRTUAL_AUDIO_DEVICE_PROCESS_LOOPBACK,
        &IAudioClient::IID,
        Some(&pv as *const BlobVariant as *const windows::core::PROPVARIANT),
        &handler,
    )?;
    if rx.recv_timeout(Duration::from_secs(5)).is_err() {
        return Err(windows::core::Error::new(HRESULT(-1), "audio activation timed out"));
    }
    let mut hr = HRESULT(0);
    let mut iface: Option<IUnknown> = None;
    op.GetActivateResult(&mut hr, &mut iface)?;
    hr.ok()?;
    let client: IAudioClient = iface.ok_or_else(|| windows::core::Error::new(HRESULT(-1), "no audio client"))?.cast()?;

    let fmt = WAVEFORMATEX {
        wFormatTag: WAVE_FORMAT_PCM as u16,
        nChannels: 2,
        nSamplesPerSec: 48_000,
        nAvgBytesPerSec: 48_000 * 4,
        nBlockAlign: 4,
        wBitsPerSample: 16,
        cbSize: 0,
    };
    client.Initialize(
        AUDCLNT_SHAREMODE_SHARED,
        AUDCLNT_STREAMFLAGS_LOOPBACK | AUDCLNT_STREAMFLAGS_EVENTCALLBACK | AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM,
        200_000, // 20 ms
        0,
        &fmt,
        None,
    )?;
    let event = CreateEventW(None, false, false, None)?;
    client.SetEventHandle(event)?;
    let capture: IAudioCaptureClient = client.GetService()?;
    client.Start()?;
    tracing::info!("live sound: capturing PC audio (without Rustcord's own sound)");

    let (lock, cv) = &**out;
    while !stop.load(Ordering::Relaxed) {
        if WaitForSingleObject(event, 100) != WAIT_OBJECT_0 {
            continue;
        }
        loop {
            let n = capture.GetNextPacketSize()?;
            if n == 0 {
                break;
            }
            let mut data: *mut u8 = std::ptr::null_mut();
            let mut frames = 0u32;
            let mut flags = 0u32;
            capture.GetBuffer(&mut data, &mut frames, &mut flags, None, None)?;
            let silent = flags & (AUDCLNT_BUFFERFLAGS_SILENT.0 as u32) != 0;
            {
                let mut q = lock.lock().unwrap();
                let samples = frames as usize * 2;
                if silent || data.is_null() {
                    q.extend(std::iter::repeat(0.0).take(samples));
                } else {
                    let s = std::slice::from_raw_parts(data as *const i16, samples);
                    q.extend(s.iter().map(|v| *v as f32 / 32768.0));
                }
                // never more than 0.3 s behind
                let cap = 48_000 * 2 * 3 / 10;
                if q.len() > cap {
                    let extra = q.len() - cap;
                    q.drain(..extra);
                }
            }
            cv.notify_one();
            capture.ReleaseBuffer(frames)?;
        }
    }
    let _ = client.Stop();
    let _ = CloseHandle(event);
    CoUninitialize();
    tracing::info!("live sound: stopped");
    Ok(())
}
