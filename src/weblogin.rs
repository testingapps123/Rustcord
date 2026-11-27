// "Log in with the Discord website": Discord's real login page, shown inside the Rustcord window
// only while you sign in. Discord's CAPTCHA (hCaptcha) only works on discord.com, so this is the
// one place it can appear: you solve it yourself on the real page; nothing is faked or skipped.
// When the page has signed you in, we read the session from that page, destroy the web view and
// delete its temporary browser data. Everything else in Rustcord stays native.

use eframe::egui;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use wry::{
    dpi::{LogicalPosition, LogicalSize},
    Rect, WebContext, WebView, WebViewBuilder,
};

/// Discord's page removes `localStorage` from its own scripts; keep a handle before it does.
const KEEP_STORAGE: &str = "try { Object.defineProperty(window, '__rcls', { value: window.localStorage }); } catch (e) {}";
const POLL: &str = "(function(){ try { return window.__rcls ? window.__rcls.getItem('token') : null; } catch (e) { return null; } })()";

fn data_dir() -> PathBuf {
    let base = std::env::var("LOCALAPPDATA").unwrap_or_else(|_| ".".into());
    PathBuf::from(base).join("Rustcord").join("weblogin")
}

fn allowed(url: &str) -> bool {
    if url.starts_with("about:") || url.starts_with("blob:") || url.starts_with("data:") {
        return true;
    }
    let host = url.strip_prefix("https://").and_then(|r| r.split(['/', '?', '#']).next()).unwrap_or("");
    host == "discord.com" || host.ends_with(".discord.com") || host.ends_with("hcaptcha.com")
}

fn to_wry(r: egui::Rect) -> Rect {
    Rect {
        position: LogicalPosition::new(r.min.x as f64, r.min.y as f64).into(),
        size: LogicalSize::new(r.width().max(1.0) as f64, r.height().max(1.0) as f64).into(),
    }
}

pub struct WebLogin {
    // `view` first so it is dropped before its web context.
    view: WebView,
    _context: WebContext,
    found: Arc<Mutex<Option<String>>>,
    last_poll: Instant,
    placed: Option<egui::Rect>,
}

impl WebLogin {
    pub fn start(frame: &eframe::Frame, ctx: &egui::Context) -> Result<Self, String> {
        let _ = std::fs::remove_dir_all(data_dir()); // always start signed-out
        let mut context = WebContext::new(Some(data_dir()));
        let view = WebViewBuilder::with_web_context(&mut context)
            .with_background_color((49, 51, 56, 255))
            .with_initialization_script(KEEP_STORAGE)
            .with_url("https://discord.com/login")
            .with_bounds(to_wry(egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1.0, 1.0))))
            .with_navigation_handler(|url: String| {
                if allowed(&url) {
                    true
                } else {
                    let _ = std::process::Command::new("explorer.exe").arg(&url).spawn();
                    false
                }
            })
            .with_new_window_req_handler(|url: String| {
                let _ = std::process::Command::new("explorer.exe").arg(&url).spawn();
                false
            })
            .build_as_child(frame)
            .map_err(|e| e.to_string())?;
        ctx.request_repaint();
        Ok(Self { view, _context: context, found: Arc::new(Mutex::new(None)), last_poll: Instant::now(), placed: None })
    }

    pub fn place(&mut self, rect: egui::Rect) {
        if self.placed != Some(rect) {
            let _ = self.view.set_bounds(to_wry(rect));
            self.placed = Some(rect);
        }
    }

    /// Returns the session once the page has signed you in.
    pub fn poll(&mut self, ctx: &egui::Context) -> Option<String> {
        if self.last_poll.elapsed() >= Duration::from_millis(800) {
            self.last_poll = Instant::now();
            let found = self.found.clone();
            let ctx2 = ctx.clone();
            let _ = self.view.evaluate_script_with_callback(POLL, move |result: String| {
                // result is JSON: null, or a string holding the JSON-quoted session key
                if let Ok(serde_json::Value::String(stored)) = serde_json::from_str::<serde_json::Value>(&result) {
                    let token = serde_json::from_str::<String>(&stored).unwrap_or(stored);
                    if token.len() > 20 {
                        *found.lock().unwrap() = Some(token);
                        ctx2.request_repaint();
                    }
                }
            });
        }
        ctx.request_repaint_after(Duration::from_millis(800));
        self.found.lock().unwrap().take()
    }
}

impl Drop for WebLogin {
    fn drop(&mut self) {
        // Wipe the temporary browser data once the web engine has let go of it.
        std::thread::spawn(|| {
            for _ in 0..10 {
                std::thread::sleep(Duration::from_secs(1));
                if std::fs::remove_dir_all(data_dir()).is_ok() || !data_dir().exists() {
                    break;
                }
            }
        });
    }
}
