// "Web chat": Discord's own web app shown in Rustcord's chat area, so the messages you type are
// sent by the official client (by you), not by Rustcord. Nothing is injected or automated.
//
// Performance: the web view is created only when first needed, one view is reused for every
// channel (we just change its address), and while it isn't on screen it is hidden and switched
// to WebView2's low-memory mode. Turning web chat off destroys it completely.

use eframe::egui;
use std::path::PathBuf;
use wry::{
    dpi::{LogicalPosition, LogicalSize},
    MemoryUsageLevel, Rect, WebContext, WebView, WebViewBuilder, WebViewBuilderExtWindows, WebViewExtWindows,
};

/// Display-only: hide Discord's own server list and channel sidebar so only the chat / call you
/// picked in Rustcord shows. Discord's code still does all the sending and calling.
const CHAT_ONLY: &str = r#"(function () {
  var css = 'nav[aria-label="Servers sidebar"], [class*="guilds_"] { display: none !important; }'
          + '[class^="sidebar_"], [class*=" sidebar_"] { display: none !important; }'
          // Calls run natively in Rustcord: hide the web page's own call buttons.
          + '[aria-label="Start Voice Call"], [aria-label="Start Video Call"], [aria-label="Join Call"], [aria-label="Join Video Call"] { display: none !important; }'
          // Voice channel chat: only the text chat fills the page; the call view and member list stay hidden under it.
          + 'html.rc-vchat [data-rc-chat] { position: fixed !important; inset: 0 !important; z-index: 2147483000 !important;'
          + ' width: auto !important; max-width: none !important; min-width: 0 !important; background: var(--background-primary, #313338) !important; }'
          + 'html.rc-vchat:not(.rc-has-chat) body::after { content: "Opening chat..."; position: fixed; inset: 0; z-index: 2147483001;'
          + ' display: flex; align-items: center; justify-content: center; color: #949ba4; font: 15px sans-serif; background: #313338; pointer-events: none; }';
  // Set by Rustcord every 2 s (Discord's page removes its storage, so a plain flag is used).
  function voiceMode() { return window.__rcv === true; }
  // Find the voice channel's text chat (message list + message box) and mark it. Light: runs only in voice chat mode.
  setInterval(function () {
    var root = document.documentElement, on = voiceMode();
    root.classList.toggle("rc-vchat", on);
    if (!on) return;
    var cur = document.querySelector("[data-rc-chat]");
    if (cur && cur.isConnected && cur.querySelector('ol[data-list-id="chat-messages"]')) { root.classList.add("rc-has-chat"); return; }
    if (cur) cur.removeAttribute("data-rc-chat");
    root.classList.remove("rc-has-chat");
    var list = document.querySelector('ol[data-list-id="chat-messages"]');
    if (!list) {
      // Discord keeps the voice channel chat behind its speech-bubble button: open it.
      var btns = document.querySelectorAll('[aria-label]');
      for (var i = 0; i < btns.length; i++) {
        var l = (btns[i].getAttribute("aria-label") || "").toLowerCase();
        if ((l === "show chat" || l === "open chat" || l === "chat" || l.indexOf("open chat") === 0) && !btns[i].closest("[data-rc-chat]")) { btns[i].click(); break; }
      }
      return;
    }
    var el = list;
    while (el.parentElement && !el.querySelector('form, [role="textbox"]')) el = el.parentElement;
    el.setAttribute("data-rc-chat", "");
    root.classList.add("rc-has-chat");
  }, 500);
  function add() {
    if (document.getElementById("rustcord-chat-only")) return;
    var s = document.createElement("style");
    s.id = "rustcord-chat-only";
    s.textContent = css;
    (document.head || document.documentElement).appendChild(s);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", add);
  add();
})();"#;

/// Lean WebView2: no GPU process (software drawing is plenty for a chat), one page process,
/// V8 tuned for size, and Edge's background services off. Login uses the same settings
/// (WebView2 requires identical settings for views sharing one profile).
pub fn lean_args() -> String {
    let mut a = String::from(
        "--disable-features=msWebOOUI,msPdfOOUI,msSmartScreenProtection,Translate,OptimizationHints,MediaRouter,SpareRendererForSitePerProcess,BackForwardCache,CalculateNativeWinOcclusion \
         --disable-gpu --disable-gpu-compositing \
         --renderer-process-limit=1 --disable-site-isolation-trials --process-per-site \
         --disable-background-networking --disable-component-update --disable-extensions --disable-sync --no-first-run \
         --js-flags=--optimize-for-size",
    );
    match crate::net::web_proxy() {
        Some(wry::ProxyConfig::Http(e)) => a.push_str(&format!(" --proxy-server=http://{}:{}", e.host, e.port)),
        Some(wry::ProxyConfig::Socks5(e)) => a.push_str(&format!(" --proxy-server=socks5://{}:{}", e.host, e.port)),
        None => {}
    }
    a
}

/// Is the mouse pointer over `rect` of our window? (The web view is a separate child window,
/// so egui doesn't see the pointer there; ask Windows.)
fn pointer_over(ctx: &egui::Context, rect: egui::Rect) -> bool {
    use windows_sys::Win32::{Foundation::POINT, UI::WindowsAndMessaging::GetCursorPos};
    let mut p = POINT { x: 0, y: 0 };
    if unsafe { GetCursorPos(&mut p) } == 0 {
        return true;
    }
    let (inner, ppp) = ctx.input(|i| (i.viewport().inner_rect, i.pixels_per_point()));
    let Some(inner) = inner else { return true };
    let pos = egui::pos2(p.x as f32 / ppp - inner.min.x, p.y as f32 / ppp - inner.min.y);
    rect.contains(pos)
}

/// Seconds since the last keyboard / mouse input anywhere.
fn idle_secs() -> u64 {
    use windows_sys::Win32::{System::SystemInformation::GetTickCount, UI::Input::KeyboardAndMouse::{GetLastInputInfo, LASTINPUTINFO}};
    let mut li = LASTINPUTINFO { cbSize: std::mem::size_of::<LASTINPUTINFO>() as u32, dwTime: 0 };
    if unsafe { GetLastInputInfo(&mut li) } == 0 {
        return 0;
    }
    (unsafe { GetTickCount() }.wrapping_sub(li.dwTime) / 1000) as u64
}

fn data_dir() -> PathBuf {
    let base = std::env::var("LOCALAPPDATA").unwrap_or_else(|_| ".".into());
    PathBuf::from(base).join("Rustcord").join("webchat")
}

fn to_wry(r: egui::Rect) -> Rect {
    Rect {
        position: LogicalPosition::new(r.min.x as f64, r.min.y as f64).into(),
        size: LogicalSize::new(r.width().max(1.0) as f64, r.height().max(1.0) as f64).into(),
    }
}

fn allowed(url: &str) -> bool {
    if url.starts_with("about:") || url.starts_with("blob:") || url.starts_with("data:") {
        return true;
    }
    let host = url.strip_prefix("https://").and_then(|r| r.split(['/', '?', '#']).next()).unwrap_or("");
    host == "discord.com" || host.ends_with(".discord.com") || host.ends_with("hcaptcha.com") || host == "discord.gg"
}

fn open_outside(url: &str) {
    if url.starts_with("https://") || url.starts_with("http://") {
        let _ = std::process::Command::new("explorer.exe").arg(url).spawn();
    }
}

pub struct WebChat {
    // `view` first so it is dropped before its web context.
    view: WebView,
    _context: WebContext,
    url: String,
    placed: Option<egui::Rect>,
    shown: bool,
    /// when it was last hidden (unloaded after a while to free all memory)
    hidden_since: Option<std::time::Instant>,
    /// last time you were using the chat
    last_active: std::time::Instant,
    /// in low-memory rest mode
    resting: bool,
    /// showing a voice channel's chat (only the chat, no call view)
    voice: bool,
    voice_sent: std::time::Instant,
}

impl WebChat {
    pub fn start(frame: &eframe::Frame, url: &str) -> Result<Self, String> {
        let mut context = WebContext::new(Some(data_dir()));
        let mut builder = WebViewBuilder::with_web_context(&mut context);
        if let Some(px) = crate::net::web_proxy() {
            builder = builder.with_proxy_config(px);
        }
        let view = builder
            .with_background_color((49, 51, 56, 255))
            .with_additional_browser_args(&lean_args())
            .with_initialization_script(CHAT_ONLY)
            .with_url(url)
            .with_bounds(to_wry(egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1.0, 1.0))))
            .with_navigation_handler(|u: String| {
                if allowed(&u) {
                    true
                } else {
                    open_outside(&u);
                    false
                }
            })
            .with_new_window_req_handler(|u: String| {
                open_outside(&u);
                false
            })
            .build_as_child(frame)
            .map_err(|e| e.to_string())?;
        Ok(Self { view, _context: context, url: url.to_string(), placed: None, shown: false, hidden_since: None, last_active: std::time::Instant::now(), resting: false, voice: false, voice_sent: std::time::Instant::now() })
    }

    /// Show the chat for `url` in `rect`.
    /// `in_call`: a voice channel / call may be running in the page, so never trim memory.
    pub fn show(&mut self, ctx: &egui::Context, rect: egui::Rect, url: &str, in_call: bool) {
        if self.url != url {
            // Move Discord's app to the channel without reloading it (like clicking a link in it).
            let path = url.strip_prefix("https://discord.com").unwrap_or(url);
            let js = format!(
                "(function(p){{ if (location.pathname === p) return; history.pushState(null, '', p); window.dispatchEvent(new PopStateEvent('popstate')); }})({:?});",
                path
            );
            if self.view.evaluate_script(&js).is_err() {
                let _ = self.view.load_url(url);
            }
            self.url = url.to_string();
        }
        if !self.shown {
            let _ = self.view.set_memory_usage_level(MemoryUsageLevel::Normal);
            let _ = self.view.set_visible(true);
            self.shown = true;
            self.resting = false;
            self.last_active = std::time::Instant::now();
            self.hidden_since = None;
        }
        if self.placed != Some(rect) {
            let _ = self.view.set_bounds(to_wry(rect));
            self.placed = Some(rect);
        }
        // Rest mode: while you aren't using the chat (pointer elsewhere and no input for 10 s),
        // WebView2 runs in low-memory mode; it wakes the moment the pointer is back over it.
        // New messages still arrive while resting. Never during a call running in the page.
        let active = in_call || pointer_over(ctx, rect) && idle_secs() < 10;
        if active {
            self.last_active = std::time::Instant::now();
            if self.resting {
                let _ = self.view.set_memory_usage_level(MemoryUsageLevel::Normal);
                self.resting = false;
            }
        } else if !self.resting && self.last_active.elapsed() >= std::time::Duration::from_secs(10) {
            let _ = self.view.set_memory_usage_level(MemoryUsageLevel::Low);
            self.resting = true;
        }
    }
    /// Voice channel chat mode: only the text chat fills the view.
    pub fn set_voice(&mut self, on: bool) {
        // Resent every 2 s too: a page that was still loading may have missed it.
        if self.voice != on || self.voice_sent.elapsed() >= std::time::Duration::from_secs(2) {
            self.voice_sent = std::time::Instant::now();
            let js = if on { "window.__rcv = true; document.documentElement.classList.add('rc-vchat')" } else { "window.__rcv = false; document.documentElement.classList.remove('rc-vchat', 'rc-has-chat')" };
            if self.view.evaluate_script(js).is_ok() {
                self.voice = on;
            }
        }
    }

    /// Not on screen. During a call it is only shrunk out of the way (stays live, so audio,
    /// video and screen share keep running); otherwise it is hidden and WebView2 frees memory.
    pub fn hide(&mut self, in_call: bool) {
        if in_call {
            let tiny = egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1.0, 1.0));
            if self.placed != Some(tiny) {
                let _ = self.view.set_bounds(to_wry(tiny));
                self.placed = Some(tiny);
            }
            return;
        }
        if self.shown {
            let _ = self.view.set_visible(false);
            let _ = self.view.set_memory_usage_level(MemoryUsageLevel::Low);
            self.shown = false;
            self.placed = None;
            self.hidden_since = Some(std::time::Instant::now());
        }
    }

    /// Hidden long enough that it should be unloaded completely?
    pub fn idle_too_long(&self) -> bool {
        self.hidden_since.map_or(false, |t| t.elapsed() >= std::time::Duration::from_secs(60))
    }
}

/// discord.com address for a channel.
pub fn channel_url(guild: Option<&str>, channel: &str) -> String {
    match guild {
        Some(g) => format!("https://discord.com/channels/{g}/{channel}"),
        None => format!("https://discord.com/channels/@me/{channel}"),
    }
}
