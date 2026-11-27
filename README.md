# Rustcord

A native Rust desktop client for Discord (Windows). No Electron and no web engine for the app
itself: the UI is [egui](https://github.com/emilk/egui), voice is [songbird](https://github.com/serenity-rs/songbird)
with DAVE end-to-end encryption via [davey](https://github.com/Snazzah/davey), and screen shares are decoded with OpenH264.
Typical memory use is ~100–150 MB.

> **Warning:** using a personal account with an unofficial client is against Discord's Terms of Service
> and can get the account restricted or banned. Use at your own risk.

## Features

- **Sign-in** through Discord's real login page (shown inside the window only while signing in;
  CAPTCHA and 2FA work there and are solved by you). Optional bot-token mode.
- **Servers, channels, DMs** with server icons, avatars, member list (roles, status), unread dots and
  @mention badges, Windows notifications, Quick Switcher (`Ctrl+K`).
- **Messages:** Markdown, images, replies, reactions (click to toggle), edit / delete / pin,
  copy text / link / ID, file upload (button or drag-and-drop), emoji picker, typing indicator.
- **Voice:** join voice channels and DM calls, speaking indicators, mute / deafen, per-person volume,
  device selection, Push to Talk (works while games are focused), voice activity with sensitivity,
  noise suppression (nnnoiseless), mic test, voice-channel text chat.
- **Screen share viewing (Go Live):** watch streams in voice channels, with keyframe requests
  (RTCP PLI) for fast start/recovery and late-frame skipping to avoid lag.

Not supported: your own camera / screen share, stream audio, payments/Nitro, account security
settings, in-game overlay.

## Build

Requires Rust (stable, MSVC toolchain), Visual Studio C++ build tools and CMake (bundled with Visual Studio).

```powershell
cargo build --release
```

If the project path is very long or inside a Temp folder, set a short `CARGO_TARGET_DIR` and
`TrackFileAccess=false` before building (libopus is built with CMake/MSBuild).

## Status
everything works except the messages when you send a message discord flagges your account working on it including the file attachment for sending files still working on it



## Notes

- `vendor/songbird` is a patched copy of songbird 0.6.1 (ISC licence, see its `LICENSE.md`):
  video packet hand-off and Go Live stream connections (`src/video.rs`), RTCP keyframe requests,
  and a small junk UDP packet before voice IP discovery for networks that block Discord voice.
- Runtime files live in `%LOCALAPPDATA%\Rustcord\` (saved session, `settings.json`, `log.txt`).
  The log never contains sign-in keys.
