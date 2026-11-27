// Rustcord: a native Rust desktop client for Discord text chat.
// No web engine: egui draws the window, the gateway WebSocket delivers live events,
// and the REST API loads history and sends messages. Calls are handed to Slimcord.
//
// WARNING: using a personal account with an unofficial client is against
// Discord's Terms of Service and can get the account banned.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod qr_login;
mod stream;
mod voice;

use eframe::egui::{self, Align2, Color32, FontId, Rect, RichText, Sense, Stroke, Vec2};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, thread};
use tungstenite::{stream::MaybeTlsStream, Message as WsMessage};

const API: &str = "https://discord.com/api/v10";
const CDN: &str = "https://cdn.discordapp.com";
const GATEWAY: &str = "wss://gateway.discord.gg/?v=10&encoding=json";
const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const MAX_CACHED: usize = 300;
const MAX_IMG: Vec2 = Vec2::new(400.0, 300.0);
// GUILDS | GUILD_MESSAGES | DIRECT_MESSAGES | MESSAGE_CONTENT (bots only)
const BOT_INTENTS: u64 = 1 | (1 << 9) | (1 << 12) | (1 << 15);

// colours
const RAIL: Color32 = Color32::from_rgb(30, 31, 34);
const SIDE: Color32 = Color32::from_rgb(43, 45, 49);
const CHAT: Color32 = Color32::from_rgb(49, 51, 56);
const INPUT: Color32 = Color32::from_rgb(56, 58, 64);
const HOVER: Color32 = Color32::from_rgb(53, 55, 60);
const ACTIVE: Color32 = Color32::from_rgb(64, 66, 73);
const TEXT: Color32 = Color32::from_rgb(219, 222, 225);
const MUTED: Color32 = Color32::from_rgb(148, 155, 164);
const BRIGHT: Color32 = Color32::from_rgb(242, 243, 245);
const ACCENT: Color32 = Color32::from_rgb(88, 101, 242);
const GREEN: Color32 = Color32::from_rgb(35, 165, 90);
const RED: Color32 = Color32::from_rgb(242, 63, 67);

// ---------- data ----------

#[derive(Clone)]
struct Chan {
    id: String,
    name: String,
    kind: u64, // 0 text, 2 voice, 4 category, 5 announcement, 13 stage
    position: i64,
    parent: Option<String>,
}

impl Chan {
    fn is_voice(&self) -> bool {
        self.kind == 2 || self.kind == 13
    }
}

#[derive(Clone)]
struct Guild {
    id: String,
    name: String,
    icon: Option<String>,
    channels: Vec<Chan>, // display order, categories included
    /// role id -> (name, colour)
    roles: HashMap<String, (String, Option<Color32>)>,
}

/// One row of Discord's member list (a group header or a member).
fn member_row(item: &Value, roles: &HashMap<String, (String, Option<Color32>)>) -> Option<ListRow> {
    let g = &item["group"];
    if g.is_object() {
        let id = s(g, "id");
        let name = match id.as_str() {
            "online" => "Online".to_string(),
            "offline" => "Offline".to_string(),
            _ => roles.get(&id).map(|r| r.0.clone()).unwrap_or_else(|| "Members".into()),
        };
        return Some(ListRow::Group { name, count: g["count"].as_u64().unwrap_or(0) });
    }
    let m = &item["member"];
    if !m.is_object() {
        return None;
    }
    let u = &m["user"];
    let name = m["nick"].as_str().filter(|n| !n.is_empty()).map(String::from).unwrap_or_else(|| display_name(u));
    let status = m["presence"]["status"].as_str().unwrap_or("offline").to_string();
    let color = arr(&m["roles"]).iter().filter_map(|r| r.as_str()).filter_map(|r| roles.get(r).and_then(|x| x.1)).next();
    Some(ListRow::Member { name, avatar: avatar_url(u), status, color })
}

/// Apply Discord's GUILD_MEMBER_LIST_UPDATE operations (SYNC/INSERT/UPDATE/DELETE).
fn apply_member_ops(rows: &mut Vec<ListRow>, ops: &Value, roles: &HashMap<String, (String, Option<Color32>)>) {
    for op in arr(ops) {
        let idx = op["index"].as_u64().unwrap_or(0) as usize;
        match op["op"].as_str() {
            Some("SYNC") => {
                let start = op["range"][0].as_u64().unwrap_or(0) as usize;
                let items: Vec<ListRow> = arr(&op["items"]).iter().filter_map(|i| member_row(i, roles)).collect();
                rows.truncate(start.min(rows.len()));
                rows.extend(items);
            }
            Some("INSERT") => {
                if let Some(r) = member_row(&op["item"], roles) {
                    rows.insert(idx.min(rows.len()), r);
                }
            }
            Some("UPDATE") => {
                if let (Some(r), true) = (member_row(&op["item"], roles), idx < rows.len()) {
                    rows[idx] = r;
                }
            }
            Some("DELETE") => {
                if idx < rows.len() {
                    rows.remove(idx);
                }
            }
            Some("INVALIDATE") => {
                if op["range"][0].as_u64() == Some(0) {
                    rows.clear();
                }
            }
            _ => {}
        }
    }
}

#[derive(Clone)]
struct Dm {
    id: String,
    name: String,
    avatar: Option<String>,
    last: u64,
}

#[derive(Clone)]
struct Img {
    url: String,
    size: Vec2,
}

#[derive(Clone)]
struct Reaction {
    label: String,
    /// custom emoji id (None for normal emoji)
    emoji_id: Option<String>,
    image: Option<String>,
    count: u64,
    me: bool,
}

impl Reaction {
    /// How the reaction is written in Discord's API URLs (percent-encoded).
    fn api_key(&self) -> String {
        let raw = match &self.emoji_id {
            Some(id) => format!("{}:{id}", self.label),
            None => self.label.clone(),
        };
        pct(&raw)
    }
}

/// Percent-encode a string for a URL path segment.
fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[derive(Clone)]
struct Msg {
    id: String,
    author_id: String,
    /// message text as typed (for editing / copying)
    raw: String,
    /// this message mentions us (or is a DM to us)
    pings_me: bool,
    author: String,
    avatar: String,
    content: String,
    images: Vec<Img>,
    /// "Today at 3:41 PM"
    time: String,
    /// "3:41 PM" (shown on hover for grouped messages)
    short: String,
    /// "November 27, 2026" (date dividers)
    day: String,
    /// (author, text) of the message this one replies to
    reply: Option<(String, String)>,
    reactions: Vec<Reaction>,
}

/// Discord-style local timestamps.
fn fmt_time(ts: &str) -> (String, String, String) {
    let Ok(t) = chrono::DateTime::parse_from_rfc3339(ts) else {
        return (ts.to_string(), String::new(), String::new());
    };
    let local = t.with_timezone(&chrono::Local);
    let today = chrono::Local::now().date_naive();
    let day = local.date_naive();
    let hm = local.format("%-I:%M %p").to_string();
    let full = if day == today {
        format!("Today at {hm}")
    } else if Some(day) == today.pred_opt() {
        format!("Yesterday at {hm}")
    } else {
        format!("{} {hm}", local.format("%d/%m/%Y"))
    };
    (full, hm, local.format("%B %-d, %Y").to_string())
}

/// Turn raw mention / emoji codes into readable text: <@123> -> @name, <:wave:123> -> :wave:.
fn clean_text(text: &str, m: &Value) -> String {
    let mut out = text.to_string();
    for u in arr(&m["mentions"]) {
        let id = s(u, "id");
        let name = format!("@{}", display_name(u));
        out = out.replace(&format!("<@{id}>"), &name).replace(&format!("<@!{id}>"), &name);
    }
    let mut result = String::with_capacity(out.len());
    let mut rest = out.as_str();
    while let Some(start) = rest.find('<') {
        result.push_str(&rest[..start]);
        let tail = &rest[start..];
        let end = tail.find('>');
        let inner = end.map(|e| &tail[1..e]).unwrap_or("");
        let emoji = inner.strip_prefix("a:").or_else(|| inner.strip_prefix(':'));
        match (end, emoji.and_then(|e| e.split(':').next())) {
            (Some(e), Some(name)) if !name.is_empty() => {
                result.push_str(&format!(":{name}:"));
                rest = &tail[e + 1..];
            }
            _ => {
                result.push('<');
                rest = &tail[1..];
            }
        }
    }
    result.push_str(rest);
    result
}

enum Evt {
    Ready { me: String, me_id: String, me_avatar: String, guilds: Vec<Guild>, dms: Vec<Dm> },
    Voice(voice::VoiceUi),
    VoiceMember { channel: Option<String>, user: String, name: String, avatar: String, streaming: bool },
    NeedUser(Option<String>, String),
    Typing { channel: String, user: String, name: Option<String> },
    MemberList(String, Value),
    UserInfo { user: String, name: String, avatar: String },
    GuildAdd(Guild),
    Message(String, Msg),
    /// channel, message id, raw text, readable text
    Edited(String, String, String, String),
    Deleted(String, String),
    Reacted { ch: String, msg: String, label: String, emoji_id: Option<String>, mine: bool, add: bool },
    History(String, Result<Vec<Msg>, String>),
    Status(String),
    AuthFailed(String),
    Qr(String),
    QrNote(String),
    LoginToken(String),
}

enum Cmd {
    History(String),
    Send { ch: String, text: String, reply_to: Option<String>, files: Vec<PathBuf> },
    Edit(String, String, String),
    Delete(String, String),
    /// channel, message, emoji api key, add?
    React(String, String, String, bool),
    Pin(String, String),
    Ring(String),
    User(String),
}

// ---------- JSON helpers ----------

fn arr(v: &Value) -> &[Value] {
    v.as_array().map(|a| a.as_slice()).unwrap_or(&[])
}

fn s(v: &Value, k: &str) -> String {
    v[k].as_str().unwrap_or("").to_string()
}

fn display_name(u: &Value) -> String {
    u["global_name"]
        .as_str()
        .filter(|x| !x.is_empty())
        .or(u["username"].as_str())
        .unwrap_or("unknown")
        .to_string()
}

fn avatar_url(u: &Value) -> String {
    let id = s(u, "id");
    match u["avatar"].as_str() {
        Some(h) => format!("{CDN}/avatars/{id}/{h}.png?size=64"),
        None => {
            let idx = id.parse::<u64>().map(|n| (n >> 22) % 6).unwrap_or(0);
            format!("{CDN}/embed/avatars/{idx}.png")
        }
    }
}

/// Ask Discord's media proxy for a small copy, so big photos don't eat memory.
fn sized_image(proxy: &str, w: f32, h: f32) -> Option<Img> {
    if proxy.is_empty() {
        return None;
    }
    let (w, h) = if w > 0.0 && h > 0.0 { (w, h) } else { (MAX_IMG.x, MAX_IMG.y) };
    let scale = (MAX_IMG.x / w).min(MAX_IMG.y / h).min(1.0);
    let size = Vec2::new((w * scale).round().max(16.0), (h * scale).round().max(16.0));
    let sep = if proxy.contains('?') { '&' } else { '?' };
    Some(Img { url: format!("{proxy}{sep}width={}&height={}", size.x as u32, size.y as u32), size })
}

fn parse_msg(m: &Value) -> Msg {
    let mut content = clean_text(&s(m, "content"), m);
    let mut images = Vec::new();
    for a in arr(&m["attachments"]) {
        let name = s(a, "filename");
        let lower = name.to_lowercase();
        let is_image = a["content_type"].as_str().map_or(false, |t| t.starts_with("image/"))
            || [".png", ".jpg", ".jpeg", ".gif", ".webp"].iter().any(|e| lower.ends_with(e));
        let w = a["width"].as_f64().unwrap_or(0.0) as f32;
        let h = a["height"].as_f64().unwrap_or(0.0) as f32;
        match (is_image, sized_image(&s(a, "proxy_url"), w, h)) {
            (true, Some(img)) => images.push(img),
            _ => {
                if !content.is_empty() {
                    content.push('\n');
                }
                content.push_str(&format!("📎 {name}"));
            }
        }
    }
    for e in arr(&m["embeds"]) {
        let pic = if e["image"].is_object() { &e["image"] } else if matches!(e["type"].as_str(), Some("image" | "gifv")) { &e["thumbnail"] } else { &Value::Null };
        let w = pic["width"].as_f64().unwrap_or(0.0) as f32;
        let h = pic["height"].as_f64().unwrap_or(0.0) as f32;
        if let Some(img) = sized_image(pic["proxy_url"].as_str().unwrap_or(""), w, h) {
            images.push(img);
        } else if let Some(title) = e["title"].as_str() {
            if !content.is_empty() {
                content.push('\n');
            }
            content.push_str(&format!("▌{title}"));
        }
    }
    if content.is_empty() && images.is_empty() {
        content = if !arr(&m["sticker_items"]).is_empty() { "[sticker]".into() } else { "[system message]".into() };
    }
    // A message that is only an image link: hide the raw link.
    if !images.is_empty() && content.starts_with("http") && !content.contains(char::is_whitespace) {
        content.clear();
    }
    let (time, short, day) = fmt_time(&s(m, "timestamp"));
    let r = &m["referenced_message"];
    let reply = r.is_object().then(|| {
        let text = clean_text(&s(r, "content"), r);
        let mut snippet: String = text.chars().take(90).collect();
        if snippet.is_empty() {
            snippet = "Click to see attachment".into();
        }
        (display_name(&r["author"]), snippet.replace('\n', " "))
    });
    let reactions = arr(&m["reactions"])
        .iter()
        .map(|r| {
            let e = &r["emoji"];
            Reaction {
                label: s(e, "name"),
                emoji_id: e["id"].as_str().map(String::from),
                image: e["id"].as_str().map(|id| format!("{CDN}/emojis/{id}.png?size=32")),
                count: r["count"].as_u64().unwrap_or(1),
                me: r["me"].as_bool().unwrap_or(false),
            }
        })
        .collect();
    Msg {
        id: s(m, "id"),
        author_id: s(&m["author"], "id"),
        raw: s(m, "content"),
        pings_me: false,
        author: display_name(&m["author"]),
        avatar: avatar_url(&m["author"]),
        content,
        images,
        time,
        short,
        day,
        reply,
        reactions,
    }
}

fn parse_guild(g: &Value) -> Option<Guild> {
    if g["unavailable"].as_bool() == Some(true) {
        return None;
    }
    let id = s(g, "id");
    let name = g["name"].as_str().or(g["properties"]["name"].as_str())?.to_string();
    let icon = g["icon"]
        .as_str()
        .or(g["properties"]["icon"].as_str())
        .map(|h| format!("{CDN}/icons/{id}/{h}.png?size=96"));
    let mut all: Vec<Chan> = arr(&g["channels"])
        .iter()
        .filter_map(|c| {
            let kind = c["type"].as_u64()?;
            if ![0, 2, 4, 5, 13].contains(&kind) {
                return None;
            }
            Some(Chan {
                id: s(c, "id"),
                name: s(c, "name"),
                kind,
                position: c["position"].as_i64().unwrap_or(0),
                parent: c["parent_id"].as_str().map(String::from),
            })
        })
        .collect();
    // Discord lists text channels above voice channels inside each category.
    all.sort_by_key(|c| (c.is_voice(), c.position));

    let is_cat = |id: &str| all.iter().any(|x| x.kind == 4 && x.id == id);
    let mut ordered: Vec<Chan> = all
        .iter()
        .filter(|c| c.kind != 4 && c.parent.as_deref().map_or(true, |p| !is_cat(p)))
        .cloned()
        .collect();
    let mut cats: Vec<&Chan> = all.iter().filter(|c| c.kind == 4).collect();
    cats.sort_by_key(|c| c.position);
    for cat in cats {
        let kids: Vec<Chan> = all
            .iter()
            .filter(|c| c.kind != 4 && c.parent.as_deref() == Some(cat.id.as_str()))
            .cloned()
            .collect();
        if !kids.is_empty() {
            ordered.push(cat.clone());
            ordered.extend(kids);
        }
    }
    let roles = arr(&g["roles"])
        .iter()
        .map(|r| {
            let c = r["color"].as_u64().unwrap_or(0) as u32;
            let color = (c != 0).then(|| Color32::from_rgb((c >> 16) as u8, (c >> 8) as u8, c as u8));
            (s(r, "id"), (s(r, "name"), color))
        })
        .collect();
    Some(Guild { id, name, icon, channels: ordered, roles })
}

fn parse_dm(c: &Value, users: &HashMap<String, (String, String)>) -> Dm {
    let id = s(c, "id");
    let mut people: Vec<(String, String)> = arr(&c["recipients"]).iter().map(|u| (display_name(u), avatar_url(u))).collect();
    if people.is_empty() {
        people = arr(&c["recipient_ids"])
            .iter()
            .filter_map(|i| i.as_str())
            .map(|i| users.get(i).cloned().unwrap_or_else(|| ("unknown".into(), format!("{CDN}/embed/avatars/0.png"))))
            .collect();
    }
    let name = c["name"].as_str().filter(|n| !n.is_empty()).map(String::from).unwrap_or_else(|| {
        if people.is_empty() { "Direct message".into() } else { people.iter().map(|p| p.0.clone()).collect::<Vec<_>>().join(", ") }
    });
    let avatar = match c["icon"].as_str() {
        Some(h) => Some(format!("{CDN}/channel-icons/{id}/{h}.png?size=64")),
        None if people.len() == 1 => Some(people[0].1.clone()),
        None => None,
    };
    Dm { id, name, avatar, last: c["last_message_id"].as_str().and_then(|x| x.parse().ok()).unwrap_or(0) }
}

// ---------- gateway (live events) ----------

enum GwErr {
    Auth,
    Other(String),
}

type VoiceTx = tokio::sync::mpsc::UnboundedSender<voice::VoiceMsg>;

fn gateway_loop(token: String, bot: bool, out: Sender<Evt>, inbox: Receiver<String>, ctx: egui::Context, vtx: VoiceTx) {
    let mut delay = 5u64;
    let mut me_id = String::new();
    loop {
        let _ = out.send(Evt::Status("Connecting to Discord...".into()));
        ctx.request_repaint();
        match run_gateway(&token, bot, &out, &inbox, &ctx, &mut delay, &vtx, &mut me_id) {
            GwErr::Auth => {
                let _ = out.send(Evt::AuthFailed("Discord signed this device out. Please sign in again.".into()));
                ctx.request_repaint();
                return;
            }
            GwErr::Other(e) => {
                let _ = out.send(Evt::Status(format!("Disconnected ({e}). Reconnecting in {delay}s...")));
                ctx.request_repaint();
            }
        }
        thread::sleep(Duration::from_secs(delay));
        delay = (delay * 2).min(60);
    }
}

fn run_gateway(
    token: &str,
    bot: bool,
    out: &Sender<Evt>,
    inbox: &Receiver<String>,
    ctx: &egui::Context,
    delay: &mut u64,
    vtx: &VoiceTx,
    me_id: &mut String,
) -> GwErr {
    let (mut ws, _) = match tungstenite::connect(GATEWAY) {
        Ok(x) => x,
        Err(e) => return GwErr::Other(e.to_string()),
    };
    let _ = match ws.get_mut() {
        MaybeTlsStream::NativeTls(s) => s.get_mut().set_read_timeout(Some(Duration::from_millis(250))),
        MaybeTlsStream::Plain(s) => s.set_read_timeout(Some(Duration::from_millis(250))),
        _ => Ok(()),
    };

    let mut interval = Duration::from_secs(40);
    let mut seq: Option<u64> = None;
    let mut last_beat = Instant::now();
    let mut identified = false;

    loop {
        if identified && last_beat.elapsed() >= interval {
            if let Err(e) = ws.send(WsMessage::Text(json!({ "op": 1, "d": seq }).to_string())) {
                return GwErr::Other(e.to_string());
            }
            last_beat = Instant::now();
        }
        while let Ok(payload) = inbox.try_recv() {
            if identified {
                if let Err(e) = ws.send(WsMessage::Text(payload)) {
                    return GwErr::Other(e.to_string());
                }
            }
        }

        match ws.read() {
            Ok(WsMessage::Text(text)) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Some(n) = v["s"].as_u64() {
                    seq = Some(n);
                }
                match v["op"].as_u64().unwrap_or(u64::MAX) {
                    10 => {
                        interval = Duration::from_millis(v["d"]["heartbeat_interval"].as_u64().unwrap_or(41_250));
                        let mut d = json!({
                            "token": token,
                            "properties": { "os": "Windows", "browser": "Chrome", "device": "" },
                        });
                        if bot {
                            d["intents"] = json!(BOT_INTENTS);
                        }
                        if let Err(e) = ws.send(WsMessage::Text(json!({ "op": 2, "d": d }).to_string())) {
                            return GwErr::Other(e.to_string());
                        }
                        identified = true;
                        last_beat = Instant::now();
                    }
                    0 => {
                        dispatch(v["t"].as_str().unwrap_or(""), &v["d"], out, delay, vtx, me_id);
                        ctx.request_repaint();
                    }
                    1 => {
                        let _ = ws.send(WsMessage::Text(json!({ "op": 1, "d": seq }).to_string()));
                        last_beat = Instant::now();
                    }
                    7 => return GwErr::Other("Discord asked to reconnect".into()),
                    9 => return GwErr::Other("session expired".into()),
                    _ => {}
                }
            }
            Ok(WsMessage::Close(frame)) => {
                let code = frame.map(|f| u16::from(f.code)).unwrap_or(0);
                return match code {
                    4004 => GwErr::Auth,
                    4014 => GwErr::Other("this bot needs Message Content Intent turned on".into()),
                    _ => GwErr::Other(format!("closed with code {code}")),
                };
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
            Err(e) => return GwErr::Other(e.to_string()),
        }
    }
}

fn dispatch(t: &str, d: &Value, out: &Sender<Evt>, delay: &mut u64, vtx: &VoiceTx, me_id: &mut String) {
    match t {
        "READY" => {
            *delay = 5;
            *me_id = s(&d["user"], "id");
            let _ = vtx.send(voice::VoiceMsg::Me(me_id.clone()));
            let _ = vtx.send(voice::VoiceMsg::Session(s(d, "session_id")));
            let mut users = HashMap::new();
            for u in arr(&d["users"]) {
                users.insert(s(u, "id"), (display_name(u), avatar_url(u)));
            }
            users.insert(me_id.clone(), (display_name(&d["user"]), avatar_url(&d["user"])));
            let guilds = arr(&d["guilds"]).iter().filter_map(parse_guild).collect();
            let mut dms: Vec<Dm> = arr(&d["private_channels"]).iter().map(|c| parse_dm(c, &users)).collect();
            dms.sort_by(|a, b| b.last.cmp(&a.last));
            let _ = out.send(Evt::Ready {
                me: display_name(&d["user"]),
                me_id: me_id.clone(),
                me_avatar: avatar_url(&d["user"]),
                guilds,
                dms,
            });
            // Who is already sitting in voice channels.
            for g in arr(&d["guilds"]) {
                for vs in arr(&g["voice_states"]) {
                    let user = s(vs, "user_id");
                    let (name, avatar) = match users.get(&user) {
                        Some(x) => x.clone(),
                        None => {
                            let _ = out.send(Evt::NeedUser(Some(s(g, "id")), user.clone()));
                            ("…".to_string(), format!("{CDN}/embed/avatars/0.png"))
                        }
                    };
                    let streaming = vs["self_stream"].as_bool().unwrap_or(false);
                    if streaming {
                        tracing::info!("gateway: READY says user {user} is streaming in {:?}", vs["channel_id"]);
                    }
                    let _ = out.send(Evt::VoiceMember { channel: vs["channel_id"].as_str().map(String::from), user, name, avatar, streaming });
                }
            }
        }
        "VOICE_STATE_UPDATE" => {
            let user = s(d, "user_id");
            let channel = d["channel_id"].as_str().map(String::from);
            if user == *me_id {
                tracing::info!("gateway: my voice state -> channel {channel:?}");
                let _ = vtx.send(voice::VoiceMsg::MyState { session_id: s(d, "session_id"), channel_id: channel.clone() });
            }
            let u = &d["member"]["user"];
            if !u.is_object() && user != *me_id {
                let _ = out.send(Evt::NeedUser(d["guild_id"].as_str().map(String::from), user.clone()));
            }
            let name = d["member"]["nick"].as_str().map(String::from).unwrap_or_else(|| {
                if u.is_object() { display_name(u) } else { "…".into() }
            });
            let avatar = if u.is_object() { avatar_url(u) } else { format!("{CDN}/embed/avatars/0.png") };
            let streaming = d["self_stream"].as_bool().unwrap_or(false);
            tracing::info!(
                "gateway: voice state user={user} channel={channel:?} self_stream={:?} self_video={:?}",
                d["self_stream"], d["self_video"]
            );
            let _ = out.send(Evt::VoiceMember { channel, user, name, avatar, streaming });
        }
        "TYPING_START" => {
            let user = s(d, "user_id");
            if user != *me_id {
                let u = &d["member"]["user"];
                let name = d["member"]["nick"].as_str().map(String::from).filter(|n| !n.is_empty()).or_else(|| u.is_object().then(|| display_name(u)));
                let _ = out.send(Evt::Typing { channel: s(d, "channel_id"), user, name });
            }
        }
        "GUILD_MEMBER_LIST_UPDATE" => {
            let _ = out.send(Evt::MemberList(s(d, "guild_id"), d["ops"].clone()));
        }
        "STREAM_CREATE" => {
            let _ = vtx.send(voice::VoiceMsg::StreamCreate { key: s(d, "stream_key"), rtc_server_id: s(d, "rtc_server_id") });
        }
        "STREAM_SERVER_UPDATE" => {
            let _ = vtx.send(voice::VoiceMsg::StreamServer {
                key: s(d, "stream_key"),
                endpoint: d["endpoint"].as_str().map(String::from),
                token: s(d, "token"),
            });
        }
        "STREAM_DELETE" => {
            let _ = vtx.send(voice::VoiceMsg::StreamGone(s(d, "stream_key")));
        }
        "GUILD_MEMBERS_CHUNK" => {
            for m in arr(&d["members"]) {
                let u = &m["user"];
                if !u.is_object() {
                    continue;
                }
                let name = m["nick"].as_str().filter(|n| !n.is_empty()).map(String::from).unwrap_or_else(|| display_name(u));
                let _ = out.send(Evt::UserInfo { user: s(u, "id"), name, avatar: avatar_url(u) });
            }
        }
        "VOICE_SERVER_UPDATE" => {
            tracing::info!("gateway: VOICE_SERVER_UPDATE received");
            let _ = vtx.send(voice::VoiceMsg::Server {
                token: s(d, "token"),
                endpoint: d["endpoint"].as_str().map(String::from),
            });
        }
        "GUILD_CREATE" => {
            if let Some(g) = parse_guild(d) {
                let _ = out.send(Evt::GuildAdd(g));
            }
        }
        "MESSAGE_CREATE" => {
            let mut m = parse_msg(d);
            let mentioned = arr(&d["mentions"]).iter().any(|u| s(u, "id") == *me_id) || d["mention_everyone"].as_bool() == Some(true);
            let is_dm = d["guild_id"].is_null();
            m.pings_me = m.author_id != *me_id && (mentioned || is_dm);
            let _ = out.send(Evt::Message(s(d, "channel_id"), m));
        }
        "MESSAGE_UPDATE" => {
            if d["content"].is_string() {
                let raw = s(d, "content");
                let shown = clean_text(&raw, d);
                let _ = out.send(Evt::Edited(s(d, "channel_id"), s(d, "id"), raw, shown));
            }
        }
        "MESSAGE_REACTION_ADD" | "MESSAGE_REACTION_REMOVE" => {
            let e = &d["emoji"];
            let _ = out.send(Evt::Reacted {
                ch: s(d, "channel_id"),
                msg: s(d, "message_id"),
                label: s(e, "name"),
                emoji_id: e["id"].as_str().map(String::from),
                mine: s(d, "user_id") == *me_id,
                add: t == "MESSAGE_REACTION_ADD",
            });
        }
        "MESSAGE_DELETE" => {
            let _ = out.send(Evt::Deleted(s(d, "channel_id"), s(d, "id")));
        }
        _ => {}
    }
}

// ---------- REST (history + sending) ----------

fn rest_worker(auth: String, rx: Receiver<Cmd>, out: Sender<Evt>, ctx: egui::Context) {
    let client = reqwest::blocking::Client::builder()
        .user_agent(UA)
        .timeout(Duration::from_secs(20))
        .build()
        .expect("could not build HTTP client");

    let check = |resp: reqwest::blocking::Response| -> Result<Value, String> {
        let status = resp.status();
        if status.is_success() {
            // 204 No Content (reactions, deletes, pins) has no body.
            return Ok(resp.json::<Value>().unwrap_or(Value::Null));
        }
        Err(match status.as_u16() {
            401 => "You were signed out. Please sign in again.".into(),
            403 => "You don't have access to this channel.".into(),
            429 => "Discord says slow down. Try again in a few seconds.".into(),
            _ => format!("Discord returned {status}"),
        })
    };

    for cmd in rx {
        let evt = match cmd {
            Cmd::History(ch) => {
                let r = client
                    .get(format!("{API}/channels/{ch}/messages?limit=50"))
                    .header("Authorization", &auth)
                    .send()
                    .map_err(|e| e.to_string())
                    .and_then(check)
                    .map(|v| {
                        let mut msgs: Vec<Msg> = arr(&v).iter().map(parse_msg).collect();
                        msgs.reverse();
                        msgs
                    });
                Evt::History(ch, r)
            }
            Cmd::Send { ch, text, reply_to, files } => {
                let mut payload = json!({ "content": text });
                if let Some(id) = reply_to {
                    payload["message_reference"] = json!({ "message_id": id });
                }
                let req = client.post(format!("{API}/channels/{ch}/messages")).header("Authorization", &auth);
                let req = if files.is_empty() {
                    Ok(req.json(&payload))
                } else {
                    // Attachments: multipart form with payload_json + files[n].
                    payload["attachments"] = json!(files
                        .iter()
                        .enumerate()
                        .map(|(i, p)| json!({ "id": i, "filename": p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default() }))
                        .collect::<Vec<_>>());
                    let mut form = reqwest::blocking::multipart::Form::new().text("payload_json", payload.to_string());
                    let mut err = None;
                    for (i, p) in files.iter().enumerate() {
                        match form.file(format!("files[{i}]"), p) {
                            Ok(f) => form = f,
                            Err(e) => {
                                err = Some(format!("couldn't read {}: {e}", p.display()));
                                form = reqwest::blocking::multipart::Form::new();
                                break;
                            }
                        }
                    }
                    match err {
                        Some(e) => Err(e),
                        None => Ok(req.multipart(form)),
                    }
                };
                match req.and_then(|r| r.send().map_err(|e| e.to_string())).and_then(check) {
                    Ok(v) => Evt::Message(ch, parse_msg(&v)),
                    Err(e) => Evt::Status(format!("Message not sent: {e}")),
                }
            }
            Cmd::Edit(ch, id, text) => {
                match client
                    .patch(format!("{API}/channels/{ch}/messages/{id}"))
                    .header("Authorization", &auth)
                    .json(&json!({ "content": text }))
                    .send()
                    .map_err(|e| e.to_string())
                    .and_then(check)
                {
                    Ok(_) => continue, // MESSAGE_UPDATE from the gateway updates the screen
                    Err(e) => Evt::Status(format!("Couldn't edit: {e}")),
                }
            }
            Cmd::Delete(ch, id) => {
                match client.delete(format!("{API}/channels/{ch}/messages/{id}")).header("Authorization", &auth).send().map_err(|e| e.to_string()).and_then(check) {
                    Ok(_) => continue,
                    Err(e) => Evt::Status(format!("Couldn't delete: {e}")),
                }
            }
            Cmd::React(ch, id, key, add) => {
                let url = format!("{API}/channels/{ch}/messages/{id}/reactions/{key}/@me");
                let req = if add { client.put(url) } else { client.delete(url) };
                match req.header("Authorization", &auth).header("Content-Length", "0").send().map_err(|e| e.to_string()).and_then(check) {
                    Ok(_) => continue,
                    Err(e) => Evt::Status(format!("Couldn't react: {e}")),
                }
            }
            Cmd::Pin(ch, id) => {
                match client.put(format!("{API}/channels/{ch}/pins/{id}")).header("Authorization", &auth).header("Content-Length", "0").send().map_err(|e| e.to_string()).and_then(check) {
                    Ok(_) => Evt::Status("Message pinned".into()),
                    Err(e) => Evt::Status(format!("Couldn't pin: {e}")),
                }
            }
            Cmd::Ring(ch) => {
                let _ = client
                    .post(format!("{API}/channels/{ch}/call/ring"))
                    .header("Authorization", &auth)
                    .json(&json!({ "recipients": null }))
                    .send();
                continue;
            }
            Cmd::User(id) => {
                match client.get(format!("{API}/users/{id}")).header("Authorization", &auth).send().map_err(|e| e.to_string()).and_then(check) {
                    Ok(u) => Evt::UserInfo { user: id, name: display_name(&u), avatar: avatar_url(&u) },
                    Err(_) => continue,
                }
            }
        };
        let _ = out.send(evt);
        ctx.request_repaint();
    }
}

enum VAction {
    Join(Option<String>, String, String),
    Leave,
    ToggleMute,
    ToggleDeaf,
    /// stream key, streamer user id, streamer name
    Watch(String, String, String),
    StopWatch,
}

struct VoiceMember {
    user: String,
    name: String,
    avatar: String,
    streaming: bool,
}

#[derive(PartialEq)]
enum VoicePhase {
    Off,
    Connecting,
    Connected,
}

// ---------- small custom widgets ----------

fn initials(name: &str) -> String {
    let s: String = name.split_whitespace().filter_map(|w| w.chars().next()).take(3).collect();
    if s.is_empty() { "?".into() } else { s }
}

/// A round server button for the left rail, Discord-style.
fn rail_button(ui: &mut egui::Ui, selected: bool, unread: bool, badge: u32, icon: Option<&str>, label: &str, tip: &str) -> bool {
    let (rect, resp) = ui.allocate_exact_size(Vec2::splat(48.0), Sense::click());
    let hovered = resp.hovered();
    let r = if selected || hovered { 16.0 } else { 24.0 };
    let painter = ui.painter().clone();
    let pill = if selected { 40.0 } else if hovered { 20.0 } else if unread { 8.0 } else { 0.0 };
    if pill > 0.0 {
        let x = ui.max_rect().left() - 10.0;
        painter.rect_filled(Rect::from_center_size(egui::pos2(x, rect.center().y), Vec2::new(4.0, pill)), 2.0, BRIGHT);
    }
    match icon {
        Some(url) => {
            egui::Image::new(url).rounding(r).paint_at(ui, rect);
        }
        None => {
            painter.rect_filled(rect, r, if selected || hovered { ACCENT } else { CHAT });
            painter.text(rect.center(), Align2::CENTER_CENTER, label, FontId::proportional(15.0), BRIGHT);
        }
    }
    mention_badge(&painter, rect.right_bottom() - Vec2::new(2.0, 2.0), badge);
    resp.on_hover_text(tip).clicked()
}

/// Discord Markdown: **bold**, *italic*, __underline__, ~~strike~~, `code`, ||spoiler||,
/// > quotes and ``` code blocks ```.
fn markdown(ui: &mut egui::Ui, text: &str) {
    for (i, part) in text.split("```").enumerate() {
        if i % 2 == 1 {
            // Code block (first line may be a language name).
            let body = match part.split_once('\n') {
                Some((lang, rest)) if !lang.contains(' ') && !lang.is_empty() => rest,
                _ => part,
            };
            egui::Frame::none().fill(Color32::from_rgb(43, 45, 49)).stroke(Stroke::new(1.0_f32, RAIL)).rounding(4.0).inner_margin(8.0).show(ui, |ui| {
                ui.set_min_width(ui.available_width().min(500.0));
                ui.label(RichText::new(body.trim_end()).monospace().size(13.5).color(TEXT));
            });
            continue;
        }
        let part = part.trim_matches('\n');
        if part.is_empty() {
            continue;
        }
        for line in part.split('\n') {
            let (quote, line) = match line.strip_prefix("> ") {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let job = inline_job(line);
            if quote {
                ui.horizontal(|ui| {
                    let (r, _) = ui.allocate_exact_size(Vec2::new(4.0, 20.0), Sense::hover());
                    ui.painter().rect_filled(r, 2.0, Color32::from_rgb(78, 80, 88));
                    ui.add_space(6.0);
                    ui.label(job);
                });
            } else {
                ui.label(job);
            }
        }
    }
}

fn inline_job(line: &str) -> egui::text::LayoutJob {
    use egui::text::{LayoutJob, TextFormat};
    let mut job = LayoutJob::default();
    let (mut bold, mut italic, mut under, mut strike, mut code, mut spoiler) = (false, false, false, false, false, false);
    let mut buf = String::new();
    let push = |job: &mut LayoutJob, buf: &mut String, b: bool, it: bool, u: bool, st: bool, c: bool, sp: bool| {
        if buf.is_empty() {
            return;
        }
        let family = if c {
            egui::FontFamily::Monospace
        } else if b {
            egui::FontFamily::Name("bold".into())
        } else if it {
            egui::FontFamily::Name("italic".into())
        } else {
            egui::FontFamily::Proportional
        };
        let mut f = TextFormat { font_id: FontId::new(if c { 13.5 } else { 15.0 }, family), color: TEXT, ..Default::default() };
        if c {
            f.background = Color32::from_rgb(43, 45, 49);
        }
        if sp {
            f.background = Color32::from_rgb(30, 31, 34);
            f.color = Color32::from_rgb(120, 124, 132);
        }
        if u {
            f.underline = Stroke::new(1.0_f32, TEXT);
        }
        if st {
            f.strikethrough = Stroke::new(1.0_f32, TEXT);
        }
        job.append(&std::mem::take(buf), 0.0, f);
    };
    let mut rest = line;
    while !rest.is_empty() {
        let toggles: [(&str, u8); 6] = [("**", 0), ("__", 1), ("~~", 2), ("||", 3), ("`", 4), ("*", 5)];
        let hit = if code {
            rest.starts_with('`').then_some(("`", 4u8))
        } else {
            toggles.iter().copied().find(|(t, _)| rest.starts_with(t))
        };
        match hit {
            Some((tok, kind)) => {
                push(&mut job, &mut buf, bold, italic, under, strike, code, spoiler);
                match kind {
                    0 => bold = !bold,
                    1 => under = !under,
                    2 => strike = !strike,
                    3 => spoiler = !spoiler,
                    4 => code = !code,
                    _ => italic = !italic,
                }
                rest = &rest[tok.len()..];
            }
            None => {
                let c = rest.chars().next().unwrap();
                buf.push(c);
                rest = &rest[c.len_utf8()..];
            }
        }
    }
    push(&mut job, &mut buf, bold, italic, under, strike, code, spoiler);
    job
}

/// Mic level meter (green), with an optional sensitivity marker (white line).
fn level_bar(ui: &mut egui::Ui, level: f32, gate: Option<f32>) {
    let w = ui.available_width().min(440.0);
    let (r, _) = ui.allocate_exact_size(Vec2::new(w, 10.0), Sense::hover());
    // perceptual scale so quiet speech still moves the bar
    let scale = |x: f32| (x.max(0.0).sqrt()).min(1.0);
    let p = ui.painter();
    p.rect_filled(r, 5.0, INPUT);
    let fill = Rect::from_min_size(r.min, Vec2::new(r.width() * scale(level), r.height()));
    let over = gate.map_or(true, |g| level >= g);
    p.rect_filled(fill, 5.0, if over { GREEN } else { Color32::from_rgb(240, 178, 50) });
    if let Some(g) = gate {
        let x = r.left() + r.width() * scale(g);
        p.vline(x, r.y_range().expand(3.0), Stroke::new(2.0_f32, Color32::WHITE));
    }
}

/// Round profile picture with a green ring while that person is talking.
fn avatar_ring(ui: &mut egui::Ui, url: &str, size: f32, speaking: bool) {
    let (rect, _) = ui.allocate_exact_size(Vec2::splat(size), Sense::hover());
    egui::Image::new(url).rounding(size / 2.0).paint_at(ui, rect);
    if speaking {
        ui.painter().circle_stroke(rect.center(), size / 2.0 + 1.0, Stroke::new(2.0_f32, GREEN));
    }
}

/// One person in the member list: picture with a status dot, name in their role colour.
fn member_list_row(ui: &mut egui::Ui, name: &str, avatar: &str, status: &str, color: Option<Color32>) {
    let w = ui.available_width();
    let (rect, resp) = ui.allocate_exact_size(Vec2::new(w, 42.0), Sense::hover());
    let p = ui.painter().clone();
    if resp.hovered() {
        p.rect_filled(rect, 4.0, HOVER);
    }
    let offline = status == "offline" || status == "invisible";
    let face = Rect::from_min_size(egui::pos2(rect.left() + 8.0, rect.center().y - 16.0), Vec2::splat(32.0));
    egui::Image::new(avatar).rounding(16.0).tint(if offline { Color32::from_white_alpha(110) } else { Color32::WHITE }).paint_at(ui, face);
    if !offline {
        let dot = match status {
            "idle" => Color32::from_rgb(240, 178, 50),
            "dnd" => RED,
            _ => GREEN,
        };
        let c = face.right_bottom() - Vec2::splat(4.0);
        p.circle_filled(c, 6.0, SIDE);
        p.circle_filled(c, 4.5, dot);
    }
    let col = if offline { Color32::from_rgb(100, 104, 112) } else { color.unwrap_or(TEXT) };
    let g = p.layout(name.to_string(), FontId::proportional(15.0), col, rect.right() - face.right() - 18.0);
    p.galley(egui::pos2(face.right() + 12.0, rect.center().y - g.size().y / 2.0), g, col);
}

/// A full-width list row (channel or DM), left aligned.
/// Red "@mention count" bubble, centred on `at`.
fn mention_badge(p: &egui::Painter, at: egui::Pos2, count: u32) {
    if count == 0 {
        return;
    }
    let text = if count > 99 { "99+".to_string() } else { count.to_string() };
    let g = p.layout_no_wrap(text, FontId::proportional(11.5), Color32::WHITE);
    let size = Vec2::new((g.size().x + 9.0).max(18.0), 18.0);
    let r = Rect::from_center_size(at - Vec2::new(size.x / 2.0 - 9.0, 9.0), size);
    p.rect_filled(r.expand(2.0), 11.0, RAIL);
    p.rect_filled(r, 9.0, RED);
    p.galley(r.center() - g.size() / 2.0, g, Color32::WHITE);
}

fn list_row(ui: &mut egui::Ui, selected: bool, unread: bool, badge: u32, icon: Option<&str>, prefix: &str, name: &str) -> bool {
    let w = ui.available_width();
    let (rect, resp) = ui.allocate_exact_size(Vec2::new(w, if icon.is_some() { 42.0 } else { 32.0 }), Sense::click());
    let painter = ui.painter().clone();
    if selected {
        painter.rect_filled(rect, 4.0, ACTIVE);
    } else if resp.hovered() {
        painter.rect_filled(rect, 4.0, HOVER);
    }
    let mut x = rect.left() + 8.0;
    if let Some(url) = icon {
        let r = Rect::from_min_size(egui::pos2(x, rect.center().y - 16.0), Vec2::splat(32.0));
        egui::Image::new(url).rounding(16.0).paint_at(ui, r);
        x += 42.0;
    } else {
        painter.text(egui::pos2(x, rect.center().y), Align2::LEFT_CENTER, prefix, FontId::proportional(18.0), MUTED);
        x += 22.0;
    }
    let color = if selected || unread { BRIGHT } else { MUTED };
    let font = FontId::proportional(15.0);
    let galley = painter.layout(name.to_string(), font, color, rect.right() - x - 8.0);
    painter.galley(egui::pos2(x, rect.center().y - galley.size().y / 2.0), galley, color);
    mention_badge(&painter, egui::pos2(rect.right() - 14.0, rect.center().y + 9.0), badge);
    if unread && !selected {
        painter.circle_filled(egui::pos2(rect.left() - 2.0, rect.center().y), 4.0, BRIGHT);
    }
    resp.clicked()
}

// ---------- app ----------

#[derive(PartialEq)]
enum Phase {
    Login,
    Connecting,
    Main,
}

#[derive(PartialEq, Clone)]
enum View {
    Dms,
    Guild(String),
}

struct Selected {
    id: String,
    title: String,
    guild: Option<String>,
    voice: bool,
}

struct App {
    evt_tx: Sender<Evt>,
    evt_rx: Receiver<Evt>,
    rest: Option<Sender<Cmd>>,
    gw: Option<Sender<String>>,
    bot: bool,
    phase: Phase,
    token_input: String,
    remember: bool,
    me: String,
    me_avatar: String,
    guilds: Vec<Guild>,
    dms: Vec<Dm>,
    chan_guild: HashMap<String, String>,
    view: View,
    sel: Option<Selected>,
    cache: HashMap<String, Result<Vec<Msg>, String>>,
    unread: HashSet<String>,
    draft: String,
    status: String,
    qr_tex: Option<egui::TextureHandle>,
    qr_note: String,
    qr_cancel: Option<Arc<AtomicBool>>,
    me_id: String,
    vtx: VoiceTx,
    /// (server, channel, title) of the voice channel or DM call we're in
    in_voice: Option<(Option<String>, String, String)>,
    voice_phase: VoicePhase,
    voice_note: String,
    muted: bool,
    deaf: bool,
    voice_members: HashMap<String, Vec<VoiceMember>>,
    remote_speaking: Vec<String>,
    self_speaking: bool,
    known_users: HashMap<String, (String, String)>,
    asked_users: HashSet<String>,
    member_requests: HashMap<String, Vec<String>>,
    /// (stream key, streamer name) we're watching
    watching: Option<(String, String)>,
    stream_live: bool,
    stream_note: String,
    frame_slot: stream::FrameSlot,
    stream_tex: Option<egui::TextureHandle>,
    /// channel -> (user id, name, when they started typing)
    typing: HashMap<String, Vec<(String, String, Instant)>>,
    /// guild -> member list rows (Discord's sidebar list)
    member_lists: HashMap<String, Vec<ListRow>>,
    show_members: bool,
    /// unread @mentions per channel (red badges)
    mentions: HashMap<String, u32>,
    /// composer modes
    reply_to: Option<(String, String)>,
    editing: Option<String>,
    files: Vec<PathBuf>,
    emoji_open: bool,
    /// Quick Switcher (Ctrl+K)
    qs_open: bool,
    qs_query: String,
    /// right-click menu: (channel, message id, where)
    msg_menu: Option<(String, String, egui::Pos2)>,
    // ---- Voice & Video settings ----
    vcfg: voice::SharedCfg,
    meter: Arc<std::sync::atomic::AtomicU32>,
    meter_val: f32,
    settings_open: bool,
    devices: (Vec<String>, Vec<String>),
    recording_key: bool,
    mic_testing: bool,
    /// per-person volume popup: (user id, name, where)
    vol_popup: Option<(String, String, egui::Pos2)>,
}

fn settings_path() -> PathBuf {
    token_path().with_file_name("settings.json")
}

/// Things you can do to a message (right-click menu / reaction chips).
enum MsgAction {
    Reply(String, String),
    Edit(String, String),
    Delete(String),
    React(String, String, bool),
    Pin(String),
    Copy(String),
}

const QUICK_REACTIONS: [&str; 6] = ["👍", "❤️", "😂", "😮", "😢", "🔥"];
const EMOJI_GRID: [&str; 64] = [
    "😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "😊", "😇", "🙂", "🙃", "😉", "😌", "😍", "🥰",
    "😘", "😋", "😛", "😜", "🤪", "😝", "🤗", "🤭", "🤔", "🤨", "😐", "😑", "😶", "🙄", "😏", "😬",
    "😢", "😭", "😤", "😠", "😡", "🤯", "😳", "🥵", "🥶", "😱", "😨", "😴", "🤤", "😷", "🤒", "🤡",
    "👍", "👎", "👏", "🙌", "🙏", "💪", "👀", "❤️", "💔", "🔥", "✨", "🎉", "💯", "✅", "❌", "💀",
];

#[derive(Clone)]
enum ListRow {
    Group { name: String, count: u64 },
    Member { name: String, avatar: String, status: String, color: Option<Color32> },
}

fn qr_texture(ctx: &egui::Context, url: &str) -> Option<egui::TextureHandle> {
    let code = qrcode::QrCode::new(url.as_bytes()).ok()?;
    let w = code.width();
    let quiet = 4;
    let size = w + quiet * 2;
    let mut img = egui::ColorImage::new([size, size], Color32::WHITE);
    for (i, c) in code.to_colors().iter().enumerate() {
        if *c == qrcode::Color::Dark {
            let (x, y) = (i % w + quiet, i / w + quiet);
            img.pixels[y * size + x] = Color32::BLACK;
        }
    }
    Some(ctx.load_texture("qr", img, egui::TextureOptions::NEAREST))
}

fn token_path() -> PathBuf {
    let base = std::env::var("LOCALAPPDATA").unwrap_or_else(|_| ".".into());
    PathBuf::from(base).join("Rustcord").join("token.txt")
}

fn setup_style(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    let files = [("segoe", "segoeui.ttf", true), ("emoji", "seguiemj.ttf", false), ("symbol", "seguisym.ttf", false)];
    for (name, file, primary) in files {
        if let Ok(bytes) = fs::read(format!("C:\\Windows\\Fonts\\{file}")) {
            fonts.font_data.insert(name.into(), egui::FontData::from_owned(bytes));
            let fam = fonts.families.get_mut(&egui::FontFamily::Proportional).unwrap();
            if primary { fam.insert(0, name.into()) } else { fam.push(name.into()) }
        }
    }
    // Bold / italic families for Markdown (fall back to the normal font if missing).
    let base = fonts.families[&egui::FontFamily::Proportional].clone();
    for (family, file) in [("bold", "segoeuib.ttf"), ("italic", "segoeuii.ttf")] {
        let mut list = base.clone();
        if let Ok(bytes) = fs::read(format!("C:\\Windows\\Fonts\\{file}")) {
            fonts.font_data.insert(family.into(), egui::FontData::from_owned(bytes));
            list.insert(0, family.into());
        }
        fonts.families.insert(egui::FontFamily::Name(family.into()), list);
    }
    ctx.set_fonts(fonts);

    let mut v = egui::Visuals::dark();
    v.panel_fill = CHAT;
    v.window_fill = SIDE;
    v.extreme_bg_color = INPUT;
    v.override_text_color = Some(TEXT);
    v.selection.bg_fill = ACCENT;
    v.hyperlink_color = Color32::from_rgb(0, 168, 252);
    v.widgets.inactive.weak_bg_fill = INPUT;
    v.widgets.hovered.weak_bg_fill = ACTIVE;
    v.widgets.active.weak_bg_fill = ACCENT;
    ctx.set_visuals(v);
    ctx.style_mut(|s| {
        s.spacing.item_spacing = Vec2::new(8.0, 4.0);
        s.text_styles.insert(egui::TextStyle::Body, FontId::proportional(15.0));
        s.text_styles.insert(egui::TextStyle::Button, FontId::proportional(14.0));
    });
}

impl App {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        setup_style(&cc.egui_ctx);
        egui_extras::install_image_loaders(&cc.egui_ctx);
        let (evt_tx, evt_rx) = channel();
        let frame_slot: stream::FrameSlot = Arc::new(std::sync::Mutex::new(None));
        let streamer: stream::Streamer = Arc::new(std::sync::Mutex::new(None));
        stream::spawn(frame_slot.clone(), streamer.clone(), cc.egui_ctx.clone());
        let vcfg: voice::SharedCfg = Arc::new(std::sync::Mutex::new(voice::VoiceCfg::load(&settings_path())));
        let meter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let vtx = voice::spawn(evt_tx.clone(), cc.egui_ctx.clone(), streamer, vcfg.clone(), meter.clone());
        let mut app = Self {
            evt_tx,
            evt_rx,
            rest: None,
            gw: None,
            bot: false,
            phase: Phase::Login,
            token_input: String::new(),
            remember: true,
            me: String::new(),
            me_avatar: String::new(),
            guilds: Vec::new(),
            dms: Vec::new(),
            chan_guild: HashMap::new(),
            view: View::Dms,
            sel: None,
            cache: HashMap::new(),
            unread: HashSet::new(),
            draft: String::new(),
            status: String::new(),
            qr_tex: None,
            qr_note: String::new(),
            qr_cancel: None,
            me_id: String::new(),
            vtx,
            in_voice: None,
            voice_phase: VoicePhase::Off,
            voice_note: String::new(),
            muted: false,
            deaf: false,
            voice_members: HashMap::new(),
            remote_speaking: Vec::new(),
            self_speaking: false,
            known_users: HashMap::new(),
            asked_users: HashSet::new(),
            member_requests: HashMap::new(),
            watching: None,
            stream_live: false,
            stream_note: String::new(),
            frame_slot,
            stream_tex: None,
            typing: HashMap::new(),
            member_lists: HashMap::new(),
            show_members: true,
            mentions: HashMap::new(),
            reply_to: None,
            editing: None,
            files: Vec::new(),
            emoji_open: false,
            qs_open: false,
            qs_query: String::new(),
            msg_menu: None,
            vcfg,
            meter,
            meter_val: 0.0,
            settings_open: false,
            devices: (Vec::new(), Vec::new()),
            recording_key: false,
            mic_testing: false,
            vol_popup: None,
        };
        match fs::read_to_string(token_path()) {
            Ok(saved) if !saved.trim().is_empty() => app.connect(&cc.egui_ctx, saved.trim().to_string(), false),
            _ => app.start_qr(&cc.egui_ctx),
        }
        app
    }

    fn start_qr(&mut self, ctx: &egui::Context) {
        self.stop_qr();
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, ctx2, flag) = (self.evt_tx.clone(), ctx.clone(), cancel.clone());
        thread::spawn(move || qr_login::run(out, ctx2, flag));
        self.qr_cancel = Some(cancel);
        self.qr_tex = None;
        self.qr_note = "Getting a QR code from Discord...".into();
    }

    fn stop_qr(&mut self) {
        if let Some(c) = self.qr_cancel.take() {
            c.store(true, Ordering::Relaxed);
        }
    }

    fn connect(&mut self, ctx: &egui::Context, raw: String, save: bool) {
        self.stop_qr();
        let token = raw.trim().trim_matches('"').to_string();
        self.bot = token.starts_with("Bot ");
        if save {
            let p = token_path();
            if let Some(dir) = p.parent() {
                let _ = fs::create_dir_all(dir);
            }
            let _ = fs::write(&p, &token);
        }
        let (cmd_tx, cmd_rx) = channel();
        let (gw_tx, gw_rx) = channel();
        let (out1, out2) = (self.evt_tx.clone(), self.evt_tx.clone());
        let (ctx1, ctx2) = (ctx.clone(), ctx.clone());
        let gw_token = token.trim_start_matches("Bot ").to_string();
        let bot = self.bot;
        let vtx = self.vtx.clone();
        thread::spawn(move || rest_worker(token, cmd_rx, out1, ctx1));
        thread::spawn(move || gateway_loop(gw_token, bot, out2, gw_rx, ctx2, vtx));
        self.rest = Some(cmd_tx);
        self.gw = Some(gw_tx);
        self.phase = Phase::Connecting;
        self.status = "Connecting to Discord...".into();
    }

    fn index_guild(&mut self, g: &Guild) {
        for c in &g.channels {
            self.chan_guild.insert(c.id.clone(), g.id.clone());
        }
    }

    fn handle_events(&mut self, ctx: &egui::Context) {
        while let Ok(evt) = self.evt_rx.try_recv() {
            match evt {
                Evt::Voice(v) => match v {
                    voice::VoiceUi::Connecting => {
                        self.voice_phase = VoicePhase::Connecting;
                        self.voice_note.clear();
                    }
                    voice::VoiceUi::Connected => self.voice_phase = VoicePhase::Connected,
                    voice::VoiceUi::Ended(why) => {
                        self.voice_phase = VoicePhase::Off;
                        // Don't leave us showing as "in the channel" to others if joining failed.
                        if let Some((g, _, _)) = self.in_voice.take() {
                            self.send_voice_state(g.as_deref(), None);
                        }
                        self.remote_speaking.clear();
                        self.self_speaking = false;
                        self.voice_note = why;
                    }
                    voice::VoiceUi::StreamConnecting => {
                        self.stream_live = false;
                        self.stream_note.clear();
                    }
                    voice::VoiceUi::StreamLive => self.stream_live = true,
                    voice::VoiceUi::StreamEnded(why) => {
                        if let Some((key, _)) = self.watching.take() {
                            if let Some(gw) = &self.gw {
                                let _ = gw.send(json!({ "op": 21, "d": { "stream_key": key } }).to_string());
                            }
                        }
                        self.stream_live = false;
                        self.stream_tex = None;
                        self.stream_note = why;
                    }
                    voice::VoiceUi::RemoteSpeaking(list) => self.remote_speaking = list,
                    voice::VoiceUi::SelfSpeaking(on) => self.self_speaking = on,
                },
                Evt::VoiceMember { channel, user, mut name, mut avatar, streaming } => {
                    if let Some((n, a)) = self.known_users.get(&user) {
                        name = n.clone();
                        avatar = a.clone();
                    }
                    for list in self.voice_members.values_mut() {
                        list.retain(|m| m.user != user);
                    }
                    if let Some(ch) = channel {
                        self.voice_members.entry(ch).or_default().push(VoiceMember { user, name, avatar, streaming });
                    }
                }
                Evt::NeedUser(guild, id) => {
                    if !self.known_users.contains_key(&id) && self.asked_users.insert(id.clone()) {
                        match guild {
                            // Batched into one gateway request per server (see flush_member_requests).
                            Some(g) => self.member_requests.entry(g).or_default().push(id),
                            None => {
                                if let Some(rest) = &self.rest {
                                    let _ = rest.send(Cmd::User(id));
                                }
                            }
                        }
                    }
                }
                Evt::Typing { channel, user, name } => {
                    let name = name
                        .or_else(|| self.known_users.get(&user).map(|k| k.0.clone()))
                        .or_else(|| self.dms.iter().find(|d| d.id == channel).map(|d| d.name.clone()))
                        .unwrap_or_else(|| "Someone".into());
                    let list = self.typing.entry(channel).or_default();
                    list.retain(|t| t.0 != user);
                    list.push((user, name, Instant::now()));
                }
                Evt::MemberList(guild, ops) => {
                    let roles = self.guilds.iter().find(|g| g.id == guild).map(|g| g.roles.clone()).unwrap_or_default();
                    let rows = self.member_lists.entry(guild).or_default();
                    apply_member_ops(rows, &ops, &roles);
                }
                Evt::UserInfo { user, name, avatar } => {
                    for m in self.voice_members.values_mut().flatten().filter(|m| m.user == user) {
                        m.name = name.clone();
                        m.avatar = avatar.clone();
                    }
                    self.known_users.insert(user, (name, avatar));
                }
                Evt::Ready { me, me_id, me_avatar, guilds, dms } => {
                    self.status = "Connected".into();
                    self.me_id = me_id;
                    self.me = me;
                    self.me_avatar = me_avatar;
                    self.chan_guild.clear();
                    for g in &guilds {
                        self.index_guild(g);
                    }
                    if !(self.bot && guilds.is_empty()) {
                        self.guilds = guilds;
                    }
                    self.dms = dms;
                    self.phase = Phase::Main;
                }
                Evt::GuildAdd(g) => {
                    self.index_guild(&g);
                    match self.guilds.iter_mut().find(|x| x.id == g.id) {
                        Some(slot) => *slot = g,
                        None => self.guilds.push(g),
                    }
                }
                Evt::Message(ch, m) => {
                    let author = m.author.clone();
                    if let Some(t) = self.typing.get_mut(&ch) {
                        t.retain(|x| x.1 != author);
                    }
                    let avatar = m.avatar.clone();
                    let viewing = self.sel.as_ref().map(|s| s.id.as_str()) == Some(ch.as_str());
                    let focused = ctx.input(|i| i.viewport().focused.unwrap_or(true));
                    if m.pings_me && !(viewing && focused) {
                        if !viewing {
                            *self.mentions.entry(ch.clone()).or_default() += 1;
                        }
                        let place = match self.chan_guild.get(&ch) {
                            Some(g) => {
                                let gname = self.guilds.iter().find(|x| &x.id == g).map(|x| x.name.as_str()).unwrap_or("");
                                let cname = self.guilds.iter().flat_map(|x| x.channels.iter()).find(|c| c.id == ch).map(|c| c.name.as_str()).unwrap_or("");
                                format!(" (#{cname}, {gname})")
                            }
                            None => String::new(),
                        };
                        let body: String = if m.content.is_empty() { "Sent an attachment".into() } else { m.content.chars().take(180).collect() };
                        let title = format!("{author}{place}");
                        std::thread::spawn(move || {
                            let _ = notify_rust::Notification::new().appname("Rustcord").summary(&title).body(&body).show();
                        });
                    }
                    if let Some(Ok(list)) = self.cache.get_mut(&ch) {
                        if !list.iter().any(|x| x.id == m.id) {
                            list.push(m);
                            if list.len() > MAX_CACHED {
                                list.drain(..list.len() - MAX_CACHED);
                            }
                        }
                    }
                    if !self.chan_guild.contains_key(&ch) {
                        match self.dms.iter().position(|d| d.id == ch) {
                            Some(pos) => {
                                let d = self.dms.remove(pos);
                                self.dms.insert(0, d);
                            }
                            None => self.dms.insert(0, Dm { id: ch.clone(), name: author, avatar: Some(avatar), last: 0 }),
                        }
                    }
                    if self.sel.as_ref().map(|s| s.id.as_str()) != Some(ch.as_str()) {
                        self.unread.insert(ch);
                    }
                }
                Evt::Edited(ch, id, raw, shown) => {
                    if let Some(Ok(list)) = self.cache.get_mut(&ch) {
                        if let Some(m) = list.iter_mut().find(|m| m.id == id) {
                            m.raw = raw;
                            m.content = shown + " (edited)";
                        }
                    }
                }
                Evt::Reacted { ch, msg, label, emoji_id, mine, add } => {
                    if let Some(m) = self.cache.get_mut(&ch).and_then(|r| r.as_mut().ok()).and_then(|l| l.iter_mut().find(|m| m.id == msg)) {
                        let pos = m.reactions.iter().position(|r| match (&r.emoji_id, &emoji_id) {
                            (Some(a), Some(b)) => a == b,
                            _ => r.label == label,
                        });
                        match (pos, add) {
                            (Some(i), true) => {
                                m.reactions[i].count += 1;
                                m.reactions[i].me |= mine;
                            }
                            (None, true) => m.reactions.push(Reaction {
                                image: emoji_id.as_ref().map(|id| format!("{CDN}/emojis/{id}.png?size=32")),
                                label,
                                emoji_id,
                                count: 1,
                                me: mine,
                            }),
                            (Some(i), false) => {
                                let r = &mut m.reactions[i];
                                r.count = r.count.saturating_sub(1);
                                if mine {
                                    r.me = false;
                                }
                                if r.count == 0 {
                                    m.reactions.remove(i);
                                }
                            }
                            (None, false) => {}
                        }
                    }
                }
                Evt::Deleted(ch, id) => {
                    if let Some(Ok(list)) = self.cache.get_mut(&ch) {
                        list.retain(|m| m.id != id);
                    }
                }
                Evt::History(ch, r) => {
                    self.cache.insert(ch, r);
                }
                Evt::Status(s) => self.status = s,
                Evt::AuthFailed(msg) => {
                    let _ = fs::remove_file(token_path());
                    self.rest = None;
                    self.gw = None;
                    self.phase = Phase::Login;
                    self.status = msg;
                    self.start_qr(ctx);
                }
                Evt::Qr(url) => {
                    self.qr_tex = qr_texture(ctx, &url);
                    self.qr_note = "Scan this with the Discord app on your phone.".into();
                }
                Evt::QrNote(note) => self.qr_note = note,
                Evt::LoginToken(token) => {
                    self.qr_tex = None;
                    let save = self.remember;
                    self.connect(ctx, token, save);
                }
            }
        }
    }

    /// Drop decoded pictures from the channel we're leaving, to keep memory low.
    fn forget_images(&self, ctx: &egui::Context, channel: &str) {
        if let Some(Ok(list)) = self.cache.get(channel) {
            for m in list {
                for img in &m.images {
                    ctx.forget_image(&img.url);
                }
            }
        }
    }

    fn open(&mut self, ctx: &egui::Context, sel: Selected) {
        if let Some(prev) = &self.sel {
            if prev.id != sel.id {
                self.forget_images(ctx, &prev.id);
            }
        }
        if sel.voice {
            self.sel = Some(sel);
            return;
        }
        let id = sel.id.clone();
        self.unread.remove(&id);
        self.mentions.remove(&id);
        self.reply_to = None;
        self.editing = None;
        if matches!(self.cache.get(&id), Some(Err(_))) {
            self.cache.remove(&id);
        }
        if !self.cache.contains_key(&id) {
            if let Some(tx) = &self.rest {
                let _ = tx.send(Cmd::History(id.clone()));
            }
        }
        // Ask Discord to stream this guild channel's live messages to us.
        if let (Some(g), false, Some(gw)) = (&sel.guild, self.bot, &self.gw) {
            let payload = json!({ "op": 14, "d": {
                "guild_id": g, "typing": true, "threads": false, "activities": false,
                "channels": { id.clone(): [[0, 99]] }
            }});
            let _ = gw.send(payload.to_string());
        }
        self.sel = Some(sel);
    }

    /// Ask Discord for names/pictures of people we only know by id (one request per server).
    fn flush_member_requests(&mut self) {
        let Some(gw) = &self.gw else { return };
        for (guild, ids) in self.member_requests.drain() {
            for chunk in ids.chunks(100) {
                let p = json!({ "op": 8, "d": { "guild_id": guild, "user_ids": chunk, "presences": false } });
                let _ = gw.send(p.to_string());
            }
        }
    }

    /// Tell Discord our voice state (join / move / leave / mute / deafen).
    fn send_voice_state(&self, guild: Option<&str>, channel: Option<&str>) {
        tracing::info!("ui: sending voice state guild={guild:?} channel={channel:?} gateway_ready={}", self.gw.is_some());
        if let Some(gw) = &self.gw {
            let p = json!({ "op": 4, "d": {
                "guild_id": guild, "channel_id": channel,
                "self_mute": self.muted, "self_deaf": self.deaf, "self_video": false
            }});
            let _ = gw.send(p.to_string());
        }
    }

    fn apply_voice(&mut self, action: Option<VAction>) {
        let Some(action) = action else { return };
        match action {
            VAction::Join(guild, channel, title) => {
                if self.in_voice.as_ref().map_or(false, |v| v.1 == channel) {
                    return;
                }
                let _ = self.vtx.send(voice::VoiceMsg::Join { guild: guild.clone(), channel: channel.clone() });
                self.send_voice_state(guild.as_deref(), Some(&channel));
                if guild.is_none() {
                    if let Some(rest) = &self.rest {
                        let _ = rest.send(Cmd::Ring(channel.clone()));
                    }
                }
                self.in_voice = Some((guild, channel, title));
                self.voice_phase = VoicePhase::Connecting;
                self.voice_note.clear();
            }
            VAction::Leave => {
                if let Some((g, _, _)) = self.in_voice.take() {
                    self.send_voice_state(g.as_deref(), None);
                }
                let _ = self.vtx.send(voice::VoiceMsg::Leave);
                self.voice_phase = VoicePhase::Off;
            }
            VAction::ToggleMute => {
                self.muted = !self.muted;
                if !self.muted {
                    self.deaf = false;
                    let _ = self.vtx.send(voice::VoiceMsg::Deafen(false));
                }
                let _ = self.vtx.send(voice::VoiceMsg::Mute(self.muted));
                if let Some((g, c, _)) = &self.in_voice {
                    self.send_voice_state(g.as_deref(), Some(c));
                }
            }
            VAction::Watch(key, user, name) => {
                tracing::info!("ui: watch clicked {key}");
                if let Some(gw) = &self.gw {
                    let _ = gw.send(json!({ "op": 20, "d": { "stream_key": key } }).to_string());
                }
                let _ = self.vtx.send(voice::VoiceMsg::WatchStream { key: key.clone(), streamer: user });
                self.watching = Some((key, name));
                self.stream_tex = None;
                self.stream_live = false;
                self.stream_note.clear();
            }
            VAction::StopWatch => {
                if let Some((key, _)) = self.watching.take() {
                    if let Some(gw) = &self.gw {
                        let _ = gw.send(json!({ "op": 21, "d": { "stream_key": key } }).to_string());
                    }
                }
                let _ = self.vtx.send(voice::VoiceMsg::StopStream);
                self.stream_tex = None;
            }
            VAction::ToggleDeaf => {
                self.deaf = !self.deaf;
                self.muted = self.deaf;
                let _ = self.vtx.send(voice::VoiceMsg::Deafen(self.deaf));
                let _ = self.vtx.send(voice::VoiceMsg::Mute(self.muted));
                if let Some((g, c, _)) = &self.in_voice {
                    self.send_voice_state(g.as_deref(), Some(c));
                }
            }
        }
    }

    fn speaking(&self) -> HashSet<String> {
        voice::speaking_set(&self.remote_speaking, Some(&self.me_id), self.self_speaking)
    }

    fn login_screen(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().frame(egui::Frame::none().fill(ACCENT)).show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space((ui.available_height() - 560.0).max(20.0) / 2.0);
                egui::Frame::none().fill(CHAT).rounding(8.0).inner_margin(32.0).show(ui, |ui| {
                    ui.set_width(420.0);
                    ui.vertical_centered(|ui| {
                        ui.label(RichText::new("Welcome back!").size(24.0).strong().color(BRIGHT));
                        ui.label(RichText::new("Rustcord · native Rust, no web engine").color(MUTED));
                        ui.add_space(20.0);
                        if self.phase == Phase::Connecting {
                            ui.add_space(60.0);
                            ui.spinner();
                            ui.label(&self.status);
                            ui.add_space(60.0);
                            return;
                        }
                        egui::Frame::none().fill(Color32::WHITE).rounding(8.0).inner_margin(8.0).show(ui, |ui| {
                            match &self.qr_tex {
                                Some(tex) => {
                                    ui.add(egui::Image::new(egui::load::SizedTexture::new(tex.id(), Vec2::splat(200.0))));
                                }
                                None => {
                                    ui.allocate_ui(Vec2::splat(200.0), |ui| {
                                        ui.centered_and_justified(|ui| ui.spinner());
                                    });
                                }
                            }
                        });
                        ui.add_space(12.0);
                        ui.label(RichText::new("Log in with QR Code").size(18.0).strong().color(BRIGHT));
                        ui.label(RichText::new(&self.qr_note).color(MUTED));
                        ui.label(RichText::new("Discord app on your phone → Settings → Scan QR Code").color(MUTED));
                        ui.add_space(8.0);
                        ui.checkbox(&mut self.remember, "Stay signed in on this PC");
                        if !self.status.is_empty() {
                            ui.colored_label(RED, &self.status);
                        }
                        ui.add_space(12.0);
                        ui.label(RichText::new("Unofficial clients are against Discord's Terms of Service. Use at your own risk.").small().color(MUTED));
                        ui.add_space(6.0);
                        egui::CollapsingHeader::new(RichText::new("Advanced: use a bot token").small()).show(ui, |ui| {
                            let field = ui.add(
                                egui::TextEdit::singleline(&mut self.token_input).password(true).hint_text("Bot ...").desired_width(340.0),
                            );
                            let enter = field.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                            if (ui.button("Connect bot").clicked() || enter) && self.token_input.trim().starts_with("Bot ") {
                                let token = std::mem::take(&mut self.token_input);
                                self.connect(ctx, token, self.remember);
                            }
                        });
                    });
                });
            });
        });
    }

    fn main_screen(&mut self, ctx: &egui::Context) {
        let mut vact: Option<VAction> = None;
        let speaking = self.speaking();

        // ---- server rail ----
        let mut pick_view: Option<View> = None;
        egui::SidePanel::left("rail")
            .resizable(false)
            .exact_width(72.0)
            .frame(egui::Frame::none().fill(RAIL).inner_margin(egui::Margin { left: 12.0, right: 12.0, top: 12.0, bottom: 12.0 }))
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().scroll_bar_visibility(egui::scroll_area::ScrollBarVisibility::AlwaysHidden).show(ui, |ui| {
                    ui.spacing_mut().item_spacing.y = 8.0;
                    let dm_unread = self.dms.iter().any(|d| self.unread.contains(&d.id));
                    let dm_badge: u32 = self.dms.iter().filter_map(|d| self.mentions.get(&d.id)).sum();
                    if rail_button(ui, self.view == View::Dms, dm_unread, dm_badge, None, "DM", "Direct Messages") {
                        pick_view = Some(View::Dms);
                    }
                    let (r, _) = ui.allocate_exact_size(Vec2::new(48.0, 2.0), Sense::hover());
                    ui.painter().rect_filled(r.shrink2(Vec2::new(12.0, 0.0)), 1.0, ACTIVE);
                    for g in &self.guilds {
                        let unread = g.channels.iter().any(|c| self.unread.contains(&c.id));
                        let on = self.view == View::Guild(g.id.clone());
                        let badge: u32 = g.channels.iter().filter_map(|c| self.mentions.get(&c.id)).sum();
                        if rail_button(ui, on, unread, badge, g.icon.as_deref(), &initials(&g.name), &g.name) {
                            pick_view = Some(View::Guild(g.id.clone()));
                        }
                    }
                });
            });
        if let Some(v) = pick_view {
            self.view = v;
        }

        // ---- channel / DM list, voice bar, user bar ----
        let mut pick: Option<Selected> = None;
        let mut sign_out = false;
        let mut open_settings = false;
        egui::SidePanel::left("channels")
            .resizable(false)
            .exact_width(240.0)
            .frame(egui::Frame::none().fill(SIDE))
            .show(ctx, |ui| {
                egui::TopBottomPanel::bottom("me")
                    .frame(egui::Frame::none().fill(Color32::from_rgb(35, 36, 40)).inner_margin(8.0))
                    .show_separator_line(false)
                    .show_inside(ui, |ui| {
                        ui.horizontal(|ui| {
                            if !self.me_avatar.is_empty() {
                                avatar_ring(ui, &self.me_avatar, 32.0, speaking.contains(&self.me_id));
                            }
                            ui.vertical(|ui| {
                                ui.label(RichText::new(&self.me).strong().color(BRIGHT));
                                let online = self.status == "Connected";
                                ui.label(RichText::new(if online { "Online" } else { self.status.as_str() }).small().color(if online { GREEN } else { MUTED }));
                            });
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if ui.small_button("⎋").on_hover_text("Sign out").clicked() {
                                    sign_out = true;
                                }
                                if ui.small_button("⚙").on_hover_text("User Settings (Ctrl+,)").clicked() {
                                    open_settings = true;
                                }
                                let deaf = RichText::new("🎧").color(if self.deaf { RED } else { TEXT });
                                if ui.small_button(deaf).on_hover_text(if self.deaf { "Undeafen" } else { "Deafen" }).clicked() {
                                    vact = Some(VAction::ToggleDeaf);
                                }
                                let mic = RichText::new("🎙").color(if self.muted { RED } else { TEXT });
                                if ui.small_button(mic).on_hover_text(if self.muted { "Unmute" } else { "Mute" }).clicked() {
                                    vact = Some(VAction::ToggleMute);
                                }
                            });
                        });
                    });

                if self.in_voice.is_some() || !self.voice_note.is_empty() {
                    egui::TopBottomPanel::bottom("voicebar")
                        .frame(egui::Frame::none().fill(Color32::from_rgb(35, 36, 40)).inner_margin(8.0))
                        .show_separator_line(false)
                        .show_inside(ui, |ui| match &self.in_voice {
                            Some((_, _, title)) => {
                                ui.horizontal(|ui| {
                                    ui.vertical(|ui| {
                                        let (txt, col) = match self.voice_phase {
                                            VoicePhase::Connected => ("Voice Connected", GREEN),
                                            _ => ("Connecting...", Color32::from_rgb(240, 178, 50)),
                                        };
                                        ui.label(RichText::new(txt).strong().color(col));
                                        ui.label(RichText::new(title).small().color(MUTED));
                                    });
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        let leave = egui::Button::new(RichText::new("📞").color(Color32::WHITE)).fill(RED);
                                        if ui.add(leave).on_hover_text("Disconnect").clicked() {
                                            vact = Some(VAction::Leave);
                                        }
                                    });
                                });
                            }
                            None => {
                                ui.label(RichText::new(&self.voice_note).small().color(RED));
                            }
                        });
                }

                let title = match &self.view {
                    View::Dms => "Direct Messages".to_string(),
                    View::Guild(gid) => self.guilds.iter().find(|g| &g.id == gid).map(|g| g.name.clone()).unwrap_or_default(),
                };
                egui::Frame::none().inner_margin(egui::Margin::symmetric(16.0, 14.0)).show(ui, |ui| {
                    ui.label(RichText::new(title).strong().size(16.0).color(BRIGHT));
                });
                ui.painter().hline(ui.max_rect().x_range(), ui.cursor().top(), Stroke::new(1.0_f32, RAIL));

                egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                    egui::Frame::none().inner_margin(egui::Margin::symmetric(8.0, 8.0)).show(ui, |ui| {
                        ui.spacing_mut().item_spacing.y = 2.0;
                        let sel_id = self.sel.as_ref().map(|s| s.id.clone());
                        match &self.view {
                            View::Dms => {
                                for d in &self.dms {
                                    let on = sel_id.as_deref() == Some(d.id.as_str());
                                    let fallback = format!("{CDN}/embed/avatars/0.png");
                                    let icon = d.avatar.as_deref().unwrap_or(&fallback);
                                    if list_row(ui, on, self.unread.contains(&d.id), self.mentions.get(&d.id).copied().unwrap_or(0), Some(icon), "", &d.name) {
                                        pick = Some(Selected { id: d.id.clone(), title: d.name.clone(), guild: None, voice: false });
                                    }
                                }
                            }
                            View::Guild(gid) => {
                                if let Some(g) = self.guilds.iter().find(|g| &g.id == gid) {
                                    for c in &g.channels {
                                        if c.kind == 4 {
                                            ui.add_space(12.0);
                                            ui.label(RichText::new(format!("⌄ {}", c.name.to_uppercase())).size(12.0).strong().color(MUTED));
                                            continue;
                                        }
                                        let on = sel_id.as_deref() == Some(c.id.as_str());
                                        let prefix = if c.is_voice() { "🔊" } else { "#" };
                                        if list_row(ui, on, self.unread.contains(&c.id), self.mentions.get(&c.id).copied().unwrap_or(0), None, prefix, &c.name) {
                                            pick = Some(Selected { id: c.id.clone(), title: c.name.clone(), guild: Some(g.id.clone()), voice: c.is_voice() });
                                        }
                                        if c.is_voice() {
                                            for m in self.voice_members.get(&c.id).map(|v| v.as_slice()).unwrap_or(&[]) {
                                                let (name, av) = self.member_face(m);
                                                ui.horizontal(|ui| {
                                                    ui.add_space(30.0);
                                                    avatar_ring(ui, av, 22.0, speaking.contains(&m.user));
                                                    ui.label(RichText::new(name).size(13.0).color(MUTED));
                                                    if m.streaming {
                                                        ui.label(RichText::new(" LIVE ").size(10.0).strong().color(Color32::WHITE).background_color(RED));
                                                    }
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                });
            });
        if open_settings {
            self.open_settings();
        }
        if sign_out {
            let _ = fs::remove_file(token_path());
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        if let Some(sel) = pick {
            self.open(ctx, sel);
        }

        // ---- channel header ----
        let in_this_voice = match (&self.sel, &self.in_voice) {
            (Some(s), Some(v)) => s.id == v.1,
            _ => false,
        };
        let mut toggle_members = false;
        egui::TopBottomPanel::top("head")
            .frame(egui::Frame::none().fill(CHAT).inner_margin(egui::Margin::symmetric(16.0, 12.0)).stroke(Stroke::new(1.0_f32, RAIL)))
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    match &self.sel {
                        Some(sel) => {
                            let prefix = if sel.voice { "🔊" } else if sel.guild.is_some() { "#" } else { "@" };
                            ui.label(RichText::new(prefix).size(20.0).color(MUTED));
                            ui.label(RichText::new(&sel.title).strong().size(16.0).color(BRIGHT));
                        }
                        None => {
                            ui.label(RichText::new("Rustcord").strong().size(16.0).color(BRIGHT));
                        }
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if let Some(sel) = &self.sel {
                            if sel.guild.is_some() && !sel.voice {
                                let c = if self.show_members { BRIGHT } else { MUTED };
                                let b = egui::Button::new(RichText::new("👥").size(18.0).color(c)).frame(false);
                                if ui.add(b).on_hover_text(if self.show_members { "Hide Member List" } else { "Show Member List" }).clicked() {
                                    toggle_members = true;
                                }
                            }
                            if sel.guild.is_none() {
                                if in_this_voice {
                                    let leave = egui::Button::new(RichText::new("Leave call").color(Color32::WHITE)).fill(RED);
                                    if ui.add(leave).clicked() {
                                        vact = Some(VAction::Leave);
                                    }
                                } else if ui.button("📞 Call").on_hover_text("Start a voice call").clicked() {
                                    vact = Some(VAction::Join(None, sel.id.clone(), sel.title.clone()));
                                }
                            }
                        }
                    });
                });
            });

        if toggle_members {
            self.show_members = !self.show_members;
        }

        // ---- member list (right side, server text channels) ----
        let member_guild = self.sel.as_ref().filter(|s| !s.voice && self.show_members).and_then(|s| s.guild.clone());
        if let Some(rows) = member_guild.as_ref().and_then(|g| self.member_lists.get(g)) {
            egui::SidePanel::right("members")
                .resizable(false)
                .exact_width(240.0)
                .frame(egui::Frame::none().fill(SIDE).inner_margin(egui::Margin { left: 8.0, right: 8.0, top: 8.0, bottom: 8.0 }))
                .show(ctx, |ui| {
                    egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                        ui.spacing_mut().item_spacing.y = 2.0;
                        for row in rows {
                            match row {
                                ListRow::Group { name, count } => {
                                    ui.add_space(14.0);
                                    ui.label(RichText::new(format!("  {} — {count}", name.to_uppercase())).size(12.0).strong().color(MUTED));
                                    ui.add_space(2.0);
                                }
                                ListRow::Member { name, avatar, status, color } => member_list_row(ui, name, avatar, status, *color),
                            }
                        }
                    });
                });
        }

        // ---- voice channel page ----
        if let Some(sel) = self.sel.as_ref().filter(|s| s.voice) {
            let (guild, id, title) = (sel.guild.clone(), sel.id.clone(), sel.title.clone());
            egui::CentralPanel::default().show(ctx, |ui| {
                if in_this_voice {
                    ui.add_space(16.0);
                    self.stream_view(ui, &mut vact);
                    if let (Some((user, name)), Some(g)) = (self.voice_tiles(ui, &id, &speaking), guild.as_ref()) {
                        vact = Some(VAction::Watch(format!("guild:{g}:{id}:{user}"), user, name));
                    }
                    ui.add_space(24.0);
                    ui.vertical_centered(|ui| {
                        ui.horizontal(|ui| {
                            ui.add_space((ui.available_width() - 360.0).max(0.0) / 2.0);
                            let mic = if self.muted { "🎙 Unmute" } else { "🎙 Mute" };
                            if ui.add(egui::Button::new(mic).min_size(Vec2::new(110.0, 40.0)).rounding(20.0)).clicked() {
                                vact = Some(VAction::ToggleMute);
                            }
                            let deaf = if self.deaf { "🎧 Undeafen" } else { "🎧 Deafen" };
                            if ui.add(egui::Button::new(deaf).min_size(Vec2::new(110.0, 40.0)).rounding(20.0)).clicked() {
                                vact = Some(VAction::ToggleDeaf);
                            }
                            let leave = egui::Button::new(RichText::new("📞 Leave").color(Color32::WHITE)).fill(RED).min_size(Vec2::new(110.0, 40.0)).rounding(20.0);
                            if ui.add(leave).clicked() {
                                vact = Some(VAction::Leave);
                            }
                        });
                    });
                } else {
                    ui.vertical_centered(|ui| {
                        ui.add_space(ui.available_height() / 4.0);
                        ui.label(RichText::new("🔊").size(48.0));
                        ui.label(RichText::new(&title).size(22.0).strong().color(BRIGHT));
                        ui.add_space(6.0);
                        let n = self.voice_members.get(&id).map_or(0, |v| v.len());
                        let who = if n == 0 { "No one is here yet.".to_string() } else { format!("{n} in voice") };
                        ui.label(RichText::new(who).color(MUTED));
                        ui.add_space(14.0);
                        let join = egui::Button::new(RichText::new("Join Voice").strong().color(Color32::WHITE)).fill(GREEN).min_size(Vec2::new(160.0, 38.0)).rounding(4.0);
                        if ui.add(join).clicked() {
                            vact = Some(VAction::Join(guild.clone(), id.clone(), title.clone()));
                        }
                        ui.label(RichText::new("Screen share isn't available in Rustcord yet.").small().color(MUTED));
                    });
                    ui.add_space(20.0);
                    self.voice_tiles(ui, &id, &speaking);
                }
            });
            self.apply_voice(vact);
            return;
        }

        // ---- DM call strip ----
        if in_this_voice {
            let id = self.sel.as_ref().map(|s| s.id.clone()).unwrap_or_default();
            egui::TopBottomPanel::top("callstrip")
                .frame(egui::Frame::none().fill(Color32::from_rgb(17, 18, 20)).inner_margin(12.0))
                .show(ctx, |ui| {
                    self.stream_view(ui, &mut vact);
                    if let Some((user, name)) = self.voice_tiles(ui, &id, &speaking) {
                        vact = Some(VAction::Watch(format!("call:{id}:{user}"), user, name));
                    }
                    ui.horizontal(|ui| {
                        let mic = if self.muted { "🎙 Unmute" } else { "🎙 Mute" };
                        if ui.button(mic).clicked() {
                            vact = Some(VAction::ToggleMute);
                        }
                        let deaf = if self.deaf { "🎧 Undeafen" } else { "🎧 Deafen" };
                        if ui.button(deaf).clicked() {
                            vact = Some(VAction::ToggleDeaf);
                        }
                    });
                });
        }

        // ---- composer ----
        if let Some(sel) = &self.sel {
            let ch = sel.id.clone();
            let now = Instant::now();
            let typers: Vec<&str> = self
                .typing
                .get(&ch)
                .map(|v| v.iter().filter(|t| now.duration_since(t.2) < Duration::from_secs(9)).map(|t| t.1.as_str()).collect())
                .unwrap_or_default();
            let typing_text = match typers.as_slice() {
                [] => String::new(),
                [a] => format!("{a} is typing..."),
                [a, b] => format!("{a} and {b} are typing..."),
                _ => "Several people are typing...".into(),
            };
            if !typing_text.is_empty() {
                ctx.request_repaint_after(Duration::from_secs(1));
            }
            let hint = if sel.guild.is_some() { format!("Message #{}", sel.title) } else { format!("Message @{}", sel.title) };
            egui::TopBottomPanel::bottom("composer")
                .frame(egui::Frame::none().fill(CHAT).inner_margin(egui::Margin { left: 16.0, right: 16.0, top: 4.0, bottom: 4.0 }))
                .show(ctx, |ui| {
                    // Reply / edit banner.
                    let banner = match (&self.editing, &self.reply_to) {
                        (Some(_), _) => Some("Editing message  ·  Esc to cancel".to_string()),
                        (None, Some((_, who))) => Some(format!("Replying to {who}")),
                        _ => None,
                    };
                    if let Some(text) = banner {
                        egui::Frame::none().fill(SIDE).rounding(egui::Rounding { nw: 8.0, ne: 8.0, sw: 0.0, se: 0.0 }).inner_margin(egui::Margin::symmetric(14.0, 6.0)).show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(RichText::new(text).size(13.0).color(MUTED));
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.small_button("✕").clicked() {
                                        if self.editing.take().is_some() {
                                            self.draft.clear();
                                        }
                                        self.reply_to = None;
                                    }
                                });
                            });
                        });
                    }
                    // Files waiting to be sent.
                    if !self.files.is_empty() {
                        let mut remove = None;
                        ui.horizontal_wrapped(|ui| {
                            for (i, f) in self.files.iter().enumerate() {
                                let name = f.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
                                egui::Frame::none().fill(SIDE).rounding(6.0).inner_margin(6.0).show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.label(RichText::new(format!("📎 {name}")).size(13.0));
                                        if ui.small_button("✕").clicked() {
                                            remove = Some(i);
                                        }
                                    });
                                });
                            }
                        });
                        if let Some(i) = remove {
                            self.files.remove(i);
                        }
                    }
                    // Emoji picker.
                    if self.emoji_open {
                        egui::Frame::none().fill(SIDE).rounding(8.0).inner_margin(8.0).show(ui, |ui| {
                            egui::Grid::new("emoji_grid").spacing(Vec2::splat(2.0)).show(ui, |ui| {
                                for (i, e) in EMOJI_GRID.iter().enumerate() {
                                    if ui.add(egui::Button::new(RichText::new(*e).size(20.0)).frame(false)).clicked() {
                                        self.draft.push_str(e);
                                    }
                                    if i % 16 == 15 {
                                        ui.end_row();
                                    }
                                }
                            });
                        });
                    }
                    egui::Frame::none().fill(INPUT).rounding(8.0).inner_margin(egui::Margin::symmetric(10.0, 8.0)).show(ui, |ui| {
                        ui.horizontal(|ui| {
                            let plus = egui::Button::new(RichText::new("➕").size(16.0).color(MUTED)).frame(false);
                            if ui.add(plus).on_hover_text("Upload a file").clicked() {
                                if let Some(picked) = rfd::FileDialog::new().set_title("Upload to Discord").pick_files() {
                                    self.files.extend(picked);
                                }
                            }
                            let w = (ui.available_width() - 34.0).max(50.0);
                            let field = ui.add(
                                egui::TextEdit::singleline(&mut self.draft)
                                    .id(egui::Id::new("composer_field"))
                                    .hint_text(hint)
                                    .frame(false)
                                    .desired_width(w)
                                    .font(FontId::proportional(15.0)),
                            );
                            let smile = egui::Button::new(RichText::new("😀").size(18.0)).frame(false);
                            if ui.add(smile).on_hover_text("Emoji (Ctrl+E)").clicked() {
                                self.emoji_open = !self.emoji_open;
                            }
                            if field.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                                if self.editing.take().is_some() {
                                    self.draft.clear();
                                }
                                self.reply_to = None;
                            }
                            let enter = field.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                            if enter && (!self.draft.trim().is_empty() || !self.files.is_empty()) {
                                if let Some(tx) = &self.rest {
                                    let text = std::mem::take(&mut self.draft);
                                    match self.editing.take() {
                                        Some(id) => {
                                            let _ = tx.send(Cmd::Edit(ch.clone(), id, text));
                                        }
                                        None => {
                                            let reply_to = self.reply_to.take().map(|r| r.0);
                                            let files = std::mem::take(&mut self.files);
                                            let _ = tx.send(Cmd::Send { ch: ch.clone(), text, reply_to, files });
                                        }
                                    }
                                }
                                self.emoji_open = false;
                                field.request_focus();
                            }
                        });
                    });
                    // "Someone is typing..." line under the box, like Discord.
                    ui.add_sized([ui.available_width(), 18.0], egui::Label::new(RichText::new(&typing_text).size(12.5).strong().color(TEXT)));
                });
        }

        // ---- messages ----
        let mut action: Option<MsgAction> = None;
        let mut open_menu: Option<(String, String, egui::Pos2)> = None;
        egui::CentralPanel::default().frame(egui::Frame::none().fill(CHAT)).show(ctx, |ui| {
            let Some(sel) = &self.sel else {
                ui.centered_and_justified(|ui| ui.label(RichText::new("Pick a server and a channel, or a friend.").color(MUTED)));
                return;
            };
            match self.cache.get(&sel.id) {
                None => {
                    ui.centered_and_justified(|ui| ui.spinner());
                }
                Some(Err(e)) => {
                    ui.centered_and_justified(|ui| ui.colored_label(RED, e));
                }
                Some(Ok(list)) => {
                    egui::ScrollArea::vertical().stick_to_bottom(true).auto_shrink([false, false]).show(ui, |ui| {
                        ui.add_space(16.0);
                        if list.is_empty() {
                            ui.label(RichText::new(format!("This is the beginning of {}.", sel.title)).color(MUTED));
                        }
                        let mut prev: Option<&Msg> = None;
                        for m in list {
                            // Date divider, like Discord's "──── November 27, 2026 ────".
                            if !m.day.is_empty() && prev.map_or(true, |p| p.day != m.day) {
                                ui.add_space(14.0);
                                let w = ui.available_width();
                                let (r, _) = ui.allocate_exact_size(Vec2::new(w, 14.0), Sense::hover());
                                let y = r.center().y;
                                ui.painter().hline(r.left() + 16.0..=r.right() - 16.0, y, Stroke::new(1.0_f32, INPUT));
                                let g = ui.painter().layout_no_wrap(m.day.clone(), FontId::proportional(12.0), MUTED);
                                let bg = Rect::from_center_size(r.center(), g.size() + Vec2::new(12.0, 2.0));
                                ui.painter().rect_filled(bg, 0.0, CHAT);
                                ui.painter().galley(bg.min + Vec2::new(6.0, 1.0), g, MUTED);
                                prev = None;
                            }
                            let first = m.reply.is_some() || prev.map_or(true, |p| p.author != m.author);
                            prev = Some(m);
                            if first {
                                ui.add_space(14.0);
                            }
                            // Paint the hover background behind the row once we know its size.
                            let bg = ui.painter().add(egui::Shape::Noop);
                            let row = ui.vertical(|ui| {
                                if let Some((who, text)) = &m.reply {
                                    ui.horizontal(|ui| {
                                        ui.add_space(36.0);
                                        ui.label(RichText::new("╭").color(MUTED));
                                        ui.label(RichText::new(format!("@{who}")).size(13.0).strong().color(MUTED));
                                        ui.label(RichText::new(text).size(13.0).color(MUTED));
                                    });
                                }
                                ui.horizontal_top(|ui| {
                                    ui.add_space(16.0);
                                    if first {
                                        ui.add(egui::Image::new(m.avatar.as_str()).fit_to_exact_size(Vec2::splat(40.0)).rounding(20.0));
                                    } else {
                                        ui.add_space(40.0);
                                    }
                                    ui.add_space(14.0);
                                    ui.vertical(|ui| {
                                        ui.set_max_width(ui.available_width() - 16.0);
                                        if first {
                                            ui.horizontal(|ui| {
                                                ui.label(RichText::new(&m.author).strong().size(15.5).color(BRIGHT));
                                                ui.label(RichText::new(&m.time).size(12.0).color(MUTED));
                                            });
                                        }
                                        if !m.content.is_empty() {
                                            markdown(ui, &m.content);
                                        }
                                        for img in &m.images {
                                            ui.add(egui::Image::new(img.url.as_str()).fit_to_exact_size(img.size).rounding(6.0));
                                        }
                                        if !m.reactions.is_empty() {
                                            ui.horizontal_wrapped(|ui| {
                                                for r in &m.reactions {
                                                    let stroke = if r.me { Stroke::new(1.0_f32, ACCENT) } else { Stroke::NONE };
                                                    let fill = if r.me { Color32::from_rgb(55, 59, 97) } else { INPUT };
                                                    let chip = egui::Frame::none().fill(fill).stroke(stroke).rounding(8.0).inner_margin(egui::Margin::symmetric(6.0, 2.0)).show(ui, |ui| {
                                                        ui.horizontal(|ui| {
                                                            match &r.image {
                                                                Some(url) => {
                                                                    ui.add(egui::Image::new(url.as_str()).fit_to_exact_size(Vec2::splat(16.0)));
                                                                }
                                                                None => {
                                                                    ui.label(RichText::new(&r.label).size(15.0));
                                                                }
                                                            }
                                                            ui.label(RichText::new(r.count.to_string()).size(13.0).strong().color(if r.me { BRIGHT } else { MUTED }));
                                                        });
                                                    });
                                                    let chip = chip.response.interact(Sense::click()).on_hover_cursor(egui::CursorIcon::PointingHand);
                                                    if chip.clicked() {
                                                        action = Some(MsgAction::React(m.id.clone(), r.api_key(), !r.me));
                                                    }
                                                }
                                            });
                                        }
                                    });
                                });
                            });
                            let rect = row.response.rect.expand2(Vec2::new(0.0, 1.0));
                            let full = Rect::from_x_y_ranges(ui.max_rect().x_range(), rect.y_range());
                            let hovered = ui.rect_contains_pointer(full);
                            if hovered && ui.input(|i| i.pointer.secondary_clicked()) {
                                if let Some(pos) = ui.input(|i| i.pointer.interact_pos()) {
                                    open_menu = Some((sel.id.clone(), m.id.clone(), pos));
                                }
                            }
                            if m.pings_me {
                                // Mentions you: gold tint + bar, like Discord.
                                ui.painter().set(bg, egui::Shape::rect_filled(full, 0.0, Color32::from_rgb(68, 63, 50)));
                                ui.painter().rect_filled(Rect::from_min_size(full.min, Vec2::new(2.0, full.height())), 0.0, Color32::from_rgb(240, 178, 50));
                            } else if hovered {
                                ui.painter().set(bg, egui::Shape::rect_filled(full, 0.0, Color32::from_rgb(46, 48, 53)));
                            }
                            if hovered {
                                if !first && !m.short.is_empty() {
                                    ui.painter().text(
                                        egui::pos2(full.left() + 36.0, full.top() + 10.0),
                                        Align2::CENTER_CENTER,
                                        &m.short,
                                        FontId::proportional(10.5),
                                        MUTED,
                                    );
                                }
                            }
                        }
                        ui.add_space(8.0);
                    });
                }
            }
        });
        if let Some(m) = open_menu {
            self.msg_menu = Some(m);
        }
        self.message_menu(ctx, &mut action);
        if let Some(a) = action {
            self.apply_msg_action(ctx, a);
        }
        self.apply_voice(vact);
    }

    /// Keyboard shortcuts + files dropped onto the window.
    fn shortcuts(&mut self, ctx: &egui::Context) {
        use egui::Key;
        if ctx.input(|i| i.modifiers.command && i.key_pressed(Key::Comma)) {
            self.open_settings();
        }
        let (k, mute, deaf, members, emoji, esc) = ctx.input(|i| {
            let c = i.modifiers.command;
            (
                c && i.key_pressed(Key::K),
                c && i.modifiers.shift && i.key_pressed(Key::M),
                c && i.modifiers.shift && i.key_pressed(Key::D),
                c && !i.modifiers.shift && i.key_pressed(Key::U),
                c && i.key_pressed(Key::E),
                i.key_pressed(Key::Escape),
            )
        });
        if k {
            self.qs_open = !self.qs_open;
            self.qs_query.clear();
        }
        if mute {
            self.apply_voice(Some(VAction::ToggleMute));
        }
        if deaf {
            self.apply_voice(Some(VAction::ToggleDeaf));
        }
        if members {
            self.show_members = !self.show_members;
        }
        if emoji {
            self.emoji_open = !self.emoji_open;
        }
        if esc {
            self.qs_open = false;
            self.emoji_open = false;
            self.vol_popup = None;
            if self.settings_open && !self.recording_key {
                self.close_settings();
            }
        }
        let dropped: Vec<PathBuf> = ctx.input(|i| i.raw.dropped_files.iter().filter_map(|f| f.path.clone()).collect());
        if !dropped.is_empty() && self.sel.as_ref().map_or(false, |s| !s.voice) {
            self.files.extend(dropped);
        }
        if ctx.input(|i| !i.raw.hovered_files.is_empty()) {
            egui::Area::new(egui::Id::new("drop_hint")).anchor(Align2::CENTER_CENTER, [0.0, 0.0]).order(egui::Order::Foreground).show(ctx, |ui| {
                egui::Frame::none().fill(ACCENT).rounding(12.0).inner_margin(28.0).show(ui, |ui| {
                    ui.label(RichText::new("Drop to upload").size(22.0).strong().color(Color32::WHITE));
                });
            });
        }
    }

    /// Ctrl+K: jump to any server channel or DM by typing part of its name.
    fn quick_switcher(&mut self, ctx: &egui::Context) {
        let q = self.qs_query.to_lowercase();
        let mut items: Vec<(String, String, Selected, View)> = Vec::new();
        for d in &self.dms {
            items.push((format!("@ {}", d.name), "Direct Message".into(), Selected { id: d.id.clone(), title: d.name.clone(), guild: None, voice: false }, View::Dms));
        }
        for g in &self.guilds {
            for c in g.channels.iter().filter(|c| c.kind != 4) {
                let icon = if c.is_voice() { "🔊" } else { "#" };
                items.push((
                    format!("{icon} {}", c.name),
                    g.name.clone(),
                    Selected { id: c.id.clone(), title: c.name.clone(), guild: Some(g.id.clone()), voice: c.is_voice() },
                    View::Guild(g.id.clone()),
                ));
            }
            items.push((format!("🏠 {}", g.name), "Server".into(), Selected { id: String::new(), title: String::new(), guild: Some(g.id.clone()), voice: false }, View::Guild(g.id.clone())));
        }
        items.retain(|it| q.is_empty() || it.0.to_lowercase().contains(&q) || it.1.to_lowercase().contains(&q));
        items.truncate(10);
        let mut pick = None;
        egui::Window::new("quick_switcher")
            .title_bar(false)
            .resizable(false)
            .anchor(Align2::CENTER_TOP, [0.0, 110.0])
            .fixed_size([540.0, 0.0])
            .frame(egui::Frame::window(&ctx.style()).fill(SIDE).rounding(8.0).inner_margin(16.0))
            .show(ctx, |ui| {
                ui.label(RichText::new("Where would you like to go?").size(18.0).strong().color(BRIGHT));
                ui.add_space(6.0);
                let field = ui.add(
                    egui::TextEdit::singleline(&mut self.qs_query)
                        .hint_text("Search for servers, channels or DMs")
                        .font(FontId::proportional(17.0))
                        .desired_width(f32::INFINITY),
                );
                field.request_focus();
                ui.add_space(6.0);
                for (i, it) in items.iter().enumerate() {
                    let text = RichText::new(format!("{}    {}", it.0, it.1)).size(15.0).color(if i == 0 { BRIGHT } else { TEXT });
                    if ui.add(egui::Button::new(text).fill(if i == 0 { ACTIVE } else { SIDE }).min_size(Vec2::new(508.0, 32.0))).clicked() {
                        pick = Some(i);
                    }
                }
                if items.is_empty() {
                    ui.label(RichText::new("No matches").color(MUTED));
                }
                ui.add_space(4.0);
                ui.label(RichText::new("Enter to open  ·  Esc to close").size(12.0).color(MUTED));
                if ui.input(|i| i.key_pressed(egui::Key::Enter)) && !items.is_empty() {
                    pick = Some(0);
                }
            });
        if let Some((_, _, sel, view)) = pick.and_then(|i| items.into_iter().nth(i)) {
            self.view = view;
            if !sel.id.is_empty() {
                self.open(ctx, sel);
            }
            self.qs_open = false;
        }
    }

    fn open_settings(&mut self) {
        self.devices = voice::list_devices();
        self.settings_open = true;
    }

    fn close_settings(&mut self) {
        self.settings_open = false;
        self.recording_key = false;
        if self.mic_testing {
            self.mic_testing = false;
            let _ = self.vtx.send(voice::VoiceMsg::MicTest(false));
        }
    }

    /// User Settings → Voice & Video.
    fn settings_window(&mut self, ctx: &egui::Context) {
        let level = f32::from_bits(self.meter.swap(0, std::sync::atomic::Ordering::Relaxed));
        self.meter_val = (self.meter_val * 0.8).max(level);
        ctx.request_repaint_after(Duration::from_millis(33));

        let before = self.vcfg.lock().unwrap().clone();
        let mut c = before.clone();
        let mut open = true;
        let mut toggle_test = false;
        egui::Window::new(RichText::new("Voice & Video").size(20.0).strong().color(BRIGHT))
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
            .fixed_size([600.0, 0.0])
            .frame(egui::Frame::window(&ctx.style()).fill(CHAT).rounding(8.0).inner_margin(20.0))
            .show(ctx, |ui| {
                let section = |ui: &mut egui::Ui, t: &str| {
                    ui.add_space(14.0);
                    ui.label(RichText::new(t.to_uppercase()).size(12.0).strong().color(MUTED));
                    ui.add_space(4.0);
                };
                ui.columns(2, |cols| {
                    section(&mut cols[0], "Input Device");
                    egui::ComboBox::from_id_salt("in_dev")
                        .width(270.0)
                        .selected_text(c.input_dev.clone().unwrap_or_else(|| "Default".into()))
                        .show_ui(&mut cols[0], |ui| {
                            ui.selectable_value(&mut c.input_dev, None, "Default");
                            for d in &self.devices.0 {
                                ui.selectable_value(&mut c.input_dev, Some(d.clone()), d);
                            }
                        });
                    section(&mut cols[1], "Output Device");
                    egui::ComboBox::from_id_salt("out_dev")
                        .width(270.0)
                        .selected_text(c.output_dev.clone().unwrap_or_else(|| "Default".into()))
                        .show_ui(&mut cols[1], |ui| {
                            ui.selectable_value(&mut c.output_dev, None, "Default");
                            for d in &self.devices.1 {
                                ui.selectable_value(&mut c.output_dev, Some(d.clone()), d);
                            }
                        });
                    section(&mut cols[0], "Input Volume");
                    let mut v = c.in_vol * 100.0;
                    cols[0].add(egui::Slider::new(&mut v, 0.0..=200.0).suffix("%").fixed_decimals(0));
                    c.in_vol = v / 100.0;
                    section(&mut cols[1], "Output Volume");
                    let mut v = c.out_vol * 100.0;
                    cols[1].add(egui::Slider::new(&mut v, 0.0..=200.0).suffix("%").fixed_decimals(0));
                    c.out_vol = v / 100.0;
                });

                section(ui, "Mic Test");
                ui.horizontal(|ui| {
                    let label = if self.mic_testing { "Stop Testing" } else { "Let's Check" };
                    if ui.add(egui::Button::new(RichText::new(label).color(Color32::WHITE)).fill(ACCENT).min_size(Vec2::new(120.0, 32.0))).clicked() {
                        toggle_test = true;
                    }
                    level_bar(ui, self.meter_val, None);
                });
                if self.in_voice.is_some() {
                    ui.label(RichText::new("You're in a call: the meter shows your mic live.").size(12.0).color(MUTED));
                }

                section(ui, "Input Mode");
                ui.radio_value(&mut c.ptt, false, "Voice Activity");
                ui.radio_value(&mut c.ptt, true, "Push to Talk");
                if c.ptt {
                    section(ui, "Shortcut");
                    ui.horizontal(|ui| {
                        let key = if self.recording_key { "Press any key or mouse button...".to_string() } else { voice::key_name(c.ptt_key) };
                        egui::Frame::none().fill(INPUT).rounding(4.0).inner_margin(egui::Margin::symmetric(12.0, 6.0)).show(ui, |ui| {
                            ui.set_min_width(240.0);
                            ui.label(RichText::new(key).strong().color(BRIGHT));
                        });
                        let rec = if self.recording_key { "Stop Recording" } else { "Edit Keybind" };
                        if ui.button(rec).clicked() {
                            self.recording_key = !self.recording_key;
                        }
                    });
                    if self.recording_key {
                        if let Some(k) = voice::held_key() {
                            c.ptt_key = k;
                            self.recording_key = false;
                        }
                    }
                    ui.label(RichText::new("Works while other apps (like games) are focused.").size(12.0).color(MUTED));
                } else {
                    section(ui, "Input Sensitivity");
                    ui.label(RichText::new("Your mic sends when the meter goes past the white line.").size(12.0).color(MUTED));
                    level_bar(ui, self.meter_val, Some(c.gate));
                    ui.add(egui::Slider::new(&mut c.gate, 0.001..=0.25).logarithmic(true).show_value(false).text("sensitivity"));
                }

                section(ui, "Voice Processing");
                ui.checkbox(&mut c.denoise, "Noise Suppression (removes background noise like fans and typing)");
                ui.label(RichText::new("Echo cancellation isn't available yet: use headphones to avoid echo.").size(12.0).color(MUTED));
            });

        if toggle_test {
            self.mic_testing = !self.mic_testing;
            let _ = self.vtx.send(voice::VoiceMsg::MicTest(self.mic_testing));
        }
        if c != before {
            let devices_changed = c.input_dev != before.input_dev || c.output_dev != before.output_dev;
            c.save(&settings_path());
            *self.vcfg.lock().unwrap() = c;
            if devices_changed {
                let _ = self.vtx.send(voice::VoiceMsg::RestartAudio);
            }
        }
        if !open {
            self.close_settings();
        }
    }

    /// Right-click a voice tile: that person's volume (0–200%).
    fn volume_popup(&mut self, ctx: &egui::Context) {
        if let Some(v) = ctx.data_mut(|d| d.remove_temp::<(String, String, egui::Pos2)>(egui::Id::new("vol_click"))) {
            self.vol_popup = Some(v);
        }
        let Some((user, name, pos)) = self.vol_popup.clone() else { return };
        let Ok(uid) = user.parse::<u64>() else {
            self.vol_popup = None;
            return;
        };
        let mut vol = self.vcfg.lock().unwrap().user_vol.get(&uid).copied().unwrap_or(1.0) * 100.0;
        let before = vol;
        let area = egui::Area::new(egui::Id::new("vol_popup")).fixed_pos(pos).order(egui::Order::Foreground).show(ctx, |ui| {
            egui::Frame::popup(ui.style()).fill(Color32::from_rgb(17, 18, 20)).rounding(6.0).inner_margin(10.0).show(ui, |ui| {
                ui.label(RichText::new(&name).strong().color(BRIGHT));
                ui.label(RichText::new("User Volume").size(12.0).color(MUTED));
                ui.add(egui::Slider::new(&mut vol, 0.0..=200.0).suffix("%").fixed_decimals(0));
                if ui.button("Reset to 100%").clicked() {
                    vol = 100.0;
                }
            });
        });
        if (vol - before).abs() > f32::EPSILON {
            let mut c = self.vcfg.lock().unwrap();
            if (vol - 100.0).abs() < 0.5 {
                c.user_vol.remove(&uid);
            } else {
                c.user_vol.insert(uid, vol / 100.0);
            }
            c.save(&settings_path());
        }
        let outside = ctx.input(|i| i.pointer.any_pressed())
            && ctx.input(|i| i.pointer.interact_pos()).map_or(false, |p| !area.response.rect.contains(p) && p != pos);
        if outside {
            self.vol_popup = None;
        }
    }

    /// Discord-style right-click menu for a message.
    fn message_menu(&mut self, ctx: &egui::Context, action: &mut Option<MsgAction>) {
        let Some((ch, mid, pos)) = self.msg_menu.clone() else { return };
        let Some(m) = self.cache.get(&ch).and_then(|r| r.as_ref().ok()).and_then(|l| l.iter().find(|m| m.id == mid)).cloned() else {
            self.msg_menu = None;
            return;
        };
        let mine = m.author_id == self.me_id;
        let link = match self.chan_guild.get(&ch) {
            Some(g) => format!("https://discord.com/channels/{g}/{ch}/{mid}"),
            None => format!("https://discord.com/channels/@me/{ch}/{mid}"),
        };
        let mut chosen = None;
        let area = egui::Area::new(egui::Id::new("msg_menu")).fixed_pos(pos).order(egui::Order::Foreground).show(ctx, |ui| {
            egui::Frame::popup(ui.style()).fill(Color32::from_rgb(17, 18, 20)).rounding(6.0).inner_margin(6.0).show(ui, |ui| {
                ui.set_min_width(200.0);
                ui.horizontal(|ui| {
                    for e in QUICK_REACTIONS {
                        if ui.add(egui::Button::new(RichText::new(e).size(18.0)).frame(false)).on_hover_text("Add reaction").clicked() {
                            let already = m.reactions.iter().any(|r| r.label == e && r.me);
                            chosen = Some(MsgAction::React(mid.clone(), pct(e), !already));
                        }
                    }
                });
                ui.separator();
                let item = |ui: &mut egui::Ui, label: &str, color: Color32| {
                    ui.add_sized([200.0, 26.0], egui::Button::new(RichText::new(label).color(color)).frame(false)).clicked()
                };
                if item(ui, "↩  Reply", TEXT) {
                    chosen = Some(MsgAction::Reply(mid.clone(), m.author.clone()));
                }
                if mine && item(ui, "✏  Edit Message", TEXT) {
                    chosen = Some(MsgAction::Edit(mid.clone(), m.raw.clone()));
                }
                if item(ui, "📌  Pin Message", TEXT) {
                    chosen = Some(MsgAction::Pin(mid.clone()));
                }
                ui.separator();
                if item(ui, "📋  Copy Text", TEXT) {
                    chosen = Some(MsgAction::Copy(m.raw.clone()));
                }
                if item(ui, "🔗  Copy Message Link", TEXT) {
                    chosen = Some(MsgAction::Copy(link.clone()));
                }
                if item(ui, "🆔  Copy Message ID", TEXT) {
                    chosen = Some(MsgAction::Copy(mid.clone()));
                }
                if mine {
                    ui.separator();
                    if item(ui, "🗑  Delete Message", RED) {
                        chosen = Some(MsgAction::Delete(mid.clone()));
                    }
                }
            });
        });
        let clicked_outside = ctx.input(|i| i.pointer.any_pressed())
            && ctx.input(|i| i.pointer.interact_pos()).map_or(false, |p| !area.response.rect.contains(p) && p != pos);
        if chosen.is_some() || clicked_outside || ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
            self.msg_menu = None;
        }
        if chosen.is_some() {
            *action = chosen;
        }
    }

    fn apply_msg_action(&mut self, ctx: &egui::Context, a: MsgAction) {
        let Some(ch) = self.sel.as_ref().map(|s| s.id.clone()) else { return };
        let focus = || ctx.memory_mut(|m| m.request_focus(egui::Id::new("composer_field")));
        match a {
            MsgAction::Reply(id, who) => {
                self.editing = None;
                self.reply_to = Some((id, who));
                focus();
            }
            MsgAction::Edit(id, raw) => {
                self.reply_to = None;
                self.editing = Some(id);
                self.draft = raw;
                focus();
            }
            MsgAction::Copy(text) => {
                ctx.output_mut(|o| o.copied_text = text);
                self.status = "Copied".into();
            }
            MsgAction::Delete(id) => {
                if let Some(r) = &self.rest {
                    let _ = r.send(Cmd::Delete(ch, id));
                }
            }
            MsgAction::React(id, key, add) => {
                if let Some(r) = &self.rest {
                    let _ = r.send(Cmd::React(ch, id, key, add));
                }
            }
            MsgAction::Pin(id) => {
                if let Some(r) = &self.rest {
                    let _ = r.send(Cmd::Pin(ch, id));
                }
            }
        }
    }

    /// Name and picture for someone in voice (ourselves included).
    fn member_face<'a>(&'a self, m: &'a VoiceMember) -> (&'a str, &'a str) {
        if m.user == self.me_id && !self.me.is_empty() {
            (self.me.as_str(), self.me_avatar.as_str())
        } else {
            (m.name.as_str(), m.avatar.as_str())
        }
    }

    /// Big participant tiles with a green outline on whoever is talking and a LIVE badge on
    /// whoever is sharing their screen. Returns (user id, name) of a live tile that was clicked.
    fn voice_tiles(&self, ui: &mut egui::Ui, channel: &str, speaking: &HashSet<String>) -> Option<(String, String)> {
        let members = self.voice_members.get(channel).map(|v| v.as_slice()).unwrap_or(&[]);
        let mut clicked = None;
        ui.horizontal_wrapped(|ui| {
            ui.spacing_mut().item_spacing = Vec2::splat(12.0);
            for m in members {
                let (name, av) = self.member_face(m);
                let live = m.streaming && m.user != self.me_id;
                let sense = if live { Sense::click() } else { Sense::hover() };
                let (rect, resp) = ui.allocate_exact_size(Vec2::new(190.0, 120.0), sense);
                let p = ui.painter().clone();
                p.rect_filled(rect, 10.0, if live && resp.hovered() { ACTIVE } else { INPUT });
                if speaking.contains(&m.user) {
                    p.rect_stroke(rect, 10.0, Stroke::new(3.0_f32, GREEN));
                }
                let face = Rect::from_center_size(rect.center() - Vec2::new(0.0, 12.0), Vec2::splat(64.0));
                egui::Image::new(av).rounding(32.0).paint_at(ui, face);
                p.text(egui::pos2(rect.center().x, rect.bottom() - 16.0), Align2::CENTER_CENTER, name, FontId::proportional(14.0), BRIGHT);
                // Right-click someone else's tile: their volume.
                if m.user != self.me_id && ui.rect_contains_pointer(rect) && ui.input(|i| i.pointer.secondary_clicked()) {
                    if let Some(pos) = ui.input(|i| i.pointer.interact_pos()) {
                        let pick = (m.user.clone(), name.to_string(), pos);
                        ui.ctx().data_mut(|d| d.insert_temp(egui::Id::new("vol_click"), pick));
                    }
                }
                let vol = m.user.parse::<u64>().ok().and_then(|u| self.vcfg.lock().unwrap().user_vol.get(&u).copied());
                if let Some(v) = vol.filter(|v| (*v - 1.0).abs() > 0.01) {
                    p.text(rect.right_top() + Vec2::new(-10.0, 16.0), Align2::RIGHT_CENTER, format!("🔉 {:.0}%", v * 100.0), FontId::proportional(11.0), MUTED);
                }
                if m.streaming {
                    let badge = Rect::from_min_size(rect.left_top() + Vec2::new(8.0, 8.0), Vec2::new(38.0, 18.0));
                    p.rect_filled(badge, 4.0, RED);
                    p.text(badge.center(), Align2::CENTER_CENTER, "LIVE", FontId::proportional(11.0), Color32::WHITE);
                }
                if live {
                    if resp.hovered() {
                        p.text(rect.center() - Vec2::new(0.0, 12.0), Align2::CENTER_CENTER, "Watch Stream", FontId::proportional(15.0), Color32::WHITE);
                    }
                    if resp.on_hover_cursor(egui::CursorIcon::PointingHand).clicked() {
                        clicked = Some((m.user.clone(), name.to_string()));
                    }
                }
            }
        });
        clicked
    }

    /// The screen share we're watching, scaled to fit.
    fn stream_view(&self, ui: &mut egui::Ui, vact: &mut Option<VAction>) {
        let Some((_, who)) = &self.watching else {
            if !self.stream_note.is_empty() {
                ui.label(RichText::new(&self.stream_note).color(MUTED));
            }
            return;
        };
        ui.horizontal(|ui| {
            ui.label(RichText::new(format!("🔴 {who}'s screen")).strong().color(BRIGHT));
            if ui.button("Stop watching").clicked() {
                *vact = Some(VAction::StopWatch);
            }
        });
        ui.add_space(8.0);
        let max = Vec2::new(ui.available_width() - 16.0, (ui.available_height() * 0.65).max(200.0));
        match &self.stream_tex {
            Some(t) => {
                let size = t.size_vec2();
                let scale = (max.x / size.x).min(max.y / size.y);
                ui.add(egui::Image::new(egui::load::SizedTexture::new(t.id(), size * scale)).rounding(8.0));
            }
            None => {
                let (rect, _) = ui.allocate_exact_size(Vec2::new(max.x, max.x * 9.0 / 16.0).min(max), Sense::hover());
                ui.painter().rect_filled(rect, 8.0, Color32::BLACK);
                let msg = if !self.stream_note.is_empty() {
                    self.stream_note.as_str()
                } else if self.stream_live {
                    "Waiting for the first picture..."
                } else {
                    "Connecting to the stream..."
                };
                ui.painter().text(rect.center(), Align2::CENTER_CENTER, msg, FontId::proportional(16.0), MUTED);
            }
        }
        ui.add_space(12.0);
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.handle_events(ctx);
        // Newest screen-share picture -> texture.
        if let Some(img) = self.frame_slot.lock().unwrap().take() {
            match &mut self.stream_tex {
                Some(t) => t.set(img, egui::TextureOptions::LINEAR),
                None => self.stream_tex = Some(ctx.load_texture("stream", img, egui::TextureOptions::LINEAR)),
            }
        }
        if !self.member_requests.is_empty() && self.phase == Phase::Main {
            self.flush_member_requests();
        }
        if self.phase == Phase::Main {
            self.shortcuts(ctx);
            self.main_screen(ctx);
            if self.qs_open {
                self.quick_switcher(ctx);
            }
            if self.settings_open {
                self.settings_window(ctx);
            }
            self.volume_popup(ctx);
        } else {
            self.login_screen(ctx);
        }
    }
}

/// Write a log to %LOCALAPPDATA%\Rustcord\log.txt (voice steps + songbird's own messages).
/// Tokens are never written: only ids, endpoints and states.
fn init_log() {
    let p = token_path().with_file_name("log.txt");
    if let Some(dir) = p.parent() {
        let _ = fs::create_dir_all(dir);
    }
    // Keep the previous run's log so a restart doesn't wipe a test.
    let _ = fs::rename(&p, p.with_file_name("log-prev.txt"));
    if let Ok(file) = fs::File::create(&p) {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(file))
            .with_ansi(false)
            .with_env_filter("songbird=debug,davey=info,rustcord=debug")
            .try_init();
    }
}

fn main() -> eframe::Result<()> {
    init_log();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1250.0, 760.0])
            .with_min_inner_size([800.0, 500.0])
            .with_title("Rustcord"),
        ..Default::default()
    };
    eframe::run_native("Rustcord", options, Box::new(|cc| Ok(Box::new(App::new(cc)))))
}
