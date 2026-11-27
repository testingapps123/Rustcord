// QR-code sign-in, the same flow the official desktop app uses:
// 1. connect to Discord's remote-auth gateway and send a fresh RSA public key
// 2. prove we hold the private key (decrypt a nonce)
// 3. show the fingerprint as a QR code; the user scans it in the Discord phone app
// 4. after they confirm on the phone, exchange the ticket for an encrypted sign-in key
// The user never sees or handles the key.

use crate::{Evt, UA};
use base64::{engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD}, Engine};
use eframe::egui;
use rsa::{pkcs8::EncodePublicKey, Oaep, RsaPrivateKey, RsaPublicKey};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tungstenite::{client::IntoClientRequest, stream::MaybeTlsStream, Message as WsMessage};

const RA_GATEWAY: &str = "wss://remote-auth-gateway.discord.gg/?v=2";
const RA_LOGIN: &str = "https://discord.com/api/v9/users/@me/remote-auth/login";

pub fn run(out: Sender<Evt>, ctx: egui::Context, cancel: Arc<AtomicBool>) {
    while !cancel.load(Ordering::Relaxed) {
        match session(&out, &ctx, &cancel) {
            Ok(Some(token)) => {
                let _ = out.send(Evt::LoginToken(token));
                ctx.request_repaint();
                return;
            }
            Ok(None) => {} // QR expired or was cancelled on the phone: make a new one
            Err(e) if e.contains("CAPTCHA") => {
                // Don't keep hammering Discord: stop and let the user decide when to try again.
                let _ = out.send(Evt::QrFailed(e));
                ctx.request_repaint();
                return;
            }
            Err(e) => {
                let _ = out.send(Evt::QrNote(format!("QR sign-in problem: {e}. Retrying...")));
                ctx.request_repaint();
                std::thread::sleep(Duration::from_secs(5));
            }
        }
    }
}

fn decrypt(key: &RsaPrivateKey, b64: &str) -> Result<Vec<u8>, String> {
    let bytes = STANDARD.decode(b64).map_err(|e| e.to_string())?;
    key.decrypt(Oaep::new::<Sha256>(), &bytes).map_err(|e| e.to_string())
}

fn session(out: &Sender<Evt>, ctx: &egui::Context, cancel: &AtomicBool) -> Result<Option<String>, String> {
    let key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).map_err(|e| e.to_string())?;
    let public_der = RsaPublicKey::from(&key).to_public_key_der().map_err(|e| e.to_string())?;

    let mut req = RA_GATEWAY.into_client_request().map_err(|e| e.to_string())?;
    req.headers_mut().insert("Origin", "https://discord.com".parse().unwrap());
    req.headers_mut().insert("User-Agent", UA.parse().unwrap());
    let (mut ws, _) = tungstenite::connect(req).map_err(|e| e.to_string())?;
    let _ = match ws.get_mut() {
        MaybeTlsStream::NativeTls(s) => s.get_mut().set_read_timeout(Some(Duration::from_millis(250))),
        MaybeTlsStream::Plain(s) => s.set_read_timeout(Some(Duration::from_millis(250))),
        _ => Ok(()),
    };

    let mut interval = Duration::from_secs(40);
    let mut last_beat = Instant::now();
    let send = |ws: &mut tungstenite::WebSocket<MaybeTlsStream<std::net::TcpStream>>, v: Value| {
        ws.send(WsMessage::Text(v.to_string())).map_err(|e| e.to_string())
    };

    loop {
        if cancel.load(Ordering::Relaxed) {
            let _ = ws.close(None);
            return Ok(None);
        }
        if last_beat.elapsed() >= interval {
            send(&mut ws, json!({ "op": "heartbeat" }))?;
            last_beat = Instant::now();
        }
        let text = match ws.read() {
            Ok(WsMessage::Text(t)) => t,
            Ok(WsMessage::Close(_)) => return Ok(None),
            Ok(_) => continue,
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) =>
            {
                continue
            }
            Err(tungstenite::Error::ConnectionClosed) | Err(tungstenite::Error::AlreadyClosed) => return Ok(None),
            Err(e) => return Err(e.to_string()),
        };
        let v: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        match v["op"].as_str().unwrap_or("") {
            "hello" => {
                interval = Duration::from_millis(v["heartbeat_interval"].as_u64().unwrap_or(41_250));
                last_beat = Instant::now();
                send(&mut ws, json!({ "op": "init", "encoded_public_key": STANDARD.encode(public_der.as_bytes()) }))?;
            }
            "nonce_proof" => {
                let nonce = decrypt(&key, v["encrypted_nonce"].as_str().unwrap_or(""))?;
                let proof = URL_SAFE_NO_PAD.encode(Sha256::digest(&nonce));
                send(&mut ws, json!({ "op": "nonce_proof", "proof": proof }))?;
            }
            "pending_remote_init" => {
                let fp = v["fingerprint"].as_str().unwrap_or("");
                let _ = out.send(Evt::Qr(format!("https://discord.com/ra/{fp}")));
                ctx.request_repaint();
            }
            "pending_ticket" => {
                let raw = decrypt(&key, v["encrypted_user_payload"].as_str().unwrap_or(""))?;
                // "id:discriminator:avatar:username"
                let user = String::from_utf8_lossy(&raw).rsplit(':').next().unwrap_or("").to_string();
                let _ = out.send(Evt::QrNote(format!("Scanned by {user}. Tap \"Log in\" on your phone.")));
                ctx.request_repaint();
            }
            "pending_login" => {
                let ticket = v["ticket"].as_str().unwrap_or("").to_string();
                let _ = ws.close(None);
                return exchange_ticket(&key, &ticket).map(Some);
            }
            "finish" => {
                // older protocol version: the key arrives directly
                let raw = decrypt(&key, v["encrypted_token"].as_str().unwrap_or(""))?;
                return Ok(Some(String::from_utf8_lossy(&raw).to_string()));
            }
            "cancel" => {
                let _ = out.send(Evt::QrNote("Sign-in was cancelled on the phone. Here's a new code.".into()));
                ctx.request_repaint();
                return Ok(None);
            }
            _ => {}
        }
    }
}

fn exchange_ticket(key: &RsaPrivateKey, ticket: &str) -> Result<String, String> {
    let client = reqwest::blocking::Client::builder()
        .user_agent(UA)
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client.post(RA_LOGIN).json(&json!({ "ticket": ticket })).send().map_err(|e| e.to_string())?;
    let status = resp.status();
    let body: Value = resp.json().unwrap_or(Value::Null);
    if !status.is_success() {
        if body.get("captcha_key").is_some() {
            return Err("Discord asked for a CAPTCHA, which Rustcord can't show. Sign in once with the official app, then try again later".into());
        }
        return Err(format!("Discord returned {status}"));
    }
    let raw = decrypt(key, body["encrypted_token"].as_str().unwrap_or(""))?;
    Ok(String::from_utf8_lossy(&raw).to_string())
}
