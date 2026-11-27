// Email/phone + password sign-in (with 2FA codes), straight to discord.com over HTTPS.
// The password is only held in memory for the request and never written anywhere.
// If Discord asks for a CAPTCHA we stop and say so: Rustcord does not get around CAPTCHAs.

use crate::UA;
use serde_json::{json, Value};
use std::time::Duration;

const API: &str = "https://discord.com/api/v9";

static LOGIN_INSTANCE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

pub enum LoginResult {
    Token(String),
    /// 2FA needed: ticket + whether an authenticator-app code works
    Mfa { ticket: String, totp: bool },
    Error(String),
}

/// One shared connection with a cookie jar, so the 2FA step is recognised as the same
/// sign-in attempt as the password step (Discord ties them together with cookies).
fn client() -> Result<reqwest::blocking::Client, String> {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    if let Some(c) = CLIENT.get() {
        return Ok(c.clone());
    }
    let c = reqwest::blocking::Client::builder()
        .user_agent(UA)
        .cookie_store(true)
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| e.to_string())?;
    Ok(CLIENT.get_or_init(|| c).clone())
}

/// Log Discord's answer without anything secret in it.
fn log_reply(step: &str, status: u16, body: &Value) {
    let mut safe = body.clone();
    for k in ["token", "ticket", "login_instance_id"] {
        if safe.get(k).is_some() {
            safe[k] = json!("<hidden>");
        }
    }
    tracing::info!("login: {step} -> {status} {safe}");
}

/// Pull a readable message out of Discord's error JSON.
fn error_text(status: u16, body: &Value) -> String {
    if body.get("captcha_key").is_some() {
        return "Discord asked for a CAPTCHA, which Rustcord can't show. Sign in with the official Discord app for now, then try again later.".into();
    }
    if status == 429 {
        return "Too many attempts. Wait a few minutes and try again.".into();
    }
    // {"errors": {"login": {"_errors": [{"message": "..."}]}}}
    if let Some(errs) = body["errors"].as_object() {
        for field in errs.values() {
            if let Some(m) = field["_errors"][0]["message"].as_str() {
                return m.to_string();
            }
        }
    }
    body["message"].as_str().map(String::from).unwrap_or_else(|| format!("Discord returned {status}"))
}

fn finish(step: &str, resp: reqwest::blocking::Response) -> LoginResult {
    let status = resp.status().as_u16();
    let body: Value = resp.json().unwrap_or(Value::Null);
    log_reply(step, status, &body);
    if status == 200 {
        if let Some(t) = body["token"].as_str() {
            return LoginResult::Token(t.to_string());
        }
        if body["mfa"].as_bool() == Some(true) {
            // The 2FA step must echo this back when Discord provides it.
            *LOGIN_INSTANCE.lock().unwrap() = body["login_instance_id"].as_str().map(String::from);
            return LoginResult::Mfa { ticket: body["ticket"].as_str().unwrap_or("").to_string(), totp: body["totp"].as_bool().unwrap_or(true) };
        }
    }
    LoginResult::Error(error_text(status, &body))
}

pub fn password(login: &str, password: &str) -> LoginResult {
    let c = match client() {
        Ok(c) => c,
        Err(e) => return LoginResult::Error(e),
    };
    match c
        .post(format!("{API}/auth/login"))
        .json(&json!({ "login": login.trim(), "password": password, "undelete": false, "login_source": null, "gift_code_sku_id": null }))
        .send()
    {
        Ok(r) => finish("password", r),
        Err(e) => LoginResult::Error(format!("Couldn't reach Discord: {e}")),
    }
}

pub fn mfa(ticket: &str, code: &str) -> LoginResult {
    let c = match client() {
        Ok(c) => c,
        Err(e) => return LoginResult::Error(e),
    };
    let code: String = code.chars().filter(|c| !c.is_whitespace() && *c != '-').collect();
    tracing::info!("login: sending 2FA code ({} characters)", code.len());
    let mut body = json!({ "code": code, "ticket": ticket, "login_source": null, "gift_code_sku_id": null });
    if let Some(id) = LOGIN_INSTANCE.lock().unwrap().clone() {
        body["login_instance_id"] = json!(id);
    }
    match c.post(format!("{API}/auth/mfa/totp")).json(&body).send() {
        Ok(r) => finish("2fa", r),
        Err(e) => LoginResult::Error(format!("Couldn't reach Discord: {e}")),
    }
}
