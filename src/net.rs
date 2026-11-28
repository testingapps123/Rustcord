// Drover-style proxy: send Rustcord's connections to Discord through an HTTP or SOCKS5 proxy
// (for example the local proxy of your VPN/proxy app). Voice audio stays on direct UDP with the
// junk-packet trick (see vendor/songbird), like Discord Drover does.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Proxy {
    pub socks: bool,
    pub host: String,
    pub port: u16,
}

static PROXY: OnceLock<Option<Proxy>> = OnceLock::new();

/// "socks5://127.0.0.1:10808", "socks5h://...", "http://127.0.0.1:8080" or "127.0.0.1:8080".
pub fn parse(url: &str) -> Option<Proxy> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }
    let (socks, rest) = if let Some(r) = url.strip_prefix("socks5h://").or_else(|| url.strip_prefix("socks5://")) {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")) {
        (false, r)
    } else {
        (false, url)
    };
    let rest = rest.trim_end_matches('/');
    let (host, port) = rest.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    (!host.is_empty()).then(|| Proxy { socks, host: host.trim_matches(['[', ']']).to_string(), port })
}

/// Call once at startup with the saved setting.
pub fn init(url: Option<&str>) {
    let p = url.and_then(parse);
    if let Some(p) = &p {
        tracing::info!("net: using {} proxy {}:{}", if p.socks { "SOCKS5" } else { "HTTP" }, p.host, p.port);
        songbird::video::set_proxy(p.socks, &p.host, p.port);
    }
    let _ = PROXY.set(p);
}

pub fn get() -> Option<&'static Proxy> {
    PROXY.get().and_then(|p| p.as_ref())
}

/// HTTP client builder that goes through the proxy when one is set.
pub fn http_client() -> reqwest::blocking::ClientBuilder {
    let b = reqwest::blocking::Client::builder();
    match get() {
        Some(p) => {
            let url = if p.socks { format!("socks5h://{}:{}", p.host, p.port) } else { format!("http://{}:{}", p.host, p.port) };
            match reqwest::Proxy::all(&url) {
                Ok(px) => b.proxy(px),
                Err(_) => b,
            }
        }
        None => b,
    }
}

/// Web view proxy setting (sign-in page and web chat).
pub fn web_proxy() -> Option<wry::ProxyConfig> {
    let p = get()?;
    let ep = wry::ProxyEndpoint { host: p.host.clone(), port: p.port.to_string() };
    Some(if p.socks { wry::ProxyConfig::Socks5(ep) } else { wry::ProxyConfig::Http(ep) })
}

/// TCP connection to `host:port`, tunnelled through the proxy if one is set.
pub fn connect(host: &str, port: u16) -> io::Result<TcpStream> {
    let Some(p) = get() else { return TcpStream::connect((host, port)) };
    let mut s = TcpStream::connect((p.host.as_str(), p.port))?;
    s.set_read_timeout(Some(Duration::from_secs(15)))?;
    if p.socks {
        socks5(&mut s, host, port)?;
    } else {
        http_connect(&mut s, host, port)?;
    }
    s.set_read_timeout(None)?;
    Ok(s)
}

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg.to_string())
}

fn http_connect(s: &mut TcpStream, host: &str, port: u16) -> io::Result<()> {
    write!(s, "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n")?;
    let mut reply = Vec::new();
    let mut byte = [0u8; 1];
    while !reply.ends_with(b"\r\n\r\n") {
        if s.read(&mut byte)? == 0 || reply.len() > 8192 {
            return Err(bad("proxy closed the connection"));
        }
        reply.push(byte[0]);
    }
    let status = String::from_utf8_lossy(&reply);
    if status.split_whitespace().nth(1) == Some("200") {
        Ok(())
    } else {
        Err(bad(&format!("proxy refused: {}", status.lines().next().unwrap_or(""))))
    }
}

fn socks5(s: &mut TcpStream, host: &str, port: u16) -> io::Result<()> {
    s.write_all(&[5, 1, 0])?; // version 5, one method: no authentication
    let mut r = [0u8; 2];
    s.read_exact(&mut r)?;
    if r != [5, 0] {
        return Err(bad("SOCKS5 proxy needs a login, which isn't supported"));
    }
    let mut req = vec![5, 1, 0, 3, host.len() as u8];
    req.extend_from_slice(host.as_bytes());
    req.extend_from_slice(&port.to_be_bytes());
    s.write_all(&req)?;
    let mut head = [0u8; 4];
    s.read_exact(&mut head)?;
    if head[1] != 0 {
        return Err(bad(&format!("SOCKS5 proxy refused the connection (code {})", head[1])));
    }
    let skip = match head[3] {
        1 => 4,
        4 => 16,
        3 => {
            let mut len = [0u8; 1];
            s.read_exact(&mut len)?;
            len[0] as usize
        }
        _ => return Err(bad("bad SOCKS5 reply")),
    };
    let mut rest = vec![0u8; skip + 2];
    s.read_exact(&mut rest)?;
    Ok(())
}
