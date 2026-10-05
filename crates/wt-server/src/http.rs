//! Minimal HTTP/1.1 for the tracker: parse one request head, then either upgrade to WebSocket
//! or answer `/`, `/stats.json` or 404 and close.

use std::io;

use base64::Engine;
use sha1::{Digest, Sha1};

use crate::config::AccessConfig;
use crate::ws::deflate::{self, Negotiated};

/// Request heads larger than this are rejected.
const MAX_HEAD: usize = 8 * 1024;

/// What the server needs from a request head.
#[derive(Debug, Default)]
pub struct Head {
    pub method: String,
    pub path: String,
    pub upgrade_websocket: bool,
    pub websocket_key: Option<String>,
    pub websocket_protocol: Option<String>,
    /// Every `Sec-WebSocket-Extensions` line, joined by `, `.
    pub websocket_extensions: Option<String>,
    pub origin: Option<String>,
    /// `Authorization` (metrics listener basic auth).
    pub authorization: Option<String>,
}

/// Parses a complete request head at the start of `buf`: the head and its length, or `None` if
/// more bytes are needed. Errors on malformed or oversized heads.
pub fn parse_head(buf: &[u8]) -> io::Result<Option<(Head, usize)>> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);
    match request.parse(buf) {
        Ok(httparse::Status::Complete(len)) => {
            let mut head = Head {
                method: request.method.unwrap_or_default().to_string(),
                path: request.path.unwrap_or_default().to_string(),
                ..Head::default()
            };
            for h in request.headers.iter() {
                let value = || String::from_utf8_lossy(h.value).trim().to_string();
                if h.name.eq_ignore_ascii_case("upgrade") {
                    head.upgrade_websocket = value().eq_ignore_ascii_case("websocket");
                } else if h.name.eq_ignore_ascii_case("sec-websocket-key") {
                    head.websocket_key = Some(value());
                } else if h.name.eq_ignore_ascii_case("sec-websocket-protocol") {
                    head.websocket_protocol = Some(value());
                } else if h.name.eq_ignore_ascii_case("sec-websocket-extensions") {
                    let value = value();
                    head.websocket_extensions = Some(match head.websocket_extensions.take() {
                        Some(earlier) => format!("{earlier}, {value}"),
                        None => value,
                    });
                } else if h.name.eq_ignore_ascii_case("origin") {
                    head.origin = Some(value());
                } else if h.name.eq_ignore_ascii_case("authorization") {
                    head.authorization = Some(value());
                }
            }
            Ok(Some((head, len)))
        }
        Ok(httparse::Status::Partial) if buf.len() < MAX_HEAD => Ok(None),
        Ok(httparse::Status::Partial) => Err(io::Error::other("request head too large")),
        Err(e) => Err(io::Error::other(e)),
    }
}

/// uWebSockets-style route pattern: `/*` matches everything, `/a/*` a prefix, else exact.
pub fn path_matches(pattern: &str, path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    match pattern.strip_suffix('*') {
        Some(prefix) => path.starts_with(prefix) || path == prefix.trim_end_matches('/'),
        None => path == pattern,
    }
}

/// The JS tracker's origin rules (`websocketsAccess`).
pub fn origin_allowed(access: &AccessConfig, origin: Option<&str>) -> bool {
    let origin = origin.unwrap_or("");
    if access.deny_empty_origin && origin.is_empty() {
        return false;
    }
    if let Some(deny) = &access.deny_origins
        && deny.iter().any(|o| o == origin)
    {
        return false;
    }
    if let Some(allow) = &access.allow_origins
        && !allow.iter().any(|o| o == origin)
    {
        return false;
    }
    true
}

/// The `101 Switching Protocols` response for a valid upgrade request, and permessage-deflate
/// if `compression` is on and the client offered it acceptably.
pub fn upgrade_response(
    head: &Head,
    compression: bool,
) -> io::Result<(String, Option<Negotiated>)> {
    let key = head
        .websocket_key
        .as_deref()
        .ok_or_else(|| io::Error::other("missing Sec-WebSocket-Key"))?;
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = base64::engine::general_purpose::STANDARD.encode(sha1.finalize());
    let mut response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n"
    );
    // Like uWebSockets.js in the JS tracker: echo the requested protocol header.
    if let Some(protocol) = &head.websocket_protocol {
        response.push_str(&format!("Sec-WebSocket-Protocol: {protocol}\r\n"));
    }
    let negotiated = match &head.websocket_extensions {
        Some(offers) if compression => deflate::negotiate(offers),
        _ => None,
    };
    if let Some((_, extension)) = &negotiated {
        response.push_str(&format!("Sec-WebSocket-Extensions: {extension}\r\n"));
    }
    response.push_str("\r\n");
    Ok((response, negotiated.map(|(n, _)| n)))
}

/// A complete response that asks the client to close.
pub fn response(status: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    response_with(status, content_type, &[], body)
}

/// [`response`] with extra header lines (`name`, `value`).
pub fn response_with(
    status: &str,
    content_type: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<u8> {
    let mut head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}
