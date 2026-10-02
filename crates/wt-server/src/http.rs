//! Minimal HTTP/1.1 for the tracker: read one request head, then either upgrade to WebSocket or
//! answer `/`, `/stats.json` or 404 and close.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::config::AccessConfig;

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
    pub origin: Option<String>,
}

/// Reads a request head. Returns it and any bytes received after it.
pub async fn read_head<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<(Head, Bytes)> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        if stream.read_buf(&mut buf).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut headers);
        match request.parse(&buf) {
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
                    } else if h.name.eq_ignore_ascii_case("origin") {
                        head.origin = Some(value());
                    }
                }
                let rest = buf.split_off(len).freeze();
                return Ok((head, rest));
            }
            Ok(httparse::Status::Partial) if buf.len() < MAX_HEAD => {}
            Ok(httparse::Status::Partial) => {
                return Err(io::Error::other("request head too large"));
            }
            Err(e) => return Err(io::Error::other(e)),
        }
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

/// Writes the `101 Switching Protocols` response for a valid upgrade request.
pub async fn accept_upgrade<S: AsyncWrite + Unpin>(stream: &mut S, head: &Head) -> io::Result<()> {
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
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

/// Writes a complete response and asks the client to close.
pub async fn respond<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    stream.shutdown().await
}

pub async fn not_found<S: AsyncWrite + Unpin>(stream: &mut S) -> io::Result<()> {
    respond(stream, "404 Not Found", None, b"404 Not Found").await
}

/// A stream that first yields bytes already read (sent right after the HTTP head).
pub struct Prefixed<S> {
    prefix: Bytes,
    inner: S,
}

impl<S> Prefixed<S> {
    pub fn new(prefix: Bytes, inner: S) -> Self {
        Self { prefix, inner }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.prefix.is_empty() {
            let n = self.prefix.len().min(buf.remaining());
            buf.put_slice(&self.prefix[..n]);
            self.prefix.advance(n);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
