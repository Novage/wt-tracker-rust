//! A minimal WebSocket client (RFC 6455 + permessage-deflate without context takeover) for
//! `--deflate` load runs and the smoke check's compression step: tungstenite rejects frames with
//! RSV1 set. Counts the bytes on the wire.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::{self, pki_types::ServerName};

/// Chrome's offer.
pub const DEFLATE_OFFER: &str = "permessage-deflate; client_max_window_bits";

/// Bytes read and written on the wire (TLS included), all clients.
#[derive(Default)]
pub struct Wire {
    pub read: AtomicU64,
    pub written: AtomicU64,
}

enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

pub enum Incoming {
    Text(String),
    /// The server's close code, if any.
    Close(Option<u16>),
}

pub struct Client {
    stream: Stream,
    buf: Vec<u8>,
    /// permessage-deflate negotiated: every message we send is compressed.
    deflate: bool,
    /// `Sec-WebSocket-Extensions` of the server's response.
    pub extension: Option<String>,
    wire: Arc<Wire>,
    /// A ping to answer with the next send (sending inside `next` would not be cancel safe).
    pong: Option<Vec<u8>>,
    fragments: Option<(bool, Vec<u8>)>,
    mask: u32,
}

thread_local! {
    static DEFLATER: RefCell<Compress> = RefCell::new(Compress::new(Compression::default(), false));
    static INFLATER: RefCell<Decompress> = RefCell::new(Decompress::new(false));
}

fn compress(data: &[u8]) -> Vec<u8> {
    DEFLATER.with_borrow_mut(|c| {
        c.reset();
        let mut out = Vec::with_capacity(data.len() / 2 + 64);
        let mut pos = 0;
        loop {
            if out.len() == out.capacity() {
                out.reserve(out.capacity().max(64));
            }
            let before = c.total_in();
            c.compress_vec(&data[pos..], &mut out, FlushCompress::Sync)
                .expect("deflate");
            pos += (c.total_in() - before) as usize;
            if pos == data.len() && out.len() < out.capacity() {
                break;
            }
        }
        out.truncate(out.len() - 4);
        out
    })
}

fn inflate(data: &[u8]) -> Result<Vec<u8>, String> {
    INFLATER.with_borrow_mut(|d| {
        d.reset(false);
        let mut input = data.to_vec();
        input.extend_from_slice(&[0, 0, 0xff, 0xff]);
        let mut out = Vec::with_capacity(data.len() * 4 + 64);
        let mut pos = 0;
        loop {
            if out.len() == out.capacity() {
                out.reserve(out.capacity());
            }
            let before = d.total_in();
            let status = d
                .decompress_vec(&input[pos..], &mut out, FlushDecompress::Sync)
                .map_err(|e| e.to_string())?;
            pos += (d.total_in() - before) as usize;
            if status == flate2::Status::StreamEnd
                || (pos == input.len() && out.len() < out.capacity())
            {
                return Ok(out);
            }
        }
    })
}

/// Connects to `ws://` or `wss://` (with `tls`), offering permessage-deflate if `deflate`.
pub async fn connect(
    url: &str,
    tls: Option<Arc<rustls::ClientConfig>>,
    deflate: bool,
    wire: Arc<Wire>,
) -> Result<Client, String> {
    let (secure, rest) = match (url.strip_prefix("wss://"), url.strip_prefix("ws://")) {
        (Some(rest), _) => (true, rest),
        (_, Some(rest)) => (false, rest),
        _ => return Err(format!("bad url {url}")),
    };
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest, "/".to_string()), |(a, p)| (a, format!("/{p}")));
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(h, _)| h)
        .to_string();
    let tcp = TcpStream::connect(authority)
        .await
        .map_err(|e| e.to_string())?;
    let _ = tcp.set_nodelay(true);
    let stream = if secure {
        let config = tls.ok_or("wss:// needs --ca")?;
        let name = ServerName::try_from(host.clone()).map_err(|e| e.to_string())?;
        let tls = tokio_rustls::TlsConnector::from(config)
            .connect(name, tcp)
            .await
            .map_err(|e| e.to_string())?;
        Stream::Tls(Box::new(tls))
    } else {
        Stream::Plain(tcp)
    };
    let mut client = Client {
        stream,
        buf: Vec::with_capacity(4096),
        deflate: false,
        extension: None,
        wire,
        pong: None,
        fragments: None,
        mask: 0x9e37_79b9,
    };
    let offer = if deflate {
        format!("Sec-WebSocket-Extensions: {DEFLATE_OFFER}\r\n")
    } else {
        String::new()
    };
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{offer}\r\n"
    );
    client
        .write(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let end = loop {
        if let Some(end) = client.buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
        if client.read().await.map_err(|e| e.to_string())? == 0 {
            return Err("closed during the handshake".into());
        }
    };
    let head = String::from_utf8_lossy(&client.buf[..end]).to_string();
    client.buf.drain(..end);
    if !head.starts_with("HTTP/1.1 101") {
        return Err(format!("handshake: {}", head.lines().next().unwrap_or("")));
    }
    client.extension = head.lines().find_map(|l| {
        let (name, value) = l.split_once(':')?;
        name.eq_ignore_ascii_case("sec-websocket-extensions")
            .then(|| value.trim().to_string())
    });
    client.deflate = client
        .extension
        .as_deref()
        .is_some_and(|e| e.starts_with("permessage-deflate"));
    Ok(client)
}

impl Client {
    pub fn deflate(&self) -> bool {
        self.deflate
    }

    async fn read(&mut self) -> std::io::Result<usize> {
        let n = match &mut self.stream {
            Stream::Plain(s) => s.read_buf(&mut self.buf).await?,
            Stream::Tls(s) => s.read_buf(&mut self.buf).await?,
        };
        self.wire.read.fetch_add(n as u64, Relaxed);
        Ok(n)
    }

    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match &mut self.stream {
            Stream::Plain(s) => s.write_all(bytes).await?,
            Stream::Tls(s) => {
                s.write_all(bytes).await?;
                s.flush().await?;
            }
        }
        self.wire.written.fetch_add(bytes.len() as u64, Relaxed);
        Ok(())
    }

    fn frame(&mut self, out: &mut Vec<u8>, opcode: u8, rsv1: bool, payload: &[u8]) {
        out.push(0x80 | if rsv1 { 0x40 } else { 0 } | opcode);
        match payload.len() {
            n if n < 126 => out.push(0x80 | n as u8),
            n if n < 65536 => {
                out.push(0x80 | 126);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            }
            n => {
                out.push(0x80 | 127);
                out.extend_from_slice(&(n as u64).to_be_bytes());
            }
        }
        // xorshift: masks only need to vary for this load generator.
        self.mask ^= self.mask << 13;
        self.mask ^= self.mask >> 17;
        self.mask ^= self.mask << 5;
        let mask = self.mask.to_ne_bytes();
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    }

    /// Sends a text message (compressed if negotiated), after a pending pong.
    pub async fn send_text(&mut self, text: &str) -> std::io::Result<()> {
        let mut out = Vec::with_capacity(text.len() + 32);
        if let Some(ping) = self.pong.take() {
            self.frame(&mut out, 0xA, false, &ping);
        }
        if self.deflate {
            let compressed = compress(text.as_bytes());
            self.frame(&mut out, 0x1, true, &compressed);
        } else {
            self.frame(&mut out, 0x1, false, text.as_bytes());
        }
        self.write(&out).await
    }

    pub async fn close(&mut self) {
        let mut out = Vec::new();
        self.frame(&mut out, 0x8, false, &1000u16.to_be_bytes());
        let _ = self.write(&out).await;
    }

    /// The next message; `Ok(None)` at end of stream. Cancel safe.
    pub async fn next(&mut self) -> Result<Option<Incoming>, String> {
        loop {
            if let Some(message) = self.parse()? {
                return Ok(Some(message));
            }
            if self.read().await.map_err(|e| e.to_string())? == 0 {
                return Ok(None);
            }
        }
    }

    fn parse(&mut self) -> Result<Option<Incoming>, String> {
        loop {
            let b = &self.buf;
            if b.len() < 2 {
                return Ok(None);
            }
            let (len, header) = match b[1] & 0x7f {
                126 if b.len() >= 4 => (u16::from_be_bytes([b[2], b[3]]) as usize, 4),
                127 if b.len() >= 10 => (
                    u64::from_be_bytes(b[2..10].try_into().unwrap()) as usize,
                    10,
                ),
                126 | 127 => return Ok(None),
                n => (n as usize, 2),
            };
            if b.len() < header + len {
                return Ok(None);
            }
            let (b0, payload) = (b[0], b[header..header + len].to_vec());
            self.buf.drain(..header + len);
            let (fin, rsv1, opcode) = (b0 & 0x80 != 0, b0 & 0x40 != 0, b0 & 0x0f);
            let message = match opcode {
                0x1 | 0x2 if fin => Some((rsv1, payload)),
                0x1 | 0x2 => {
                    self.fragments = Some((rsv1, payload));
                    None
                }
                0x0 => {
                    let (compressed, mut data) =
                        self.fragments.take().ok_or("stray continuation")?;
                    data.extend_from_slice(&payload);
                    if fin {
                        Some((compressed, data))
                    } else {
                        self.fragments = Some((compressed, data));
                        None
                    }
                }
                0x8 => {
                    let code =
                        (payload.len() >= 2).then(|| u16::from_be_bytes([payload[0], payload[1]]));
                    return Ok(Some(Incoming::Close(code)));
                }
                0x9 => {
                    self.pong = Some(payload);
                    None
                }
                _ => None,
            };
            if let Some((compressed, data)) = message {
                let data = if compressed { inflate(&data)? } else { data };
                return String::from_utf8(data)
                    .map(|t| Some(Incoming::Text(t)))
                    .map_err(|e| e.to_string());
            }
        }
    }
}
