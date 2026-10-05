#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wt_server::{Config, Server};

pub const WAIT: Duration = Duration::from_secs(5);

/// Starts a server from a JSON config; listeners on 127.0.0.1 with port 0 unless given.
/// Adds a `/metrics` listener on 127.0.0.1 (any port) unless the config has `metrics`.
pub fn start(config: &str) -> Server {
    let mut value: serde_json::Value = serde_json::from_str(config).expect("config JSON");
    let object = value.as_object_mut().expect("config object");
    if !object.contains_key("metrics") {
        object.insert(
            "metrics".into(),
            serde_json::json!({ "host": "127.0.0.1", "port": 0 }),
        );
    }
    let config = Config::from_json(&value.to_string()).expect("config");
    wt_server::start(config).expect("start")
}

pub fn plain(workers: usize) -> Server {
    start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}}],"workers":{workers}}}"#
    ))
}

/// `hash` placement: swarms spread over shards by info_hash (for cross-shard paths).
pub fn hashed(workers: usize) -> Server {
    start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}}],"workers":{workers},"placement":"hash"}}"#
    ))
}

pub fn url(server: &Server) -> String {
    format!("ws://{}/", server.local_addrs()[0])
}

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(server: &Server) -> Ws {
    tokio_tungstenite::connect_async(url(server))
        .await
        .expect("connect")
        .0
}

pub async fn connect_with_origin(server: &Server, origin: Option<&str>) -> Result<Ws, String> {
    let mut request = url(server).into_client_request().unwrap();
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("Origin", origin.parse().unwrap());
    }
    tokio_tungstenite::connect_async(request)
        .await
        .map(|(ws, _)| ws)
        .map_err(|e| e.to_string())
}

pub async fn send(ws: &mut Ws, text: &str) {
    ws.send(Message::text(text)).await.expect("send");
}

/// Next text message, or `None` if the connection closed / nothing arrives within `WAIT`.
pub async fn recv(ws: &mut Ws) -> Option<String> {
    loop {
        match tokio::time::timeout(WAIT, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return Some(t.to_string()),
            Ok(Some(Ok(Message::Binary(b)))) => {
                return Some(String::from_utf8(b.to_vec()).unwrap());
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
            Ok(Some(Ok(_))) => {}
        }
    }
}

/// Collects `n` text messages.
pub async fn recv_n(ws: &mut Ws, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(
            recv(ws)
                .await
                .unwrap_or_else(|| panic!("expected {n} messages, got {out:?}")),
        );
    }
    out
}

/// True if the server closes the connection within `WAIT` (reading and discarding messages).
pub async fn closed(ws: &mut Ws) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Err(_) => return false,
            Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => return true,
            Ok(Some(Ok(_))) => {}
        }
    }
}

pub fn announce(info_hash: &str, peer_id: &str, offers: usize) -> String {
    let offers: Vec<String> = (0..offers)
        .map(|i| format!(r#"{{"offer":{{"type":"offer","sdp":"sdp-{peer_id}-{i}"}},"offer_id":"{peer_id}-o{i}"}}"#))
        .collect();
    format!(
        r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"{peer_id}","numwant":10,"offers":[{}]}}"#,
        offers.join(",")
    )
}

pub fn reply(info_hash: &str, complete: u32, incomplete: u32) -> String {
    format!(
        r#"{{"action":"announce","interval":20,"info_hash":"{info_hash}","complete":{complete},"incomplete":{incomplete}}}"#
    )
}

pub fn offer(info_hash: &str, from: &str, i: usize) -> String {
    format!(
        r#"{{"action":"announce","info_hash":"{info_hash}","offer_id":"{from}-o{i}","peer_id":"{from}","offer":{{"type":"offer","sdp":"sdp-{from}-{i}"}}}}"#
    )
}

/// `/stats.json` (or any GET path) over plain HTTP.
pub async fn http_get(addr: SocketAddr, path: &str) -> (String, String) {
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    tcp.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    tcp.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    (head.lines().next().unwrap().to_string(), body.to_string())
}

pub async fn stats(server: &Server) -> serde_json::Value {
    let (status, body) = http_get(server.local_addrs()[0], "/stats.json").await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    serde_json::from_str(&body).unwrap()
}

/// One swarm from `/stats.json?infoHash=`: `{"infoHash", "peers", "workers": [{"worker", "peers"}]}`.
pub async fn swarm(server: &Server, info_hash: &str) -> serde_json::Value {
    let hex: String = info_hash.bytes().map(|b| format!("{b:02x}")).collect();
    let (status, body) = http_get(
        server.local_addrs()[0],
        &format!("/stats.json?infoHash={hex}"),
    )
    .await;
    assert_eq!(status, "HTTP/1.1 200 OK", "{body}");
    serde_json::from_str(&body).unwrap()
}

/// Workers that hold a swarm for `info_hash`.
pub async fn swarm_workers(server: &Server, info_hash: &str) -> Vec<usize> {
    swarm(server, info_hash).await["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["worker"].as_u64().unwrap() as usize)
        .collect()
}

/// One `/metrics` sample: name, labels, value.
pub type Sample = (String, Vec<(String, String)>, f64);

/// Parsed `/metrics` samples.
pub struct Metrics(pub Vec<Sample>);

impl Metrics {
    /// Sum of the samples of `name` whose labels include every `filter` (e.g. all workers).
    pub fn sum(&self, name: &str, filters: &[(&str, &str)]) -> u64 {
        let total: f64 = self
            .0
            .iter()
            .filter(|(n, labels, _)| {
                n == name
                    && filters
                        .iter()
                        .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|(_, _, value)| value)
            .sum();
        total as u64
    }

    pub fn has(&self, name: &str) -> bool {
        self.0.iter().any(|(n, _, _)| n == name)
    }
}

/// GET `/metrics` from the server's metrics listener.
pub async fn metrics(server: &Server) -> Metrics {
    let addr = server.metrics_addr().expect("metrics listener");
    let (status, body) = http_get(addr, "/metrics").await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    Metrics(body.lines().filter_map(parse_sample).collect())
}

/// `name{k="v",...} value` (labels without `"` or `,` in tests).
fn parse_sample(line: &str) -> Option<Sample> {
    if line.starts_with('#') || line.is_empty() {
        return None;
    }
    let (series, value) = line.rsplit_once(' ')?;
    let (name, labels) = match series.split_once('{') {
        Some((name, rest)) => {
            let labels = rest
                .trim_end_matches('}')
                .split(',')
                .filter_map(|pair| {
                    let (k, v) = pair.split_once('=')?;
                    Some((k.to_string(), v.trim_matches('"').to_string()))
                })
                .collect();
            (name, labels)
        }
        None => (series, Vec::new()),
    };
    Some((name.to_string(), labels, value.parse().ok()?))
}

/// Polls stats until `peersCount == n`.
pub async fn wait_peers(server: &Server, n: u64) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let peers = stats(server).await["peersCount"].as_u64().unwrap();
        if peers == n {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "peersCount {peers}, expected {n}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A raw client: handshake by hand, then masked frames written byte by byte as the test wants.
pub struct Raw {
    pub tcp: TcpStream,
}

pub const HANDSHAKE: &str = "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

/// A masked client frame.
pub fn frame(fin: bool, opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mask = [1u8, 2, 3, 4];
    let mut out = vec![(fin as u8) << 7 | opcode];
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
    out.extend_from_slice(&mask);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    out
}

impl Raw {
    /// Connects and sends the handshake plus `then` in the same write.
    pub async fn connect(server: &Server, then: &[u8]) -> Raw {
        let mut tcp = TcpStream::connect(server.local_addrs()[0]).await.unwrap();
        let mut bytes = HANDSHAKE.as_bytes().to_vec();
        bytes.extend_from_slice(then);
        tcp.write_all(&bytes).await.unwrap();
        let mut raw = Raw { tcp };
        let head = raw.read_until(b"\r\n\r\n").await;
        assert!(
            head.starts_with(b"HTTP/1.1 101"),
            "{}",
            String::from_utf8_lossy(&head)
        );
        raw
    }

    async fn read_until(&mut self, end: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(end) {
            self.tcp.read_exact(&mut byte).await.unwrap();
            buf.push(byte[0]);
        }
        buf
    }

    /// Next server frame `(opcode, payload)`, or `None` on close / timeout.
    pub async fn read_frame(&mut self) -> Option<(u8, Vec<u8>)> {
        let fut = async {
            let mut h = [0u8; 2];
            self.tcp.read_exact(&mut h).await.ok()?;
            let len = match h[1] & 0x7f {
                126 => {
                    let mut l = [0u8; 2];
                    self.tcp.read_exact(&mut l).await.ok()?;
                    u16::from_be_bytes(l) as usize
                }
                127 => {
                    let mut l = [0u8; 8];
                    self.tcp.read_exact(&mut l).await.ok()?;
                    u64::from_be_bytes(l) as usize
                }
                n => n as usize,
            };
            let mut payload = vec![0u8; len];
            self.tcp.read_exact(&mut payload).await.ok()?;
            Some((h[0] & 0x0f, payload))
        };
        tokio::time::timeout(WAIT, fut).await.ok().flatten()
    }

    /// Next text frame, skipping control frames.
    pub async fn read_text(&mut self) -> Option<String> {
        loop {
            match self.read_frame().await? {
                (1, p) => return Some(String::from_utf8(p).unwrap()),
                (8, _) => return None,
                _ => {}
            }
        }
    }
}

/// A blocking TLS client (rustls over std TCP): WebSocket upgrade done.
pub type Tls = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

/// A client config trusting `roots`; it keeps the session tickets it receives.
pub fn tls_client(roots: &rustls::RootCertStore) -> std::sync::Arc<rustls::ClientConfig> {
    std::sync::Arc::new(
        rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots.clone())
        .with_no_client_auth(),
    )
}

/// Connects with `config`; returns the stream, the leaf certificate the server sent (the
/// original one on a resumed session) and the handshake kind.
pub fn tls_connect(
    addr: std::net::SocketAddr,
    config: &std::sync::Arc<rustls::ClientConfig>,
) -> (Tls, Vec<u8>, rustls::HandshakeKind) {
    use std::io::{Read, Write};
    let conn =
        rustls::ClientConnection::new(config.clone(), "localhost".try_into().unwrap()).unwrap();
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut tls = rustls::StreamOwned::new(conn, tcp);
    tls.write_all(HANDSHAKE.as_bytes()).unwrap();
    // Reading the 101 response also processes the ticket sent after the handshake.
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        tls.read_exact(&mut byte).unwrap();
        head.push(byte[0]);
    }
    assert!(head.starts_with(b"HTTP/1.1 101"));
    let leaf = tls.conn.peer_certificates().unwrap()[0].to_vec();
    let kind = tls.conn.handshake_kind().unwrap();
    (tls, leaf, kind)
}
