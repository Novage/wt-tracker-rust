//! permessage-deflate (spec §13.2): negotiation like the JS tracker's uWebSockets shared
//! compressor, compressed client messages (single and fragmented), opt-in outgoing compression,
//! limits and errors, over ws and wss, and across a connection move.

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};

const OFFER: &str = "permessage-deflate; client_max_window_bits"; // Chrome's
const AGREED: &str = "permessage-deflate; client_no_context_takeover; server_no_context_takeover";
const H: &str = "hdeflate000000000001";

/// Compresses like a browser: raw deflate, sync flush, the 4-byte tail removed.
fn compress(data: &[u8]) -> Vec<u8> {
    let mut c = Compress::new(Compression::default(), false);
    let mut out = Vec::with_capacity(data.len() + 64);
    c.compress_vec(data, &mut out, FlushCompress::Sync).unwrap();
    assert!(out.ends_with(&[0, 0, 0xff, 0xff]));
    out.truncate(out.len() - 4);
    out
}

fn decompress(data: &[u8]) -> Vec<u8> {
    let mut d = Decompress::new(false);
    let mut input = data.to_vec();
    input.extend_from_slice(&[0, 0, 0xff, 0xff]);
    let mut out = Vec::with_capacity(1 << 20);
    d.decompress_vec(&input, &mut out, FlushDecompress::Sync)
        .unwrap();
    out
}

/// A masked client frame; `rsv1`: compressed.
fn client_frame(fin: bool, opcode: u8, payload: &[u8], rsv1: bool) -> Vec<u8> {
    let mut f = frame(fin, opcode, payload);
    if rsv1 {
        f[0] |= 0x40;
    }
    f
}

/// A blocking client over any stream: the handshake, then raw frames.
struct Client<S: Read + Write> {
    stream: S,
    /// `Sec-WebSocket-Extensions` of the 101 response.
    extension: Option<String>,
}

impl<S: Read + Write> Client<S> {
    fn open(mut stream: S, offer: Option<&str>) -> Self {
        let offer = offer.map_or(String::new(), |o| {
            format!("Sec-WebSocket-Extensions: {o}\r\n")
        });
        let request = HANDSHAKE.replace("\r\n\r\n", &format!("\r\n{offer}\r\n"));
        stream.write_all(request.as_bytes()).unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        assert!(head.starts_with("HTTP/1.1 101"), "{head}");
        let extension = head
            .lines()
            .find_map(|l| l.strip_prefix("Sec-WebSocket-Extensions: "))
            .map(str::to_string);
        Client { stream, extension }
    }

    fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).unwrap();
    }

    /// One compressed text message (or several frames of it).
    fn send_compressed(&mut self, text: &str, fragments: usize) {
        let compressed = compress(text.as_bytes());
        let size = compressed.len().div_ceil(fragments).max(1);
        let chunks: Vec<&[u8]> = compressed.chunks(size).collect();
        let mut bytes = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            let opcode = if i == 0 { 1 } else { 0 };
            bytes.extend(client_frame(i + 1 == chunks.len(), opcode, chunk, i == 0));
        }
        self.send(&bytes);
    }

    /// `(first header byte, payload)` of the next server frame.
    fn read_frame(&mut self) -> (u8, Vec<u8>) {
        let mut h = [0u8; 2];
        self.stream.read_exact(&mut h).unwrap();
        let len = match h[1] & 0x7f {
            126 => {
                let mut l = [0u8; 2];
                self.stream.read_exact(&mut l).unwrap();
                u16::from_be_bytes(l) as usize
            }
            127 => {
                let mut l = [0u8; 8];
                self.stream.read_exact(&mut l).unwrap();
                u64::from_be_bytes(l) as usize
            }
            n => n as usize,
        };
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).unwrap();
        (h[0], payload)
    }

    /// The next text message and whether it came compressed.
    fn read_text(&mut self) -> (String, bool) {
        loop {
            let (b0, payload) = self.read_frame();
            match b0 & 0x0f {
                1 => {
                    let compressed = b0 & 0x40 != 0;
                    let data = if compressed {
                        decompress(&payload)
                    } else {
                        payload
                    };
                    return (String::from_utf8(data).unwrap(), compressed);
                }
                9 | 10 => {}
                op => panic!("unexpected opcode {op}"),
            }
        }
    }

    /// The close code the server sent.
    fn read_close(&mut self) -> u16 {
        loop {
            let (b0, payload) = self.read_frame();
            if b0 & 0x0f == 8 {
                return u16::from_be_bytes([payload[0], payload[1]]);
            }
        }
    }
}

fn tcp(addr: SocketAddr) -> TcpStream {
    let tcp = TcpStream::connect(addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    tcp
}

fn server(extra: &str) -> wt_server::Server {
    start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}},"websockets":{{{extra}}}}}],"workers":2}}"#
    ))
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn negotiates_like_the_js_tracker_and_inflates_client_messages() {
    let server = server("");
    let addr = server.local_addrs()[0];
    blocking(move || {
        let mut c = Client::open(tcp(addr), Some(OFFER));
        assert_eq!(c.extension.as_deref(), Some(AGREED));
        c.send_compressed(&announce(H, "p1", 0), 1);
        // Replies are not compressed by default (like the JS tracker).
        assert_eq!(c.read_text(), (reply(H, 0, 1), false));
        // Fragmented: RSV1 on the first frame only.
        c.send_compressed(&announce(H, "p1", 0), 3);
        assert_eq!(c.read_text(), (reply(H, 0, 1), false));
        // Uncompressed messages still work.
        c.send(&client_frame(
            true,
            1,
            announce(H, "p1", 0).as_bytes(),
            false,
        ));
        assert_eq!(c.read_text(), (reply(H, 0, 1), false));
        // No offer: no extension.
        let c = Client::open(tcp(addr), None);
        assert_eq!(c.extension, None);
        // An offer we cannot accept: no extension either.
        let c = Client::open(
            tcp(addr),
            Some("permessage-deflate; server_max_window_bits=7"),
        );
        assert_eq!(c.extension, None);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_compression_rsv1_is_a_protocol_error() {
    let server = server(r#""compression":0"#);
    let addr = server.local_addrs()[0];
    blocking(move || {
        let mut c = Client::open(tcp(addr), Some(OFFER));
        assert_eq!(c.extension, None);
        c.send_compressed(&announce(H, "p1", 0), 1);
        assert_eq!(c.read_close(), 1002);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn inflate_limits_and_errors_close_the_connection() {
    let server = server(r#""maxPayloadLength":1000"#);
    let addr = server.local_addrs()[0];
    blocking(move || {
        // Small when compressed, 5000 bytes inflated.
        let mut c = Client::open(tcp(addr), Some(OFFER));
        let big = format!(r#"{{"action":"scrape","x":"{}"}}"#, "a".repeat(5000));
        c.send_compressed(&big, 1);
        assert_eq!(c.read_close(), 1009);

        let mut c = Client::open(tcp(addr), Some(OFFER));
        c.send(&client_frame(true, 1, &[0xff, 0xff, 0xff, 0xff], true));
        assert_eq!(c.read_close(), 1007);

        // Invalid UTF-8 after inflating.
        let mut c = Client::open(tcp(addr), Some(OFFER));
        c.send(&client_frame(true, 1, &compress(&[0xc3, 0x28]), true));
        assert_eq!(c.read_close(), 1007);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_outgoing_messages_are_compressed_when_enabled() {
    let server = server(r#""compressOutgoingMinSize":200"#);
    let addr = server.local_addrs()[0];
    blocking(move || {
        let mut a = Client::open(tcp(addr), Some(OFFER));
        a.send_compressed(&announce(H, "pa", 0), 1);
        // The reply is shorter than 200 bytes: not compressed.
        assert_eq!(a.read_text(), (reply(H, 0, 1), false));
        let mut b = Client::open(tcp(addr), Some(OFFER));
        let sdp = "v=0 ".repeat(100);
        let with_offer = format!(
            r#"{{"action":"announce","info_hash":"{H}","peer_id":"pb","numwant":5,"offers":[{{"offer":{{"type":"offer","sdp":"{sdp}"}},"offer_id":"o1"}}]}}"#
        );
        b.send_compressed(&with_offer, 1);
        assert_eq!(b.read_text(), (reply(H, 0, 2), false));
        let (offer, compressed) = a.read_text();
        assert!(compressed);
        assert_eq!(
            offer,
            format!(
                r#"{{"action":"announce","info_hash":"{H}","offer_id":"o1","peer_id":"pb","offer":{{"type":"offer","sdp":"{sdp}"}}}}"#
            )
        );
        // A client that did not negotiate compression gets it uncompressed.
        let mut c = Client::open(tcp(addr), None);
        c.send(&client_frame(true, 1, announce(H, "pc", 0).as_bytes(), false));
        assert_eq!(c.read_text(), (reply(H, 0, 3), false));
        // Two offers: one each for a and c.
        let two = with_offer.replace(
            r#""offer_id":"o1"}]"#,
            r#""offer_id":"o1"},{"offer":{"type":"offer","sdp":"x"},"offer_id":"o2"}]"#,
        );
        b.send_compressed(&two, 1);
        assert_eq!(b.read_text(), (reply(H, 0, 3), false));
        let (to_c, compressed_c) = c.read_text();
        let (to_a, compressed_a) = a.read_text();
        assert!(!compressed_c && to_c.contains(r#""offer":"#));
        // The short offer ("sdp":"x") may go to a: compressed only if long enough.
        assert_eq!(compressed_a, to_a.len() >= 200);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn compression_over_tls() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-deflate-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert_file, cert.cert.pem()).unwrap();
    std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}},"websockets":{{"compressOutgoingMinSize":1}}}}],"workers":2}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    ));
    let addr = server.local_addrs()[0];
    let der = cert.cert.der().clone();
    blocking(move || {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).unwrap();
        let config = Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
        );
        let session =
            rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
        let mut c = Client::open(rustls::StreamOwned::new(session, tcp(addr)), Some(OFFER));
        assert_eq!(c.extension.as_deref(), Some(AGREED));
        // A 40 KB message, compressed, in 4 fragments.
        let sdp = "a=candidate:1 1 udp 2122260223 192.168.1.2 54321 typ host ".repeat(700);
        let big = format!(
            r#"{{"action":"announce","info_hash":"{H}","peer_id":"p1","numwant":5,"offers":[{{"offer":{{"type":"offer","sdp":"{sdp}"}},"offer_id":"o1"}}]}}"#
        );
        assert!(big.len() > 40_000 && big.len() < 65_536);
        c.send_compressed(&big, 4);
        // compressOutgoingMinSize 1: the reply comes compressed.
        assert_eq!(c.read_text(), (reply(H, 0, 1), true));
    })
    .await;
}

/// A connection that moves to another worker at its first (compressed) announce keeps
/// permessage-deflate.
#[tokio::test(flavor = "multi_thread")]
async fn a_moved_connection_keeps_compression() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0},"websockets":{"compressOutgoingMinSize":1}}],"workers":4}"#,
    );
    // Pinned idle connections: new content avoids their workers, so connections move.
    let mut crowd = Vec::new();
    for _ in 0..8 {
        let mut ws = connect(&server).await;
        send(&mut ws, r#"{"action":"scrape"}"#).await;
        recv(&mut ws).await.unwrap();
        crowd.push(ws);
    }
    let addr = server.local_addrs()[0];
    let clients = blocking(move || {
        let mut clients = Vec::new();
        for i in 0..12u32 {
            let h = format!("hmove{:015}", i % 3);
            let mut c = Client::open(tcp(addr), Some(OFFER));
            // The first announce plus a second message in the same write.
            let mut bytes = client_frame(
                true,
                1,
                &compress(announce(&h, &format!("p{i}"), 0).as_bytes()),
                true,
            );
            bytes.extend(client_frame(
                true,
                1,
                &compress(announce(&h, &format!("p{i}"), 0).as_bytes()),
                true,
            ));
            c.send(&bytes);
            let first = c.read_text();
            let second = c.read_text();
            assert!(first.1 && second.1, "replies compressed");
            assert_eq!(first.0, second.0);
            assert!(first.0.contains(&h));
            clients.push(c);
        }
        clients
    })
    .await;
    let stats = stats(&server).await;
    assert!(
        stats["placement"]["movedConnections"].as_u64().unwrap() > 0,
        "{stats}"
    );
    assert_eq!(stats["placement"]["remoteRequests"], 0, "{stats}");
    drop((clients, crowd));
}

/// Default `compressOutgoingMinSize` (1024): offers are compressed, short replies are not;
/// `/stats.json` `traffic` counts what was sent, received, compressed and inflated.
#[tokio::test(flavor = "multi_thread")]
async fn default_compresses_large_outgoing_messages_and_stats_count_traffic() {
    let server = server("");
    let addr = server.local_addrs()[0];
    let sdp = "a=candidate:1 1 udp 2122260223 192.168.1.2 54321 typ host ".repeat(40);
    blocking(move || {
        let mut a = Client::open(tcp(addr), Some(OFFER));
        a.send_compressed(&announce(H, "pa", 0), 1);
        assert_eq!(a.read_text(), (reply(H, 0, 1), false));
        let mut b = Client::open(tcp(addr), Some(OFFER));
        let with_offer = format!(
            r#"{{"action":"announce","info_hash":"{H}","peer_id":"pb","numwant":5,"offers":[{{"offer":{{"type":"offer","sdp":"{sdp}"}},"offer_id":"o1"}}]}}"#
        );
        b.send_compressed(&with_offer, 1);
        assert_eq!(b.read_text(), (reply(H, 0, 2), false));
        let (offer, compressed) = a.read_text();
        assert!(compressed && offer.len() >= 1024 && offer.contains(&sdp));
        (a, b)
    })
    .await;
    let stats = stats(&server).await;
    let t = &stats["traffic"];
    let n = |v: &serde_json::Value| v.as_u64().unwrap();
    assert_eq!(n(&t["received"]["announces"]["messages"]), 2, "{t}");
    assert_eq!(n(&t["sent"]["announceReplies"]["messages"]), 2, "{t}");
    assert_eq!(n(&t["sent"]["offers"]["messages"]), 1, "{t}");
    assert!(n(&t["sent"]["offers"]["bytes"]) >= 1024, "{t}");
    let deflated = &t["compression"]["deflated"];
    assert_eq!(n(&deflated["messages"]), 1, "{t}");
    assert_eq!(
        n(&deflated["bytesBefore"]),
        n(&t["sent"]["offers"]["bytes"]),
        "{t}"
    );
    assert!(
        n(&deflated["bytesAfter"]) < n(&deflated["bytesBefore"]) / 4,
        "{t}"
    );
    let inflated = &t["compression"]["inflated"];
    assert_eq!(n(&inflated["messages"]), 2, "{t}");
    assert_eq!(
        n(&inflated["bytesAfter"]),
        n(&t["received"]["announces"]["bytes"]),
        "{t}"
    );
    assert!(
        n(&t["socketBytes"]["in"]) > 0 && n(&t["socketBytes"]["out"]) > 0,
        "{t}"
    );
}
