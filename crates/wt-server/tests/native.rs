//! WebSocket transport specifics (shared read buffers, own framing): frames split across many
//! reads, large messages across TLS records, many frames in one read.

mod common;

use std::time::Duration;

use common::*;
use tokio::io::AsyncWriteExt;

const H: &str = "h0000000000000000000";

#[tokio::test(flavor = "multi_thread")]
async fn frame_split_byte_by_byte() {
    let server = plain(2);
    let mut raw = Raw::connect(&server, b"").await;
    raw.tcp.set_nodelay(true).unwrap();
    let bytes = frame(true, 1, announce(H, "p1", 2).as_bytes());
    for b in bytes {
        raw.tcp.write_all(&[b]).await.unwrap();
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    assert_eq!(raw.read_text().await.unwrap(), reply(H, 0, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn many_frames_in_one_write_are_all_handled_in_order() {
    let server = plain(1);
    let mut bytes = Vec::new();
    for i in 0..200 {
        bytes.extend(frame(
            true,
            1,
            announce(&format!("h{i:019}"), "p1", 0).as_bytes(),
        ));
    }
    let mut raw = Raw::connect(&server, &bytes).await;
    for i in 0..200 {
        assert_eq!(
            raw.read_text().await.unwrap(),
            reply(&format!("h{i:019}"), 0, 1),
            "message {i}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn large_message_over_tls_spans_records() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-native-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert_file, cert.cert.pem()).unwrap();
    std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":2}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    ));

    use rustls;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = std::sync::Arc::new(
        rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    );
    let url = format!("wss://localhost:{}/", server.local_addrs()[0].port());
    let connect = || {
        tokio_tungstenite::connect_async_tls_with_config(
            url.clone(),
            None,
            false,
            Some(tokio_tungstenite::Connector::Rustls(client.clone())),
        )
    };
    let (mut a, _) = connect().await.unwrap();
    let (mut b, _) = connect().await.unwrap();
    send(&mut a, &announce(H, "pa", 0)).await;
    assert_eq!(recv(&mut a).await.unwrap(), reply(H, 0, 1));

    // ~60 KB announce: 4 offers of 15 KB each, several TLS records.
    let sdp = "x".repeat(15_000);
    let offers: Vec<String> = (0..4)
        .map(|i| format!(r#"{{"offer":{{"type":"offer","sdp":"{sdp}"}},"offer_id":"o{i}"}}"#))
        .collect();
    let big = format!(
        r#"{{"action":"announce","info_hash":"{H}","peer_id":"pb","numwant":4,"offers":[{}]}}"#,
        offers.join(",")
    );
    assert!(big.len() > 60_000);
    send(&mut b, &big).await;
    assert_eq!(recv(&mut b).await.unwrap(), reply(H, 0, 2));
    // `a` gets one offer with the full 15 KB SDP.
    let offer = recv(&mut a).await.unwrap();
    assert!(
        offer.contains(&format!(r#""sdp":"{sdp}""#)),
        "offer of {} bytes",
        offer.len()
    );
}

/// The client's Finished and its HTTP upgrade request arrive in one TCP write: the request is
/// decrypted during the handshake and must still be read (regression: it was left in the
/// session and the connection timed out).
#[tokio::test(flavor = "multi_thread")]
async fn tls_request_in_the_same_flight_as_finished() {
    use rustls;
    use std::io::{Read, Write};

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-native-flight-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert_file, cert.cert.pem()).unwrap();
    std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":1}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    ));
    let addr = server.local_addrs()[0];
    let der = cert.cert.der().clone();

    let response = tokio::task::spawn_blocking(move || {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut conn = rustls::ClientConnection::new(
            std::sync::Arc::new(config),
            "localhost".try_into().unwrap(),
        )
        .unwrap();
        // Queued now, sent by rustls right after the client Finished.
        conn.writer().write_all(HANDSHAKE.as_bytes()).unwrap();
        let mut tcp = std::net::TcpStream::connect(addr).unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut plain = Vec::new();
        loop {
            // Everything rustls has to send goes out in one write.
            let mut out = Vec::new();
            while conn.wants_write() {
                conn.write_tls(&mut out).unwrap();
            }
            if !out.is_empty() {
                tcp.write_all(&out).unwrap();
            }
            if conn.read_tls(&mut tcp).unwrap() == 0 {
                break;
            }
            conn.process_new_packets().unwrap();
            let mut buf = [0u8; 4096];
            while let Ok(n) = conn.reader().read(&mut buf) {
                if n == 0 {
                    break;
                }
                plain.extend_from_slice(&buf[..n]);
            }
            if plain.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&plain).to_string()
    })
    .await
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 101"), "{response:?}");
}

/// TLS session resumption with stateless tickets: after one full handshake a client resumes
/// (no certificate exchange) on whichever worker accepts it, even after more other clients than
/// a server-side session cache would hold (rustls' default keeps 256; production sees ~120 new
/// connections per second).
#[tokio::test(flavor = "multi_thread")]
async fn tls_reconnects_resume_the_session() {
    use rustls::HandshakeKind;
    use std::io::{Read, Write};
    use std::sync::Arc;

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-native-resume-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert_file, cert.cert.pem()).unwrap();
    std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":2}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    ));
    let addr = server.local_addrs()[0];
    let der = cert.cert.der().clone();

    let (first, others, again) = tokio::task::spawn_blocking(move || {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).unwrap();
        // A client config keeps the tickets it received: one per simulated browser.
        let client = || {
            Arc::new(
                rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots.clone())
                .with_no_client_auth(),
            )
        };
        let connect = |config: &Arc<rustls::ClientConfig>| {
            let conn =
                rustls::ClientConnection::new(config.clone(), "localhost".try_into().unwrap())
                    .unwrap();
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
            tls.conn.handshake_kind().unwrap()
        };
        let a = client();
        let first = connect(&a);
        // More full handshakes of other clients than a 256-entry session cache holds.
        let others: Vec<_> = (0..300).map(|_| connect(&client())).collect();
        let again: Vec<_> = (0..5).map(|_| connect(&a)).collect();
        (first, others, again)
    })
    .await
    .unwrap();
    assert_eq!(first, HandshakeKind::Full);
    assert!(others.iter().all(|k| *k == HandshakeKind::Full));
    assert!(
        again.iter().all(|k| *k == HandshakeKind::Resumed),
        "{again:?}"
    );
}
