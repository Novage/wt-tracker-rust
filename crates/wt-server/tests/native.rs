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
        let client = || tls_client(&roots);
        let connect = |config: &_| tls_connect(addr, config).2;
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

/// Announces on an open TLS WebSocket and reads the reply.
fn tls_announce(tls: &mut Tls, peer_id: &str) -> String {
    use std::io::{Read, Write};
    tls.write_all(&frame(true, 1, announce(H, peer_id, 0).as_bytes()))
        .unwrap();
    let mut h = [0u8; 2];
    tls.read_exact(&mut h).unwrap();
    let mut payload = vec![0u8; (h[1] & 0x7f) as usize];
    tls.read_exact(&mut payload).unwrap();
    String::from_utf8(payload).unwrap()
}

/// The certificate is swapped while the server runs (spec §13.2) by `Server::reload_tls`
/// (SIGHUP): open connections stay, tickets of the old certificate still
/// resume, a key that does not match is rejected and the old certificate kept.
#[tokio::test(flavor = "multi_thread")]
async fn tls_certificate_reloads_without_a_restart() {
    use std::sync::Arc;

    let certs: Vec<_> = (0..3)
        .map(|_| rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap())
        .collect();
    let dir = std::env::temp_dir().join(format!("wt-native-reload-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    let pems: Vec<(String, String)> = certs
        .iter()
        .map(|c| (c.cert.pem(), c.signing_key.serialize_pem()))
        .collect();
    let write = {
        let (cert_file, key_file) = (cert_file.clone(), key_file.clone());
        move |cert: usize, key: usize| {
            std::fs::write(&cert_file, &pems[cert].0).unwrap();
            std::fs::write(&key_file, &pems[key].1).unwrap();
        }
    };
    write(0, 0);
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":2}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    ));
    let addr = server.local_addrs()[0];
    let der: Vec<Vec<u8>> = certs.iter().map(|c| c.cert.der().to_vec()).collect();
    let mut roots = rustls::RootCertStore::empty();
    for c in &certs {
        roots.add(c.cert.der().clone()).unwrap();
    }
    let client = move || tls_client(&roots);
    let server = Arc::new(server);
    let reload = {
        let server = server.clone();
        move || server.reload_tls().remove(0).1
    };
    let open = tokio::task::spawn_blocking(move || {
        use wt_server::Reload;
        let served = || tls_connect(addr, &client()).1;

        // Certificate 0; this client keeps a ticket and a connection.
        let returning = client();
        let (mut open, leaf, _) = tls_connect(addr, &returning);
        assert_eq!(leaf, der[0]);
        assert!(tls_announce(&mut open, "p-open").contains("interval"));

        // New files are served only after a reload (SIGHUP).
        write(1, 1);
        assert_eq!(served(), der[0]);
        assert!(matches!(reload(), Reload::Reloaded { .. }));
        assert_eq!(served(), der[1]);
        // The connection from before the reload still works, and the old ticket resumes.
        assert!(tls_announce(&mut open, "p-open").contains("interval"));
        assert_eq!(
            tls_connect(addr, &returning).2,
            rustls::HandshakeKind::Resumed
        );

        // Certificate 2 with the key of 1: rejected (on every reload), certificate 1 stays.
        write(2, 1);
        assert!(matches!(reload(), Reload::Failed(_)));
        assert!(matches!(reload(), Reload::Failed(_)));
        assert_eq!(served(), der[1]);

        // Fixed: the next reload serves certificate 2; the same files again are unchanged.
        write(2, 2);
        assert!(matches!(reload(), Reload::Reloaded { .. }));
        assert_eq!(served(), der[2]);
        assert_eq!(reload(), Reload::Unchanged);
        open
    })
    .await
    .unwrap();

    let m = metrics(&server).await;
    assert_eq!(m.sum("wt_tls_reloads_total", &[("result", "ok")]), 2);
    assert_eq!(m.sum("wt_tls_reloads_total", &[("result", "error")]), 2);
    assert!(m.sum("wt_tls_certificate_expiry_seconds", &[]) > 1_700_000_000);
    drop(open);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Production stall (spec §13.2): a client's first TLS record was larger than the 8 KiB request
/// head limit and arrived in parts. With more than 8 KiB of the incomplete record pending, the
/// head reader stopped reading the socket, got no plaintext, waited for readability (still
/// ready: the rest was unread) and looped without ever yielding: the worker spun at 100% CPU,
/// its timeouts and every other connection stalled. The head (> 8 KiB) must be rejected and the
/// worker keep serving.
#[tokio::test(flavor = "multi_thread")]
async fn tls_record_larger_than_the_head_limit_does_not_stall_the_worker() {
    use std::io::{Read, Write};

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-native-bighead-{}", std::process::id()));
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
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let config = tls_client(&roots);

    let closed = tokio::task::spawn_blocking(move || {
        let mut conn =
            rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
        let mut tcp = std::net::TcpStream::connect(addr).unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        while conn.is_handshaking() {
            conn.complete_io(&mut tcp).unwrap();
        }
        // One ~12 KiB record: a request head over the 8 KiB limit.
        let head = format!(
            "GET / HTTP/1.1\r\nHost: x\r\nX-Pad: {}\r\n\r\n",
            "a".repeat(12_000)
        );
        conn.writer().write_all(head.as_bytes()).unwrap();
        let mut record = Vec::new();
        while conn.wants_write() {
            conn.write_tls(&mut record).unwrap();
        }
        assert!(record.len() > 9_000);
        // More than 8 KiB of it first, the rest a moment later.
        tcp.set_nodelay(true).unwrap();
        tcp.write_all(&record[..9_000]).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        tcp.write_all(&record[9_000..]).unwrap();
        // The server rejects the head and closes; a stalled worker never answers.
        let mut buf = [0u8; 1024];
        loop {
            match tcp.read(&mut buf) {
                Ok(0) => return true,
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return true,
                Err(_) => return false,
            }
        }
    })
    .await
    .unwrap();
    if !closed {
        // The stalled worker would never join: leak the server instead of hanging the test.
        std::mem::forget(server);
        panic!("the connection was not closed: the worker stalled");
    }
    // The worker still serves others.
    assert_eq!(metrics(&server).await.sum("wt_worker_up", &[]), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}
