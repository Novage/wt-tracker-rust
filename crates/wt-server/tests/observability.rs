//! Observability (spec §13.7): connections counted by why they ended, messages by why they were
//! rejected, and the close code of a rejected message.

mod common;

use std::time::Duration;

use common::*;
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

/// Polls `/metrics` until `name{filters}` (summed over workers) reaches `want`.
async fn wait_metric(server: &wt_server::Server, name: &str, filters: &[(&str, &str)], want: u64) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let got = metrics(server).await.sum(name, filters);
        if got == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{name}{filters:?} = {got}, expected {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The close code the server sends next (skipping other messages).
async fn close_code(ws: &mut Ws) -> Option<CloseCode> {
    loop {
        match tokio::time::timeout(WAIT, ws.next()).await.ok()?? {
            Ok(Message::Close(frame)) => return frame.map(|f| f.code),
            Ok(_) => {}
            Err(_) => return None,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_are_counted_by_why_they_ended() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0},"websockets":{"idleTimeout":1,"maxPayloadLength":1024}}],"workers":1}"#,
    );
    let closed = |reason: &'static str| ("reason", reason);

    // The client closes.
    let mut ws = connect(&server).await;
    ws.send(Message::Close(None)).await.unwrap();
    assert!(common::closed(&mut ws).await);
    wait_metric(
        &server,
        "wt_closed_connections_total",
        &[closed("client_close")],
        1,
    )
    .await;

    // Not JSON: rejected (1008).
    let mut raw = Raw::connect(&server, &frame(true, 1, b"not json")).await;
    assert_eq!(raw.read_frame().await.map(|f| f.0), Some(8));
    wait_metric(
        &server,
        "wt_closed_connections_total",
        &[closed("rejected")],
        1,
    )
    .await;
    wait_metric(
        &server,
        "wt_rejected_messages_total",
        &[("reason", "invalid_json")],
        1,
    )
    .await;

    // Larger than maxPayloadLength (1009).
    let mut raw = Raw::connect(&server, &frame(true, 1, &[b'x'; 2000])).await;
    assert_eq!(raw.read_frame().await.map(|f| f.0), Some(8));
    wait_metric(
        &server,
        "wt_closed_connections_total",
        &[closed("too_big")],
        1,
    )
    .await;

    // The TCP connection just ends.
    drop(Raw::connect(&server, &[]).await);
    wait_metric(&server, "wt_closed_connections_total", &[closed("eof")], 1).await;

    // Not an HTTP request.
    let mut tcp = TcpStream::connect(server.local_addrs()[0]).await.unwrap();
    tcp.write_all(b"\x16\x03\x01 not http\r\n\r\n")
        .await
        .unwrap();
    wait_metric(
        &server,
        "wt_closed_connections_total",
        &[closed("bad_request")],
        1,
    )
    .await;

    // Silent for longer than idleTimeout (1 s); pings are not answered by a raw client.
    let _idle = Raw::connect(&server, &[]).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    wait_metric(
        &server,
        "wt_closed_connections_total",
        &[closed("idle_timeout")],
        1,
    )
    .await;

    let m = metrics(&server).await;
    assert_eq!(m.sum("wt_worker_up", &[]), 1);
    assert!(m.has("wt_build_info") && m.has("process_start_time_seconds"));
}

/// Spec §5.3: answers that may not be delivered (here: to an unknown peer, spread over 4 shards
/// by hash) are dropped and counted; the connections stay open. A malformed answer (no
/// `info_hash`) closes its connection with 1008.
#[tokio::test(flavor = "multi_thread")]
async fn undeliverable_answers_are_dropped_and_malformed_ones_rejected() {
    let server = hashed(4);
    let mut clients = Vec::new();
    for i in 0..12 {
        let info_hash = format!("hdrop{i:015}");
        let mut ws = connect(&server).await;
        send(&mut ws, &announce(&info_hash, &format!("p{i}"), 0)).await;
        recv(&mut ws).await.unwrap();
        send(
            &mut ws,
            &format!(
                r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"p{i}","to_peer_id":"ghost","answer":{{"type":"answer","sdp":"y"}},"offer_id":"o"}}"#
            ),
        )
        .await;
        clients.push(ws);
    }
    wait_metric(&server, "wt_dropped_answers_total", &[], 12).await;
    // Still connected: a scrape is answered.
    for ws in &mut clients {
        send(ws, r#"{"action":"scrape"}"#).await;
        assert!(recv(ws).await.unwrap().starts_with(r#"{"action":"scrape""#));
    }
    let m = metrics(&server).await;
    assert_eq!(m.sum("wt_rejected_messages_total", &[]), 0);
    assert!(
        m.sum("wt_routed_requests_total", &[("target", "remote")]) > 0,
        "some answers were dropped by another worker's shard"
    );

    let mut malformed = connect(&server).await;
    send(
        &mut malformed,
        r#"{"action":"announce","peer_id":"p0","to_peer_id":"p1","answer":{}}"#,
    )
    .await;
    assert_eq!(close_code(&mut malformed).await, Some(CloseCode::Policy));
    wait_metric(
        &server,
        "wt_rejected_messages_total",
        &[("reason", "bad_field")],
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_metrics_listener_unless_configured() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":1,"metrics":null}"#,
    );
    assert_eq!(server.metrics_addr(), None);
}

/// `/swarms` on the metrics listener: the largest swarms of all shards, most peers first; the
/// hex works with `/stats.json?infoHash=`.
#[tokio::test(flavor = "multi_thread")]
async fn swarms_lists_the_largest_swarms_of_all_shards() {
    let server = hashed(4);
    // Swarm i has i + 1 peers.
    let mut keep = Vec::new();
    for i in 0..6 {
        let info_hash = format!("hswarms{i:013}");
        for p in 0..=i {
            let mut ws = connect(&server).await;
            send(&mut ws, &announce(&info_hash, &format!("s{i}p{p}"), 0)).await;
            recv(&mut ws).await.unwrap();
            keep.push(ws);
        }
    }
    let metrics_addr = server.metrics_addr().unwrap();
    let get = |path: &'static str| async move {
        let (status, body) = http_get(metrics_addr, path).await;
        assert_eq!(status, "HTTP/1.1 200 OK", "{body}");
        serde_json::from_str::<serde_json::Value>(&body).unwrap()
    };

    let top = get("/swarms?top=3").await;
    assert_eq!(top["total"], 6, "{top}");
    let swarms = top["swarms"].as_array().unwrap();
    let peers: Vec<u64> = swarms
        .iter()
        .map(|s| s["peers"].as_u64().unwrap())
        .collect();
    assert_eq!(peers, [6, 5, 4], "{top}");
    let hex: String = "hswarms0000000000005"
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(swarms[0]["infoHash"], hex.as_str());
    let one = swarm(&server, "hswarms0000000000005").await;
    assert_eq!(one["peers"], 6);
    assert_eq!(one["workers"][0]["worker"], swarms[0]["worker"]);

    assert_eq!(get("/swarms").await["swarms"].as_array().unwrap().len(), 6);
    assert_eq!(
        get("/swarms?top=0").await["swarms"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(
        http_get(metrics_addr, "/swarms?top=x").await.0,
        "HTTP/1.1 400 Bad Request"
    );
    // Not on the public listener.
    assert_eq!(
        http_get(server.local_addrs()[0], "/swarms").await.0,
        "HTTP/1.1 404 Not Found"
    );
    drop(keep);
}

/// Spec §13.7: basic auth on every route of the metrics listener (`/metrics`, `/swarms`,
/// `/stats.json`); 401 with a `WWW-Authenticate` challenge otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn metrics_listener_basic_auth() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":2,
            "metrics":{"host":"127.0.0.1","port":0,"username":"grafana","password":"s3cret"}}"#,
    );
    let addr = server.metrics_addr().unwrap();
    for path in ["/metrics", "/swarms", "/stats.json"] {
        for headers in [
            String::new(),
            basic_auth("grafana", "wrong"),
            basic_auth("other", "s3cret"),
            "Authorization: Bearer s3cret\r\n".into(),
        ] {
            let (status, header_lines, _) = http_get_with(addr, path, &headers).await;
            assert_eq!(status, "HTTP/1.1 401 Unauthorized", "{path} {headers:?}");
            assert!(
                header_lines.contains("WWW-Authenticate: Basic realm=\"wt-tracker\""),
                "{header_lines}"
            );
        }
        let (status, _, body) = http_get_with(addr, path, &basic_auth("grafana", "s3cret")).await;
        assert_eq!(status, "HTTP/1.1 200 OK", "{path}: {body}");
    }
    // The metrics listener's /stats.json is the public one.
    let (_, _, body) = http_get_with(addr, "/stats.json", &basic_auth("grafana", "s3cret")).await;
    let private: serde_json::Value = serde_json::from_str(&body).unwrap();
    let public = stats(&server).await;
    let keys =
        |v: &serde_json::Value| -> Vec<String> { v.as_object().unwrap().keys().cloned().collect() };
    assert_eq!(keys(&private), keys(&public));
    // The public listener needs no password.
    assert_eq!(
        http_get(server.local_addrs()[0], "/stats.json").await.0,
        "HTTP/1.1 200 OK"
    );
}

/// Spec §13.7: an HTTPS metrics listener with basic auth; its certificate reloads with the
/// others (`Server::reload_tls`, SIGHUP) and shows in `wt_tls_certificate_expiry_seconds`. Also
/// checks that every metric the Grafana dashboard and the alert rules in `monitoring/` use is
/// exported, so they cannot drift from `/metrics`.
#[tokio::test(flavor = "multi_thread")]
async fn metrics_listener_https_reloads_and_serves_the_monitoring_kit() {
    use std::sync::Arc;

    let certs: Vec<_> = (0..2)
        .map(|_| rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap())
        .collect();
    let dir = std::env::temp_dir().join(format!("wt-metrics-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    let write = {
        let (cert_file, key_file) = (cert_file.clone(), key_file.clone());
        let pems: Vec<(String, String)> = certs
            .iter()
            .map(|c| (c.cert.pem(), c.signing_key.serialize_pem()))
            .collect();
        move |i: usize| {
            std::fs::write(&cert_file, &pems[i].0).unwrap();
            std::fs::write(&key_file, &pems[i].1).unwrap();
        }
    };
    write(0);
    let server = Arc::new(start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}}],"workers":2,
            "metrics":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{},
                        "username":"grafana","password":"s3cret"}}}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    )));
    let addr = server.metrics_addr().unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for c in &certs {
        roots.add(c.cert.der().clone()).unwrap();
    }
    let client = tls_client(&roots);
    let auth = basic_auth("grafana", "s3cret");

    // Plain HTTP to the HTTPS port gets no HTTP response.
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut reply = Vec::new();
    let _ = tokio::time::timeout(
        WAIT,
        tokio::io::AsyncReadExt::read_to_end(&mut tcp, &mut reply),
    )
    .await;
    assert!(!reply.starts_with(b"HTTP/"), "{reply:?}");

    let (body, metrics_text) = {
        let (server, client, auth, roots) =
            (server.clone(), client.clone(), auth.clone(), roots.clone());
        let der: Vec<Vec<u8>> = certs.iter().map(|c| c.cert.der().to_vec()).collect();
        tokio::task::spawn_blocking(move || {
            let (status, _, leaf) = https_get(addr, &client, "/stats.json", "");
            assert_eq!(status, "HTTP/1.1 401 Unauthorized");
            assert_eq!(leaf, der[0]);
            let (status, swarms, _) = https_get(addr, &client, "/swarms", &auth);
            assert_eq!(status, "HTTP/1.1 200 OK", "{swarms}");
            let (status, body, _) = https_get(addr, &client, "/stats.json", &auth);
            assert_eq!(status, "HTTP/1.1 200 OK");

            // The metrics listener's certificate reloads with the others.
            write(1);
            let results = server.reload_tls();
            assert_eq!(results.len(), 1, "{results:?}");
            assert_eq!(results[0].0, "127.0.0.1:0");
            assert!(matches!(results[0].1, wt_server::Reload::Reloaded { .. }));
            // A fresh client: a resumed session would report the certificate it began with.
            let (status, metrics_text, leaf) =
                https_get(addr, &tls_client(&roots), "/metrics", &auth);
            assert_eq!(status, "HTTP/1.1 200 OK");
            assert_eq!(leaf, der[1]);
            (body, metrics_text)
        })
        .await
        .unwrap()
    };
    assert!(serde_json::from_str::<serde_json::Value>(&body).unwrap()["peersCount"].is_u64());
    assert!(
        metrics_text.contains("wt_tls_certificate_expiry_seconds{listener=\"127.0.0.1:0\"}"),
        "{metrics_text}"
    );

    // Every metric name in the monitoring kit is exported (Linux-only ones on Linux).
    let linux_only = [
        "process_cpu_seconds_total",
        "process_open_fds",
        "process_max_fds",
        "wt_worker_cpu_seconds_total",
        "wt_tcp_listen_overflows_total",
    ];
    let exported = |name: &str| {
        metrics_text
            .lines()
            .any(|l| l.starts_with(&format!("{name}{{")) || l.starts_with(&format!("{name} ")))
    };
    let kit = [
        include_str!("../../../monitoring/grafana-dashboard.json"),
        include_str!("../../../monitoring/alerts.yaml"),
    ];
    let mut names: Vec<&str> = kit
        .iter()
        .flat_map(|text| {
            text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .filter(|w| w.starts_with("wt_") || w.starts_with("process_"))
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    assert!(names.len() > 15, "{names:?}");
    for name in names {
        if cfg!(target_os = "linux") || !linux_only.contains(&name) {
            assert!(
                exported(name),
                "{name} is used by monitoring/ but not in /metrics"
            );
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
