//! In-process server tests over real sockets (spec §13).

use std::collections::BTreeMap;
use std::time::Duration;

use crate::common::*;
use tokio::io::AsyncWriteExt;

const H: [&str; 8] = [
    "h0000000000000000000",
    "h0000000000000000001",
    "h0000000000000000002",
    "h0000000000000000003",
    "h0000000000000000004",
    "h0000000000000000005",
    "h0000000000000000006",
    "h0000000000000000007",
];

#[tokio::test(flavor = "multi_thread")]
async fn offers_and_answers_across_workers_and_shards() {
    let server = hashed(4);
    let mut keep = Vec::new();
    for (i, info_hash) in H.iter().enumerate() {
        // Unique peer_ids: reusing one on a new connection would move (re-create) the peer.
        let (pa, pb) = (format!("a{i}"), format!("b{i}"));
        let (mut a, mut b) = (connect(&server).await, connect(&server).await);
        send(&mut a, &announce(info_hash, &pa, 1)).await;
        assert_eq!(recv(&mut a).await.unwrap(), reply(info_hash, 0, 1));
        send(&mut b, &announce(info_hash, &pb, 1)).await;
        assert_eq!(recv(&mut b).await.unwrap(), reply(info_hash, 0, 2));
        assert_eq!(recv(&mut a).await.unwrap(), offer(info_hash, &pb, 0));

        send(
            &mut a,
            &format!(r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"{pa}","to_peer_id":"{pb}","answer":{{"type":"answer","sdp":"y"}},"offer_id":"{pb}-o0"}}"#),
        )
        .await;
        assert_eq!(
            recv(&mut b).await.unwrap(),
            format!(
                r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"{pa}","answer":{{"type":"answer","sdp":"y"}},"offer_id":"{pb}-o0"}}"#
            )
        );
        keep.push((a, b));
    }
    // The swarms really are spread over several shards.
    let m = metrics(&server).await;
    let used = (0..4)
        .filter(|w| m.sum("wt_peers", &[("worker", &w.to_string())]) > 0)
        .count();
    assert!(used >= 2, "swarms on {used} shards");
    assert_eq!(stats(&server).await["peersCount"], 16);
    let first = swarm(&server, H[0]).await;
    assert_eq!(first["peers"], 2, "{first}");
    assert_eq!(first["workers"].as_array().unwrap().len(), 1, "{first}");
}

#[tokio::test(flavor = "multi_thread")]
async fn scrape_merges_shards_in_request_order() {
    let server = hashed(4);
    let mut clients = Vec::new();
    for (i, info_hash) in H[..6].iter().enumerate() {
        for p in 0..=i {
            let mut ws = connect(&server).await;
            let event = if p == 0 {
                r#","event":"completed""#
            } else {
                ""
            };
            send(&mut ws, &format!(r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"p{i}-{p}"{event}}}"#)).await;
            recv(&mut ws).await.unwrap();
            clients.push(ws);
        }
    }
    let mut ws = connect(&server).await;
    send(
        &mut ws,
        &format!(
            r#"{{"action":"scrape","info_hash":["{}","nope","{}","{}"]}}"#,
            H[3], H[1], H[3]
        ),
    )
    .await;
    assert_eq!(
        recv(&mut ws).await.unwrap(),
        format!(
            r#"{{"action":"scrape","files":{{"{}":{{"complete":1,"incomplete":3,"downloaded":1}},"nope":{{"complete":0,"incomplete":0,"downloaded":0}},"{}":{{"complete":1,"incomplete":1,"downloaded":1}}}}}}"#,
            H[3], H[1]
        )
    );

    send(&mut ws, r#"{"action":"scrape"}"#).await;
    let all: serde_json::Value = serde_json::from_str(&recv(&mut ws).await.unwrap()).unwrap();
    let files: BTreeMap<String, u64> = all["files"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(h, f)| {
            (
                h.clone(),
                f["incomplete"].as_u64().unwrap() + f["complete"].as_u64().unwrap(),
            )
        })
        .collect();
    let expected: BTreeMap<String, u64> = H[..6]
        .iter()
        .enumerate()
        .map(|(i, h)| (h.to_string(), i as u64 + 1))
        .collect();
    assert_eq!(files, expected);

    send(
        &mut ws,
        &format!(r#"{{"action":"scrape","info_hash":"{}"}}"#, H[2]),
    )
    .await;
    assert_eq!(
        recv(&mut ws).await.unwrap(),
        format!(
            r#"{{"action":"scrape","files":{{"{}":{{"complete":1,"incomplete":2,"downloaded":1}}}}}}"#,
            H[2]
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_removes_peers_from_every_shard() {
    let server = hashed(4);
    let mut ws = connect(&server).await;
    for (i, info_hash) in H.iter().enumerate() {
        // Several peer_ids on one connection, in swarms of different shards.
        send(&mut ws, &announce(info_hash, &format!("p{i}"), 0)).await;
        recv(&mut ws).await.unwrap();
    }
    wait_peers(&server, 8).await;
    drop(ws);
    wait_peers(&server, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_frames_close_the_connection_and_remove_its_peers() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0},"websockets":{"maxPayloadLength":1000}}],"workers":2}"#,
    );
    for bad in ["{", "[]", r#"{"action":"nope"}"#, &"x".repeat(2000)] {
        let mut ws = connect(&server).await;
        send(&mut ws, &announce(H[0], "p", 0)).await;
        recv(&mut ws).await.unwrap();
        wait_peers(&server, 1).await;
        send(&mut ws, bad).await;
        assert!(closed(&mut ws).await, "not closed after {:.20}", bad);
        wait_peers(&server, 0).await;
    }
    // Invalid UTF-8 in a binary frame.
    let mut raw = Raw::connect(&server, &frame(true, 2, b"{\"action\":\"\xff\"}")).await;
    assert_eq!(raw.read_text().await, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_fragmented_and_pipelined_frames() {
    let server = plain(2);
    // A binary frame sent in the same packet as the handshake.
    let mut raw = Raw::connect(&server, &frame(true, 2, announce(H[0], "p1", 0).as_bytes())).await;
    assert_eq!(raw.read_text().await.unwrap(), reply(H[0], 0, 1));

    // A message fragmented into three frames.
    let text = announce(H[0], "p2", 0);
    let (a, rest) = text.as_bytes().split_at(10);
    let (b, c) = rest.split_at(10);
    let mut bytes = frame(false, 1, a);
    bytes.extend(frame(false, 0, b));
    bytes.extend(frame(true, 0, c));
    raw.tcp.write_all(&bytes).await.unwrap();
    assert_eq!(raw.read_text().await.unwrap(), reply(H[0], 0, 2));
}

#[tokio::test(flavor = "multi_thread")]
async fn answer_without_info_hash_needs_a_single_shard() {
    for (workers, delivered) in [(1, true), (2, false)] {
        let server = hashed(workers);
        let (mut a, mut b) = (connect(&server).await, connect(&server).await);
        send(&mut a, &announce(H[0], "pa", 0)).await;
        recv(&mut a).await.unwrap();
        send(
            &mut b,
            r#"{"action":"announce","peer_id":"pb","to_peer_id":"pa","answer":{}}"#,
        )
        .await;
        if delivered {
            assert_eq!(
                recv(&mut a).await.unwrap(),
                r#"{"action":"announce","peer_id":"pb","answer":{}}"#
            );
        } else {
            assert!(closed(&mut b).await, "workers {workers}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_and_pings() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0},"websockets":{"idleTimeout":2}}],"workers":1}"#,
    );
    // A client that answers pings stays connected past the timeout.
    let mut ws = connect(&server).await;
    let alive = tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        while started.elapsed() < Duration::from_secs(4) {
            if tokio::time::timeout(
                Duration::from_millis(500),
                futures_util::StreamExt::next(&mut ws),
            )
            .await
            .is_ok_and(|m| m.is_none() || m.unwrap().is_err())
            {
                return false;
            }
        }
        true
    });
    // A silent client (never reads, never answers pings) is closed.
    let mut raw = Raw::connect(&server, b"").await;
    let started = std::time::Instant::now();
    loop {
        match raw.read_frame().await {
            Some((9, _)) => {} // ping: ignored on purpose
            Some((8, _)) | None => break,
            Some(_) => {}
        }
    }
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "closed too early"
    );
    assert!(alive.await.unwrap(), "a client answering pings was closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn http_routes_and_ws_path() {
    let dir = std::env::temp_dir().join(format!("wt-server-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let index = dir.join("index.html");
    std::fs::write(&index, "Novage WebTorrent Tracker").unwrap();
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}},"websockets":{{"path":"/announce"}}}}],"workers":1,"indexHtml":{}}}"#,
        serde_json::to_string(&index).unwrap()
    ));
    let addr = server.local_addrs()[0];
    assert_eq!(
        http_get(addr, "/").await,
        ("HTTP/1.1 200 OK".into(), "Novage WebTorrent Tracker".into())
    );
    assert_eq!(
        http_get(addr, "/nope").await,
        ("HTTP/1.1 404 Not Found".into(), "404 Not Found".into())
    );
    let stats = stats(&server).await;
    let mut keys: Vec<&str> = stats
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "memory",
            "peersCount",
            "servers",
            "torrentsCount",
            "uptimeSeconds",
            "workers"
        ],
        "{stats}"
    );
    assert_eq!(stats["servers"][0]["webSocketsCount"], 0);
    // An unknown swarm, and a malformed infoHash.
    let unknown = swarm(&server, "nothing here").await;
    assert_eq!(
        (unknown["peers"].clone(), unknown["workers"].clone()),
        (0.into(), serde_json::json!([]))
    );
    assert_eq!(
        http_get(addr, "/stats.json?infoHash=xyz").await.0,
        "HTTP/1.1 400 Bad Request"
    );
    // `/metrics` is only on its own listener.
    assert_eq!(http_get(addr, "/metrics").await.0, "HTTP/1.1 404 Not Found");
    let metrics_addr = server.metrics_addr().unwrap();
    assert_eq!(
        http_get(metrics_addr, "/").await.0,
        "HTTP/1.1 404 Not Found"
    );
    let m = metrics(&server).await;
    assert_eq!(m.sum("wt_http_requests_total", &[("route", "index")]), 1);
    assert_eq!(
        m.sum("wt_http_requests_total", &[("route", "not_found")]),
        2
    );
    // Upgrades only on the configured path.
    assert!(
        tokio_tungstenite::connect_async(format!("ws://{addr}/"))
            .await
            .is_err()
    );
    assert!(
        tokio_tungstenite::connect_async(format!("ws://{addr}/announce"))
            .await
            .is_ok()
    );

    let no_index = plain(1);
    assert_eq!(
        http_get(no_index.local_addrs()[0], "/").await.0,
        "HTTP/1.1 404 Not Found"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn origin_rules() {
    let allow = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":1,"websocketsAccess":{"allowOrigins":["https://a.com"]}}"#,
    );
    assert!(
        connect_with_origin(&allow, Some("https://a.com"))
            .await
            .is_ok()
    );
    assert!(
        connect_with_origin(&allow, Some("https://b.com"))
            .await
            .is_err()
    );
    assert!(connect_with_origin(&allow, None).await.is_err());

    let deny = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":1,"websocketsAccess":{"denyOrigins":["https://b.com"],"denyEmptyOrigin":true}}"#,
    );
    assert!(
        connect_with_origin(&deny, Some("https://a.com"))
            .await
            .is_ok()
    );
    assert!(
        connect_with_origin(&deny, Some("https://b.com"))
            .await
            .is_err()
    );
    assert!(connect_with_origin(&deny, None).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn max_connections_like_uws_tracker() {
    // uws-tracker denies when `count > maxConnections`, i.e. it admits max + 1.
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0},"websockets":{"maxConnections":1}}],"workers":1}"#,
    );
    let a = connect(&server).await;
    let b = connect(&server).await;
    assert!(
        tokio_tungstenite::connect_async(url(&server))
            .await
            .is_err()
    );
    drop((a, b));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(tokio_tungstenite::connect_async(url(&server)).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn wss_with_rustls() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-server-tls-{}", std::process::id()));
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
    let client = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let url = format!("wss://localhost:{}/", server.local_addrs()[0].port());
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        url,
        None,
        false,
        Some(tokio_tungstenite::Connector::Rustls(std::sync::Arc::new(
            client,
        ))),
    )
    .await
    .expect("wss connect");
    send(&mut ws, &announce(H[0], "p1", 0)).await;
    assert_eq!(recv(&mut ws).await.unwrap(), reply(H[0], 0, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn backpressure_drops_messages_for_a_client_that_does_not_read() {
    let server = start(
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":2,"maxBackpressure":65536}"#,
    );
    // The slow peer joins and then never reads again.
    let mut slow = Raw::connect(
        &server,
        &frame(true, 1, announce(H[0], "slow", 0).as_bytes()),
    )
    .await;
    slow.read_text().await.unwrap();

    // A busy peer makes the tracker send the slow one ~10 MB of offers.
    let mut busy = connect(&server).await;
    let big_sdp = "x".repeat(10_000);
    let frame = format!(
        r#"{{"action":"announce","info_hash":"{}","peer_id":"busy","numwant":1,"offers":[{{"offer":{{"type":"offer","sdp":"{big_sdp}"}},"offer_id":"o"}}]}}"#,
        H[0]
    );
    for _ in 0..1000 {
        send(&mut busy, &frame).await;
        recv(&mut busy).await.unwrap();
    }
    assert!(metrics(&server).await.sum("wt_dropped_messages_total", &[]) > 0);
    // The server and the busy peer are fine.
    send(&mut busy, r#"{"action":"scrape"}"#).await;
    assert!(
        recv(&mut busy)
            .await
            .unwrap()
            .starts_with(r#"{"action":"scrape""#)
    );
}
