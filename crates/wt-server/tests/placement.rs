//! `content` placement (spec §13.3): the swarms of one piece of content share a shard, and
//! connections move to it at their first announce, so their requests stay local.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use futures_util::SinkExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

const WORKERS: usize = 4;

fn hex(info_hash: &str) -> String {
    info_hash.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Shards whose swarms include `info_hash`.
fn shards_of(stats: &Value, info_hash: &str) -> Vec<usize> {
    stats["peersCountPerInfoHashPerTracker"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .filter(|(_, shard)| shard.get(hex(info_hash)).is_some())
        .map(|(i, _)| i)
        .collect()
}

fn placement(stats: &Value, field: &str) -> u64 {
    stats["placement"][field].as_u64().unwrap()
}

/// Exactly one shard holds `info_hash`; returns it.
fn one_shard(stats: &Value, info_hash: &str) -> usize {
    let shards = shards_of(stats, info_hash);
    assert_eq!(shards.len(), 1, "{info_hash} on shards {shards:?}: {stats}");
    shards[0]
}

/// Idle connections pinned to their accepting workers (their first message is not an
/// announce). New content is placed on workers with fewer connections, so with these open,
/// connections move whatever worker accepts them (even if one worker accepts all).
async fn crowd(server: &wt_server::Server) -> Vec<Ws> {
    let mut conns = Vec::new();
    for _ in 0..8 {
        let mut ws = connect(server).await;
        send(
            &mut ws,
            r#"{"action":"scrape","info_hash":"hcrowd00000000000001"}"#,
        )
        .await;
        recv(&mut ws).await.unwrap();
        conns.push(ws);
    }
    conns
}

#[tokio::test(flavor = "multi_thread")]
async fn new_content_leaves_a_crowded_worker_and_its_viewers_follow() {
    let server = plain(WORKERS);
    let crowd = crowd(&server).await;
    let mut conns = Vec::new();
    for k in 0..10 {
        let h = format!("hnew{k:016}");
        for i in 0..2 {
            let mut ws = connect(&server).await;
            send(&mut ws, &announce(&h, &format!("p{k}-{i}"), 0)).await;
            assert_eq!(recv(&mut ws).await.unwrap(), reply(&h, 0, i + 1));
            conns.push(ws);
        }
    }
    drop(crowd);
    let stats = stats(&server).await;
    for k in 0..10 {
        one_shard(&stats, &format!("hnew{k:016}"));
    }
    assert!(placement(&stats, "movedConnections") > 0, "{stats}");
    assert_eq!(placement(&stats, "remoteRequests"), 0, "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_swarm_stays_on_one_shard_and_every_request_is_local() {
    let server = plain(WORKERS);
    const H: &str = "hplace00000000000001";
    let mut conns = Vec::new();
    for i in 0..30 {
        let mut ws = connect(&server).await;
        send(&mut ws, &announce(H, &format!("p{i}"), 0)).await;
        assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, i + 1));
        conns.push(ws);
    }
    // Re-announces of placed connections.
    for (i, ws) in conns.iter_mut().enumerate() {
        send(ws, &announce(H, &format!("p{i}"), 0)).await;
        assert_eq!(recv(ws).await.unwrap(), reply(H, 0, 30));
    }
    let stats = stats(&server).await;
    one_shard(&stats, H);
    assert_eq!(stats["placement"]["mode"], "content");
    assert_eq!(placement(&stats, "remoteRequests"), 0, "{stats}");
    assert_eq!(placement(&stats, "localRequests"), 60, "{stats}");
    assert_eq!(placement(&stats, "directorySize"), 1);
    eprintln!(
        "moved {} of 30 connections",
        placement(&stats, "movedConnections")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn video_and_audio_of_one_connection_share_a_shard() {
    let server = plain(WORKERS);
    const V: &str = "hvideo00000000000001";
    const A: &str = "haudio00000000000001";
    let mut conns = Vec::new();
    for i in 0..12 {
        let peer = format!("p{i}");
        let mut ws = connect(&server).await;
        send(&mut ws, &announce(V, &peer, 0)).await;
        assert_eq!(recv(&mut ws).await.unwrap(), reply(V, 0, i + 1));
        send(&mut ws, &announce(A, &peer, 0)).await;
        assert_eq!(recv(&mut ws).await.unwrap(), reply(A, 0, i + 1));
        conns.push(ws);
    }
    // An offer and its answer in the audio swarm.
    let mut b = connect(&server).await;
    send(&mut b, &announce(V, "pb", 0)).await;
    recv(&mut b).await.unwrap();
    send(&mut b, &announce(A, "pb", 1)).await;
    assert_eq!(recv(&mut b).await.unwrap(), reply(A, 0, 13));
    let mut answered = false;
    for (i, ws) in conns.iter_mut().enumerate() {
        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(100), recv(ws)).await {
            assert_eq!(msg, offer(A, "pb", 0));
            send(
                ws,
                &format!(r#"{{"action":"announce","info_hash":"{A}","peer_id":"p{i}","to_peer_id":"pb","answer":{{"type":"answer","sdp":"y"}},"offer_id":"pb-o0"}}"#),
            )
            .await;
            answered = true;
            break;
        }
    }
    assert!(answered, "nobody got the offer");
    assert!(recv(&mut b).await.unwrap().contains(r#""answer":"#));

    let stats = stats(&server).await;
    assert_eq!(one_shard(&stats, V), one_shard(&stats, A), "{stats}");
    assert_eq!(placement(&stats, "remoteRequests"), 0, "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quality_switch_lands_on_the_same_shard() {
    let server = plain(WORKERS);
    const Q1: &str = "hqual100000000000001";
    const Q2: &str = "hqual200000000000001";
    const A: &str = "haudio00000000000002";
    let mut conns = Vec::new();
    for i in 0..10 {
        let peer = format!("p{i}");
        let mut ws = connect(&server).await;
        for h in [Q1, A] {
            send(&mut ws, &announce(h, &peer, 0)).await;
            recv(&mut ws).await.unwrap();
        }
        conns.push(ws);
    }
    let before = one_shard(&stats(&server).await, Q1);
    for (i, ws) in conns.iter_mut().enumerate() {
        let peer = format!("p{i}");
        send(ws, &announce(Q2, &peer, 0)).await;
        assert_eq!(recv(ws).await.unwrap(), reply(Q2, 0, i as u32 + 1));
        send(
            ws,
            &format!(
                r#"{{"action":"announce","event":"stopped","info_hash":"{Q1}","peer_id":"{peer}"}}"#
            ),
        )
        .await;
    }
    wait_peers(&server, 20).await;
    let stats = stats(&server).await;
    assert!(shards_of(&stats, Q1).is_empty(), "{stats}");
    assert_eq!(one_shard(&stats, Q2), before);
    assert_eq!(one_shard(&stats, A), before);
    assert_eq!(placement(&stats, "remoteRequests"), 0, "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_whose_first_message_is_not_an_announce_stays_and_forwards() {
    let server = plain(WORKERS);
    const H: &str = "hpinned0000000000001";
    let mut owner = connect(&server).await;
    send(&mut owner, r#"{"action":"scrape"}"#).await;
    recv(&mut owner).await.unwrap();
    send(&mut owner, &announce(H, "po", 0)).await;
    assert_eq!(recv(&mut owner).await.unwrap(), reply(H, 0, 1));
    let mut pinned = Vec::new();
    for i in 0..8 {
        let mut ws = connect(&server).await;
        send(
            &mut ws,
            &format!(r#"{{"action":"scrape","info_hash":"{H}"}}"#),
        )
        .await;
        assert!(recv(&mut ws).await.unwrap().contains(r#""incomplete":"#));
        send(&mut ws, &announce(H, &format!("p{i}"), 0)).await;
        assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, i + 2));
        pinned.push(ws);
    }
    let stats = stats(&server).await;
    one_shard(&stats, H);
    assert_eq!(placement(&stats, "movedConnections"), 0, "{stats}");
}

/// Frames sent together with the first announce (a ping before it, more messages after it)
/// travel with the moving connection and are answered in order.
#[tokio::test(flavor = "multi_thread")]
async fn input_after_the_first_announce_moves_with_the_connection() {
    let server = plain(WORKERS);
    let crowd = crowd(&server).await;
    const H: &str = "hpipe000000000000001";
    const A: &str = "hpipeaudio0000000001";
    let mut first = connect(&server).await;
    send(&mut first, &announce(H, "p0", 0)).await;
    recv(&mut first).await.unwrap();

    let mut raws = Vec::new();
    for i in 1..=20u32 {
        let peer = format!("p{i}");
        let mut bytes = frame(true, 9, b"hi");
        bytes.extend(frame(true, 1, announce(H, &peer, 0).as_bytes()));
        bytes.extend(frame(true, 1, announce(A, &peer, 0).as_bytes()));
        bytes.extend(frame(
            true,
            1,
            format!(r#"{{"action":"scrape","info_hash":"{H}"}}"#).as_bytes(),
        ));
        let mut raw = Raw::connect(&server, &bytes).await;
        assert_eq!(raw.read_frame().await.unwrap(), (0xA, b"hi".to_vec()));
        assert_eq!(raw.read_text().await.unwrap(), reply(H, 0, i + 1));
        assert_eq!(raw.read_text().await.unwrap(), reply(A, 0, i));
        let scrape = raw.read_text().await.unwrap();
        assert!(
            scrape.contains(&format!(r#""incomplete":{}"#, i + 1)),
            "{scrape}"
        );
        raws.push(raw);
    }
    drop(crowd);
    let stats = stats(&server).await;
    assert_eq!(one_shard(&stats, H), one_shard(&stats, A));
    assert_eq!(placement(&stats, "remoteRequests"), 0, "{stats}");
    assert!(placement(&stats, "movedConnections") > 0, "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_wss_connections_keep_their_tls_session() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("wt-placement-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert_file, cert.cert.pem()).unwrap();
    std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    let server = start(&format!(
        // A plain listener first, for /stats.json.
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}},{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":{WORKERS}}}"#,
        serde_json::to_string(&cert_file).unwrap(),
        serde_json::to_string(&key_file).unwrap()
    ));
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    );
    let url = format!("wss://localhost:{}/", server.local_addrs()[1].port());
    let crowd = crowd(&server).await;
    const H: &str = "htls0000000000000001";
    const A: &str = "htlsaudio00000000001";
    let mut conns = Vec::new();
    for i in 0..20u32 {
        let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
            url.clone(),
            None,
            false,
            Some(tokio_tungstenite::Connector::Rustls(client.clone())),
        )
        .await
        .unwrap();
        let peer = format!("p{i}");
        // Several messages in one flush (one TLS write).
        ws.feed(Message::text(announce(H, &peer, 0))).await.unwrap();
        ws.feed(Message::text(announce(A, &peer, 0))).await.unwrap();
        ws.feed(Message::text(announce(H, &peer, 0))).await.unwrap();
        ws.flush().await.unwrap();
        assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, i + 1));
        assert_eq!(recv(&mut ws).await.unwrap(), reply(A, 0, i + 1));
        assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, i + 1));
        conns.push(ws);
    }
    // Moved connections keep working: a large offer to every peer.
    // 19 offers × 3 KB, under maxPayloadLength (64 KiB).
    let sdp = "x".repeat(3_000);
    let big = format!(
        r#"{{"action":"announce","info_hash":"{H}","peer_id":"p0","numwant":30,"offers":[{}]}}"#,
        (0..19)
            .map(|i| format!(r#"{{"offer":{{"type":"offer","sdp":"{sdp}"}},"offer_id":"o{i}"}}"#))
            .collect::<Vec<_>>()
            .join(",")
    );
    send(&mut conns[0], &big).await;
    assert_eq!(recv(&mut conns[0]).await.unwrap(), reply(H, 0, 20));
    for ws in &mut conns[1..] {
        assert!(recv(ws).await.unwrap().contains(&sdp));
    }
    drop(crowd);
    let stats = stats(&server).await;
    assert_eq!(placement(&stats, "remoteRequests"), 0, "{stats}");
    assert!(placement(&stats, "movedConnections") > 0, "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_announces_of_new_content_bind_one_shard() {
    let server = Arc::new(plain(WORKERS));
    const H: &str = "hrace000000000000001";
    let tasks: Vec<_> = (0..50)
        .map(|i| {
            let server = server.clone();
            tokio::spawn(async move {
                let mut ws = connect(&server).await;
                send(&mut ws, &announce(H, &format!("p{i}"), 0)).await;
                recv(&mut ws).await.unwrap();
                ws
            })
        })
        .collect();
    let mut conns = Vec::new();
    for task in tasks {
        conns.push(task.await.unwrap());
    }
    wait_peers(&server, 50).await;
    let stats = stats(&server).await;
    one_shard(&stats, H);
    assert_eq!(placement(&stats, "directorySize"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_swarms_release_their_binding() {
    let server = start(&format!(
        r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}}],"workers":{WORKERS},"tracker":{{"announceInterval":1}}}}"#
    ));
    const H: &str = "hgone000000000000001";
    let stop =
        format!(r#"{{"action":"announce","event":"stopped","info_hash":"{H}","peer_id":"p"}}"#);
    for round in 0..2 {
        let mut ws = connect(&server).await;
        send(&mut ws, &announce(H, "p", 0)).await;
        assert!(recv(&mut ws).await.unwrap().contains(r#""incomplete":1"#));
        assert_eq!(placement(&stats(&server).await, "directorySize"), 1);
        send(&mut ws, &stop).await;
        wait_peers(&server, 0).await;
        // The expiry tick (every announceInterval) releases the empty binding.
        let deadline = tokio::time::Instant::now() + WAIT;
        while placement(&stats(&server).await, "directorySize") != 0 {
            assert!(tokio::time::Instant::now() < deadline, "round {round}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
