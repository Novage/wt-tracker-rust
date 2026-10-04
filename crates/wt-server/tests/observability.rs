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

/// A rejected message closes its connection with 1008, also when another worker's shard
/// rejected it (spec §13.2): answers to an unknown peer, spread over 4 shards by hash.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_rejected_on_any_shard_closes_with_policy_violation() {
    let server = hashed(4);
    let mut clients = Vec::new();
    for i in 0..12 {
        let mut ws = connect(&server).await;
        send(
            &mut ws,
            &format!(
                r#"{{"action":"announce","info_hash":"hreject{i:013}","peer_id":"p{i}","to_peer_id":"ghost","answer":{{"type":"answer","sdp":"y"}},"offer_id":"o"}}"#
            ),
        )
        .await;
        clients.push(ws);
    }
    for ws in &mut clients {
        assert_eq!(close_code(ws).await, Some(CloseCode::Policy));
    }
    wait_metric(
        &server,
        "wt_rejected_messages_total",
        &[("reason", "unknown_peer")],
        12,
    )
    .await;
    wait_metric(
        &server,
        "wt_closed_connections_total",
        &[("reason", "rejected")],
        12,
    )
    .await;
    // Some of them were rejected by a remote shard.
    let remote = metrics(&server)
        .await
        .sum("wt_routed_requests_total", &[("target", "remote")]);
    assert!(remote > 0, "every answer stayed local");
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
