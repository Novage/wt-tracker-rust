//! `/stats.json`, in the JS tracker's shape (spec §13.5).

use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use serde_json::{Map, Value, json};

use crate::worker::{DROPPED_MESSAGES, Received, ShardStats, Worker};
use crate::ws::driver::IoCounters;

/// Hex of an info_hash like JS `Buffer.from(infoHash, "binary").toString("hex")`: one byte per
/// character (its low 8 bits).
fn binary_hex(info_hash: &[u8]) -> String {
    String::from_utf8_lossy(info_hash)
        .chars()
        .map(|c| format!("{:02x}", (c as u32) & 0xff))
        .collect()
}

fn count(c: wt_proto::Count) -> Value {
    json!({ "messages": c.messages, "bytes": c.bytes })
}

/// Message and byte totals of all workers (since start).
fn traffic(per_shard: &[ShardStats]) -> Value {
    let mut sent = wt_proto::Counters::default();
    let mut received = Received::default();
    let mut io = IoCounters::default();
    for s in per_shard {
        sent += s.sent;
        let r = s.received;
        received.announces += r.announces;
        received.answers += r.answers;
        received.stops += r.stops;
        received.scrapes += r.scrapes;
        received.invalid += r.invalid;
        io.socket_in += s.io.socket_in;
        io.socket_out += s.io.socket_out;
        io.deflated += s.io.deflated;
        io.deflate_in += s.io.deflate_in;
        io.deflate_out += s.io.deflate_out;
        io.inflated += s.io.inflated;
        io.inflate_in += s.io.inflate_in;
        io.inflate_out += s.io.inflate_out;
    }
    json!({
        "sent": {
            "announceReplies": count(sent.announce_replies),
            "offers": count(sent.offers),
            "answers": count(sent.answers),
            "scrapes": count(sent.scrapes),
        },
        "received": {
            "announces": count(received.announces),
            "answers": count(received.answers),
            "stops": count(received.stops),
            "scrapes": count(received.scrapes),
            "invalid": count(received.invalid),
        },
        "socketBytes": { "in": io.socket_in, "out": io.socket_out },
        "compression": {
            "deflated": { "messages": io.deflated, "bytesBefore": io.deflate_in, "bytesAfter": io.deflate_out },
            "inflated": { "messages": io.inflated, "bytesBefore": io.inflate_in, "bytesAfter": io.inflate_out },
        },
    })
}

pub(crate) async fn json(me: &Rc<Worker>) -> String {
    let per_shard = me.stats().await;
    let mut torrents = 0usize;
    let mut peers = 0u64;
    let per_tracker: Vec<Value> = per_shard
        .iter()
        .map(|shard| {
            let mut map = Map::new();
            let mut total = 0u64;
            for (info_hash, count) in &shard.swarms {
                torrents += 1;
                total += *count as u64;
                map.insert(binary_hex(info_hash), json!(count));
            }
            peers += total;
            map.insert("totalPeers".into(), json!(total));
            Value::Object(map)
        })
        .collect();

    let servers: Vec<Value> = me
        .shared
        .listeners
        .iter()
        .map(|l| json!({ "server": l.name, "webSocketsCount": l.web_sockets.load(Relaxed) }))
        .collect();

    json!({
        "torrentsCount": torrents,
        "peersCount": peers,
        "servers": servers,
        "memory": { "rss": rss_bytes() },
        "workers": me.shared.workers,
        "droppedMessages": DROPPED_MESSAGES.load(Relaxed),
        "placement": {
            "mode": me.shared.placement.name(),
            "movedConnections": per_shard.iter().map(|s| s.moved_in).sum::<u64>(),
            "localRequests": per_shard.iter().map(|s| s.local_requests).sum::<u64>(),
            "remoteRequests": per_shard.iter().map(|s| s.remote_requests).sum::<u64>(),
            "workers": me.shared.loads.snapshot().iter()
                .map(|l| json!({ "connections": l.conns, "busy": l.busy as f64 / 1000.0 }))
                .collect::<Vec<_>>(),
            "directorySize": me.shared.directory.len(),
        },
        "traffic": traffic(&per_shard),
        "peersCountPerInfoHashPerTracker": per_tracker,
    })
    .to_string()
}

/// Resident set size of this process, if available.
fn rss_bytes() -> Option<u64> {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let kb = status.lines().find_map(|l| l.strip_prefix("VmRSS:"))?;
        return kb
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse::<u64>()
            .ok()
            .map(|k| k * 1024);
    }
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<u64>()
        .ok()
        .map(|k| k * 1024)
}
