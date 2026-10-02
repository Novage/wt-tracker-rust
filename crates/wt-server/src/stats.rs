//! `/stats.json`, in the JS tracker's shape (spec §13.5).

use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use serde_json::{Map, Value, json};

use crate::worker::{DROPPED_MESSAGES, Worker};

/// Hex of an info_hash like JS `Buffer.from(infoHash, "binary").toString("hex")`: one byte per
/// character (its low 8 bits).
fn binary_hex(info_hash: &[u8]) -> String {
    String::from_utf8_lossy(info_hash)
        .chars()
        .map(|c| format!("{:02x}", (c as u32) & 0xff))
        .collect()
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
