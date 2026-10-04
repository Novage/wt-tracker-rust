//! `/stats.json`: a small summary, or one swarm with `?infoHash=<hex>` (spec §13.5); the
//! largest swarms for `/swarms` on the private listener (spec §13.7).

use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use serde_json::{Value, json};

use crate::worker::Worker;

/// The summary: torrents, peers, connections per listener, memory, workers, uptime.
pub(crate) async fn json(me: &Rc<Worker>) -> String {
    let stats = me.stats().await;
    let servers: Vec<Value> = me
        .shared
        .listeners
        .iter()
        .map(|l| json!({ "server": l.name, "webSocketsCount": l.web_sockets.load(Relaxed) }))
        .collect();
    json!({
        "torrentsCount": stats.iter().map(|s| s.swarms).sum::<usize>(),
        "peersCount": stats.iter().map(|s| s.peers).sum::<usize>(),
        "servers": servers,
        "memory": { "rss": rss_bytes() },
        "workers": me.shared.workers,
        "uptimeSeconds": me.shared.started.elapsed().as_secs(),
    })
    .to_string()
}

/// One swarm: total peers and the workers that hold it. `None`: `hex` is not an info_hash in
/// hex (2 to 80 hex digits).
pub(crate) async fn swarm_json(me: &Rc<Worker>, hex: &str) -> Option<String> {
    let info_hash = info_hash_from_hex(hex)?;
    let per_worker = me.swarm_peers(&info_hash).await;
    let workers: Vec<Value> = per_worker
        .iter()
        .enumerate()
        .filter_map(|(worker, peers)| peers.map(|p| json!({ "worker": worker, "peers": p })))
        .collect();
    let peers: u64 = per_worker.iter().flatten().map(|&p| u64::from(p)).sum();
    Some(
        json!({
            "infoHash": hex.to_ascii_lowercase(),
            "peers": peers,
            "workers": workers,
        })
        .to_string(),
    )
}

/// The largest swarms of all workers, most peers first (ties by hex): `{"swarms":[{"infoHash",
/// "peers", "worker"}], "total"}`, at most `top` of them (0: all). `total`: swarms of all workers.
pub(crate) async fn swarms_json(me: &Rc<Worker>, top: usize) -> String {
    let per_worker = me.top_swarms(top).await;
    let total: usize = per_worker.iter().map(|(_, total)| total).sum();
    let mut swarms: Vec<(u32, String, usize)> = per_worker
        .into_iter()
        .enumerate()
        .flat_map(|(worker, (swarms, _))| {
            swarms
                .into_iter()
                .map(move |(key, peers)| (peers, binary_hex(&key), worker))
        })
        .collect();
    swarms.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    if top > 0 {
        swarms.truncate(top);
    }
    let swarms: Vec<Value> = swarms
        .into_iter()
        .map(|(peers, hex, worker)| json!({ "infoHash": hex, "peers": peers, "worker": worker }))
        .collect();
    json!({ "swarms": swarms, "total": total }).to_string()
}

/// Hex of an info_hash key, the inverse of [`info_hash_from_hex`]: the key is the UTF-8 of a
/// "binary" string, one byte per character (its low 8 bits, like JS
/// `Buffer.from(s, "binary").toString("hex")`).
fn binary_hex(key: &[u8]) -> String {
    String::from_utf8_lossy(key)
        .chars()
        .map(|c| format!("{:02x}", (c as u32) & 0xff))
        .collect()
}

/// The info_hash key for a hex string. Clients send the info_hash as a "binary" JSON string
/// (one character per byte, U+0000 to U+00FF) and the tracker keys it by that string's UTF-8
/// bytes, so each byte becomes the UTF-8 encoding of the character with that code.
pub(crate) fn info_hash_from_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.len() < 2 || hex.len() > 80 || !hex.len().is_multiple_of(2) {
        return None;
    }
    let text: Option<String> = (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(hex.get(i..i + 2)?, 16)
                .ok()
                .map(char::from)
        })
        .collect();
    Some(text?.into_bytes())
}

/// The value of `name` in a query string (`a=1&b=2`), if present.
pub(crate) fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
}

/// Resident set size of this process, if available.
pub(crate) fn rss_bytes() -> Option<u64> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_hash_hex_maps_bytes_to_binary_string_keys() {
        assert_eq!(info_hash_from_hex("4142").unwrap(), b"AB");
        // A byte >= 0x80 is a character U+0080..U+00FF: two UTF-8 bytes in the key.
        assert_eq!(info_hash_from_hex("ff").unwrap(), "\u{ff}".as_bytes());
        assert_eq!(info_hash_from_hex("FF"), info_hash_from_hex("ff"));
        assert_eq!(info_hash_from_hex("abc"), None);
        assert_eq!(info_hash_from_hex("zz"), None);
        assert_eq!(info_hash_from_hex(""), None);
        for hex in ["4142", "00ff80", "68736f6d65"] {
            assert_eq!(binary_hex(&info_hash_from_hex(hex).unwrap()), hex);
        }
    }

    #[test]
    fn query_params() {
        assert_eq!(query_param("infoHash=ab&x=1", "infoHash"), Some("ab"));
        assert_eq!(query_param("x=1&infoHash=ab", "infoHash"), Some("ab"));
        assert_eq!(query_param("infoHashes=ab", "infoHash"), None);
        assert_eq!(query_param("", "infoHash"), None);
    }
}
