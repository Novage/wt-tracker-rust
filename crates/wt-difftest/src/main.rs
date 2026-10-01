//! Runs traces of raw wire frames through `wt_proto::handle` + `wt_core::Shard` and prints what
//! was sent, op by op. Driven by `difftest/run.ts`, which runs the same frames through the JS
//! tracker (JSON.parse → FastTracker → JSON.stringify) and compares.
//!
//! Usage: `wt-difftest <input.json>`. Input: `{ "selection": "sample" | "window" |
//! "round_robin", "backend": "serde_json" | "sonic", "traces": [{ "seed": u64, "ops": [...] }] }`
//! with ops `{op:"frame", conn, frame}`, `{op:"disconnect", conn}`, `{op:"advance", secs}`,
//! `{op:"expire"}`. A rejected frame closes its connection (`Shard::disconnect`), like the JS
//! server. Output (stdout): one result array per trace.

use std::collections::BTreeMap;

use serde_json::{Value, json};
use wt_core::{ConnId, OfferSelection, Settings, Shard};
use wt_proto::{Encoder, SerdeJson, Sonic, handle_with};

type Handle = fn(&mut Shard, u32, ConnId, &[u8], &mut Encoder) -> Result<(), wt_proto::ProtoError>;

fn key_str(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).expect("keys are UTF-8")
}

fn state(shard: &Shard) -> Value {
    let mut swarms = BTreeMap::new();
    let mut peers = BTreeMap::new();
    for (info_hash, stats) in shard.swarms() {
        let mut ids: Vec<String> = shard
            .swarm_peer_ids(info_hash.as_bytes())
            .unwrap()
            .iter()
            .map(|k| key_str(k.as_bytes()))
            .collect();
        ids.sort();
        for id in &ids {
            // Every peer has at least one membership, so this visits all peers.
            peers.insert(id.clone(), shard.peer_connection(id.as_bytes()).unwrap().0);
        }
        swarms.insert(
            key_str(info_hash.as_bytes()),
            json!({ "peers": ids, "complete": stats.complete }),
        );
    }
    json!({ "swarms": swarms, "peers": peers })
}

fn run_trace(selection: OfferSelection, handle: Handle, trace: &Value) -> Vec<Value> {
    let settings = Settings {
        offer_selection: selection,
        ..Settings::default()
    };
    let mut shard = Shard::new(settings, trace["seed"].as_u64().unwrap_or(0));
    let mut out = Encoder::new();
    let mut now: u32 = 0;
    let mut results = Vec::new();

    for op in trace["ops"].as_array().expect("ops") {
        out.clear();
        let conn = ConnId(op["conn"].as_u64().unwrap_or(0));
        let mut error = false;
        match op["op"].as_str().expect("op") {
            "frame" => {
                let frame = op["frame"].as_str().expect("frame").as_bytes();
                if handle(&mut shard, now, conn, frame, &mut out).is_err() {
                    error = true;
                    shard.disconnect(conn, &mut out);
                }
            }
            "disconnect" => shard.disconnect(conn, &mut out),
            "advance" => now += op["secs"].as_u64().unwrap() as u32,
            "expire" => {
                shard.expire(now, &mut out);
            }
            other => panic!("unknown op {other}"),
        }

        let messages: Vec<Value> = out
            .messages()
            .map(|(to, m)| json!([to.0, std::str::from_utf8(m).expect("UTF-8 output")]))
            .collect();
        let removed: Vec<Value> = out
            .removed()
            .iter()
            .map(|(peer_id, conn)| json!([key_str(peer_id.as_bytes()), conn.0]))
            .collect();
        results.push(json!({
            "error": error,
            "messages": messages,
            "removed": removed,
            "state": state(&shard),
            "invariants": shard.check_invariants().err(),
        }));
    }
    results
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: wt-difftest <input.json>");
    let input: Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read input")).expect("parse input");
    let selection = match input["selection"].as_str().unwrap_or("sample") {
        "sample" => OfferSelection::RandomSample,
        "window" => OfferSelection::RandomWindow,
        "round_robin" => OfferSelection::RoundRobin,
        other => panic!("unknown selection {other}"),
    };
    let handle: Handle = match input["backend"].as_str().unwrap_or("serde_json") {
        "serde_json" => handle_with::<SerdeJson>,
        "sonic" => handle_with::<Sonic>,
        other => panic!("unknown backend {other}"),
    };
    let results: Vec<Vec<Value>> = input["traces"]
        .as_array()
        .expect("traces")
        .iter()
        .map(|trace| run_trace(selection, handle, trace))
        .collect();
    println!("{}", serde_json::to_string(&results).unwrap());
}
