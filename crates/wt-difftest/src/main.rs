//! Runs operation traces through a `wt_core::Shard` and prints what it emitted, op by op.
//! Driven by `difftest/run.ts`, which runs the same traces through the JS `FastTracker` and
//! compares the results. Trace and result formats are documented there.
//!
//! Usage: `wt-difftest <input.json>`. Input: `{ "selection": "sample" | "window" |
//! "round_robin", "traces": [{ "seed": u64, "ops": [...] }] }`. Output (stdout): one result
//! array per trace.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use wt_core::{
    AnnounceEvent, ConnId, Key, OfferSelection, Outbox, Request, ScrapeTarget, Settings, Shard,
};

/// Offer payloads are the offer index; answer payloads are the op's answer id.
const OFFERS: [u32; 64] = {
    let mut offers = [0; 64];
    let mut i = 0;
    while i < 64 {
        offers[i] = i as u32;
        i += 1;
    }
    offers
};

fn key_str(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).expect("trace keys are UTF-8")
}

#[derive(Default)]
struct Recorder {
    replies: Vec<Value>,
    offers: Vec<Value>,
    answers: Vec<Value>,
    removed: Vec<Value>,
    scrape: Option<Map<String, Value>>,
}

impl Outbox<u32> for Recorder {
    fn announce_reply(
        &mut self,
        to: ConnId,
        info_hash: &Key,
        interval: u32,
        complete: u32,
        incomplete: u32,
    ) {
        self.replies.push(json!([
            to.0,
            key_str(info_hash.as_bytes()),
            interval,
            complete,
            incomplete
        ]));
    }

    fn offer(&mut self, to: ConnId, from_peer_id: &Key, info_hash: &Key, offer: &u32) {
        self.offers.push(json!([
            to.0,
            key_str(from_peer_id.as_bytes()),
            key_str(info_hash.as_bytes()),
            offer
        ]));
    }

    fn answer(&mut self, to: ConnId, answer: &u32) {
        self.answers.push(json!([to.0, answer]));
    }

    fn scrape_entry(
        &mut self,
        _: ConnId,
        info_hash: &[u8],
        complete: u32,
        incomplete: u32,
        downloaded: u32,
    ) {
        self.scrape.get_or_insert_default().insert(
            key_str(info_hash),
            json!({ "complete": complete, "incomplete": incomplete, "downloaded": downloaded }),
        );
    }

    fn scrape_end(&mut self, _: ConnId) {
        self.scrape.get_or_insert_default();
    }

    fn peer_removed(&mut self, peer_id: &Key, conn: ConnId) {
        self.removed
            .push(json!([key_str(peer_id.as_bytes()), conn.0]));
    }
}

fn str_field<'a>(op: &'a Value, name: &str) -> &'a [u8] {
    op[name]
        .as_str()
        .unwrap_or_else(|| panic!("{name} missing in {op}"))
        .as_bytes()
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

fn run_trace(selection: OfferSelection, trace: &Value) -> Vec<Value> {
    let seed = trace["seed"].as_u64().unwrap_or(0);
    let settings = Settings {
        offer_selection: selection,
        ..Settings::default()
    };
    let mut shard = Shard::new(settings, seed);
    let mut now: u32 = 0;
    let mut results = Vec::new();

    for op in trace["ops"].as_array().expect("ops") {
        let mut out = Recorder::default();
        let conn = ConnId(op["conn"].as_u64().unwrap_or(0));
        let request_result = match op["op"].as_str().expect("op") {
            "announce" => {
                let event = match op["event"].as_str() {
                    None => AnnounceEvent::None,
                    Some("started") => AnnounceEvent::Started,
                    Some("completed") => AnnounceEvent::Completed,
                    Some(other) => panic!("unknown event {other}"),
                };
                let offers = op["offers"].as_u64().map(|n| &OFFERS[..n as usize]);
                shard.handle(
                    now,
                    conn,
                    Request::Announce {
                        info_hash: str_field(op, "info_hash"),
                        peer_id: str_field(op, "peer_id"),
                        event,
                        left_zero: op["left"].as_u64() == Some(0),
                        numwant: op["numwant"].as_u64().map(|n| n as u32),
                        offers,
                    },
                    &mut out,
                )
            }
            "answer" => {
                let id = op["id"].as_u64().unwrap() as u32;
                shard.handle(
                    now,
                    conn,
                    Request::Answer {
                        to_peer_id: str_field(op, "to_peer_id"),
                        answer: &id,
                    },
                    &mut out,
                )
            }
            "stop" => shard.handle(
                now,
                conn,
                Request::Stop::<u32> {
                    info_hash: str_field(op, "info_hash"),
                    peer_id: str_field(op, "peer_id"),
                },
                &mut out,
            ),
            "scrape" => {
                let many: Vec<&[u8]>;
                let target = match &op["info_hash"] {
                    Value::Null => ScrapeTarget::All,
                    Value::String(h) => ScrapeTarget::One(h.as_bytes()),
                    Value::Array(hs) => {
                        many = hs.iter().map(|h| h.as_str().unwrap().as_bytes()).collect();
                        ScrapeTarget::Many(&many)
                    }
                    other => panic!("bad scrape info_hash {other}"),
                };
                shard.handle(now, conn, Request::Scrape::<u32> { target }, &mut out)
            }
            "disconnect" => {
                shard.disconnect(conn, &mut out);
                Ok(())
            }
            "advance" => {
                now += op["secs"].as_u64().unwrap() as u32;
                Ok(())
            }
            "expire" => {
                shard.expire(now, &mut out);
                Ok(())
            }
            other => panic!("unknown op {other}"),
        };

        results.push(json!({
            "error": request_result.is_err(),
            "replies": out.replies,
            "offers": out.offers,
            "answers": out.answers,
            "removed": out.removed,
            "scrape": out.scrape,
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
    let results: Vec<Vec<Value>> = input["traces"]
        .as_array()
        .expect("traces")
        .iter()
        .map(|trace| run_trace(selection, trace))
        .collect();
    println!("{}", serde_json::to_string(&results).unwrap());
}
