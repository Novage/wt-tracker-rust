//! Timing benchmark of wt-core. Prints JSON results to stdout, progress to stderr.

use std::sync::Barrier;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use wt_bench::*;
use wt_core::{AnnounceEvent, ConnId, OfferSelection, Outbox, Settings, Shard};
use wt_proto::{Backend, Encoder, Message, SerdeJson, Sonic};

const WARMUP: usize = 2;
const RUNS: usize = 5;
const SCALING_THREADS: [usize; 4] = [1, 2, 4, 8];
const SCALING_PASSES: usize = 3;
const SCALING_TRIALS: usize = 3;

fn bench(name: &str, ops: usize, mut run: impl FnMut() -> (Duration, Counter)) -> Value {
    eprint!("{name:<28}");
    for _ in 0..WARMUP {
        run();
    }
    let mut times = Vec::with_capacity(RUNS);
    let mut counter = Counter::default();
    for _ in 0..RUNS {
        let (elapsed, c) = run();
        times.push(elapsed);
        counter = c;
    }
    times.sort();
    let median = times[RUNS / 2];
    let per_op = |d: Duration| d.as_nanos() as f64 / ops as f64;
    eprintln!(
        "{:>10.1} ns/op  (min {:.1})  {:>8.2} ms/run",
        per_op(median),
        per_op(times[0]),
        median.as_secs_f64() * 1e3
    );
    json!({
        "name": name,
        "ops": ops,
        "median_ns_per_op": per_op(median),
        "min_ns_per_op": per_op(times[0]),
        "median_ms_per_run": median.as_secs_f64() * 1e3,
        "ops_per_sec": ops as f64 / median.as_secs_f64(),
        "messages": {
            "replies": counter.replies,
            "offers": counter.offers,
            "answers": counter.answers,
            "removed": counter.removed,
            "bytes": counter.bytes,
        },
    })
}

/// Runs `setup` untimed, then times `measure` on the resulting shard.
fn timed(
    setup: impl FnOnce(&mut Shard, &mut Counter),
    measure: impl FnOnce(&mut Shard, &mut Counter),
) -> (Duration, Counter) {
    timed_with(new_shard(), setup, measure)
}

fn timed_with(
    mut shard: Shard,
    setup: impl FnOnce(&mut Shard, &mut Counter),
    measure: impl FnOnce(&mut Shard, &mut Counter),
) -> (Duration, Counter) {
    setup(&mut shard, &mut Counter::default());
    let mut out = Counter::default();
    let start = Instant::now();
    measure(&mut shard, &mut out);
    let elapsed = start.elapsed();
    drop(shard); // not timed
    (elapsed, out)
}

fn scaling(ids: &Ids, mode: Scaling) -> Vec<Value> {
    let mut results = Vec::new();
    for threads in SCALING_THREADS {
        let mut trials = Vec::new();
        for _ in 0..SCALING_TRIALS {
            let barrier = Barrier::new(threads);
            let total_ops = std::sync::atomic::AtomicUsize::new(0);
            let elapsed: Vec<Duration> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|t| {
                        let (barrier, total_ops) = (&barrier, &total_ops);
                        scope.spawn(move || {
                            let list = match mode {
                                Scaling::Weak => mp_memberships_of_shard(0, 1),
                                Scaling::Strong => mp_memberships_of_shard(t, threads),
                            };
                            let mut shard = new_shard();
                            let mut out = Counter::default();
                            run_announce_list(
                                &mut shard,
                                &mut out,
                                ids,
                                &list,
                                AnnounceEvent::Started,
                            );
                            total_ops.fetch_add(list.len() * SCALING_PASSES, Relaxed);
                            barrier.wait();
                            let start = Instant::now();
                            for _ in 0..SCALING_PASSES {
                                run_announce_list(
                                    &mut shard,
                                    &mut out,
                                    ids,
                                    &list,
                                    AnnounceEvent::None,
                                );
                            }
                            start.elapsed()
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let slowest = elapsed.into_iter().max().unwrap();
            trials.push(total_ops.load(Relaxed) as f64 / slowest.as_secs_f64());
        }
        trials.sort_by(f64::total_cmp);
        let ops_per_sec = trials[SCALING_TRIALS / 2];
        eprintln!(
            "scaling {:<6} {threads} threads: {:>8.2} M announces/s",
            mode.name(),
            ops_per_sec / 1e6
        );
        results
            .push(json!({ "mode": mode.name(), "threads": threads, "ops_per_sec": ops_per_sec }));
    }
    results
}

#[derive(Clone, Copy)]
enum Scaling {
    /// Each thread owns a full copy of the multi-peer state (total work grows with threads).
    Weak,
    /// One multi-peer state split across threads by `swarm % threads` (sharded server).
    Strong,
}

impl Scaling {
    fn name(self) -> &'static str {
        match self {
            Self::Weak => "weak",
            Self::Strong => "strong",
        }
    }
}

fn main() {
    let ids = Ids::new(MANY_SWARMS_PEERS, MANY_SWARMS_SWARMS);
    let mut scenarios = Vec::new();

    scenarios.push(bench("join_one_swarm", ONE_SWARM_PEERS, || {
        timed(|_, _| {}, |s, o| run_join_one_swarm(s, o, &ids))
    }));
    scenarios.push(bench("join_many_swarms", MANY_SWARMS_PEERS, || {
        timed(|_, _| {}, |s, o| run_join_many_swarms(s, o, &ids))
    }));
    scenarios.push(bench("multi_peer_join", MP_MEMBERSHIPS, || {
        timed(|_, _| {}, |s, o| run_multi_peer_join(s, o, &ids, 0))
    }));

    {
        let mut shard = new_shard();
        run_multi_peer_join(&mut shard, &mut Counter::default(), &ids, 0);
        scenarios.push(bench("reannounce", MP_MEMBERSHIPS, || {
            let mut out = Counter::default();
            let start = Instant::now();
            run_reannounce(&mut shard, &mut out, &ids, 0);
            (start.elapsed(), out)
        }));
        scenarios.push(bench("answer", ANSWERS, || {
            let mut out = Counter::default();
            let start = Instant::now();
            run_answers(&mut shard, &mut out, &ids);
            (start.elapsed(), out)
        }));
    }
    for (suffix, offer_selection) in [
        // "" is the default (RandomSample); the JS tracker always uses a random window.
        ("", OfferSelection::RandomSample),
        ("_window", OfferSelection::RandomWindow),
        ("_round_robin", OfferSelection::RoundRobin),
    ] {
        let settings = Settings {
            offer_selection,
            ..Settings::default()
        };
        if !suffix.is_empty() {
            let mut shard = new_shard_with(settings);
            run_multi_peer_join(&mut shard, &mut Counter::default(), &ids, 0);
            scenarios.push(bench(
                &format!("reannounce{suffix}"),
                MP_MEMBERSHIPS,
                || {
                    let mut out = Counter::default();
                    let start = Instant::now();
                    run_reannounce(&mut shard, &mut out, &ids, 0);
                    (start.elapsed(), out)
                },
            ));
        }
        let mut shard = new_shard_with(settings);
        run_join_one_swarm(&mut shard, &mut Counter::default(), &ids);
        scenarios.push(bench(
            &format!("reannounce_one_swarm{suffix}"),
            ONE_SWARM_PEERS,
            || {
                let mut out = Counter::default();
                let start = Instant::now();
                run_reannounce_one_swarm(&mut shard, &mut out, &ids);
                (start.elapsed(), out)
            },
        ));
    }

    scenarios.push(bench("stop_all", MP_MEMBERSHIPS, || {
        timed(
            |s, o| run_multi_peer_join(s, o, &ids, 0),
            |s, o| run_stop_all(s, o, &ids),
        )
    }));
    scenarios.push(bench("disconnect_all", MP_CONNS, || {
        timed(
            |s, o| run_multi_peer_join(s, o, &ids, 0),
            run_disconnect_all,
        )
    }));
    scenarios.push(bench("expire_sweep", MP_MEMBERSHIPS, || {
        timed(
            |s, o| {
                run_multi_peer_join(s, o, &ids, 0);
                refresh_even_peers(s, o, &ids);
            },
            |s, o| {
                s.expire(EXPIRE_NOW, o);
            },
        )
    }));

    proto_scenarios(&ids, &mut scenarios);

    let mut scaling_results = scaling(&ids, Scaling::Strong);
    scaling_results.extend(scaling(&ids, Scaling::Weak));

    let result = json!({
        "runtime": "rust",
        "env": {
            "arch": std::env::consts::ARCH,
            "os": std::env::consts::OS,
            "parallelism": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        },
        "scenarios": scenarios,
        "scaling": scaling_results,
    });
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}

fn proto_scenarios(ids: &Ids, scenarios: &mut Vec<Value>) {
    let members = proto_memberships();
    let announces: Vec<(u64, Vec<u8>)> = members
        .iter()
        .enumerate()
        .map(|(n, &(c, p, s))| {
            (
                c as u64,
                announce_frame(&ids.swarms[s], &ids.peers[p], n).into_bytes(),
            )
        })
        .collect();
    let answers: Vec<(u64, Vec<u8>)> = members
        .iter()
        .enumerate()
        .map(|(n, &(c, p, s))| {
            let frame = answer_frame(
                &ids.swarms[s],
                &ids.peers[p],
                &ids.peers[answer_target(n)],
                n,
            );
            (c as u64, frame.into_bytes())
        })
        .collect();

    fn parse_bench<B: Backend>(name: &str, frames: &[(u64, Vec<u8>)]) -> Value {
        bench(name, PROTO_MSGS, || {
            let mut out = Counter::default();
            let start = Instant::now();
            for i in 0..PROTO_MSGS {
                let frame = &frames[i % frames.len()].1;
                std::hint::black_box(B::parse(frame).unwrap());
                out.bytes += frame.len() as u64;
            }
            (start.elapsed(), out)
        })
    }
    scenarios.push(parse_bench::<SerdeJson>(
        "proto_parse_announce_serde_json",
        &announces,
    ));
    scenarios.push(parse_bench::<Sonic>(
        "proto_parse_announce_sonic",
        &announces,
    ));

    // Encoding only: reply + 10 offers per announce, from pre-parsed frames.
    let parsed: Vec<Message<'_>> = announces
        .iter()
        .map(|(_, f)| SerdeJson::parse(f).unwrap())
        .collect();
    scenarios.push(bench("proto_encode_announce", PROTO_MSGS, || {
        let mut enc = Encoder::new();
        let mut out = Counter::default();
        let start = Instant::now();
        for i in 0..PROTO_MSGS {
            let Message::Announce {
                info_hash,
                peer_id,
                offers: Some(offers),
                ..
            } = &parsed[i % parsed.len()]
            else {
                unreachable!()
            };
            enc.clear();
            enc.announce_reply(ConnId(1), info_hash, 20, 1, 59);
            for offer in offers {
                enc.offer(ConnId(2), peer_id, info_hash, offer);
            }
            out.replies += 1;
            out.offers += offers.len() as u64;
            out.bytes += enc.bytes() as u64;
        }
        (start.elapsed(), out)
    }));

    fn pipeline<B: Backend>(
        name: &str,
        ids: &Ids,
        frames: &[(u64, Vec<u8>)],
        answers: bool,
    ) -> Value {
        let mut shard = new_shard();
        run_multi_peer_join(&mut shard, &mut Counter::default(), ids, 0);
        bench(name, PROTO_MSGS, || {
            let mut enc = Encoder::new();
            let mut out = Counter::default();
            let start = Instant::now();
            for i in 0..PROTO_MSGS {
                let (conn, frame) = &frames[i % frames.len()];
                enc.clear();
                wt_proto::handle_with::<B>(&mut shard, 0, ConnId(*conn), frame, &mut enc).unwrap();
                let messages = enc.messages().len() as u64;
                if answers {
                    out.answers += messages;
                } else {
                    out.replies += 1;
                    out.offers += messages - 1;
                }
                out.bytes += enc.bytes() as u64;
            }
            (start.elapsed(), out)
        })
    }
    scenarios.push(pipeline::<SerdeJson>(
        "pipeline_reannounce_serde_json",
        ids,
        &announces,
        false,
    ));
    scenarios.push(pipeline::<Sonic>(
        "pipeline_reannounce_sonic",
        ids,
        &announces,
        false,
    ));
    scenarios.push(pipeline::<SerdeJson>(
        "pipeline_answer_serde_json",
        ids,
        &answers,
        true,
    ));
    scenarios.push(pipeline::<Sonic>(
        "pipeline_answer_sonic",
        ids,
        &answers,
        true,
    ));
}
