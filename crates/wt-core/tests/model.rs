//! Model-based test: random operation sequences against a naive reference model.

mod common;

use std::collections::BTreeMap;

use common::{Event, Recorder};
use proptest::prelude::*;
use wt_core::{AnnounceEvent, ConnId, OfferSelection, Request, Settings, Shard};

const CONNS: u8 = 4;
const PEERS: u8 = 6;
const SWARMS: u8 = 3;
const INTERVAL: u32 = 20;

#[derive(Clone, Debug)]
enum Op {
    Announce {
        conn: u8,
        peer: u8,
        swarm: u8,
        completed: bool,
    },
    Stop {
        conn: u8,
        peer: u8,
        swarm: u8,
    },
    Answer {
        conn: u8,
        from: u8,
        to: u8,
        swarm: u8,
    },
    Disconnect {
        conn: u8,
    },
    Advance {
        secs: u8,
    },
    Expire,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..CONNS, 0..PEERS, 0..SWARMS, any::<bool>())
            .prop_map(|(conn, peer, swarm, completed)| Op::Announce { conn, peer, swarm, completed }),
        2 => (0..CONNS, 0..PEERS, 0..SWARMS).prop_map(|(conn, peer, swarm)| Op::Stop { conn, peer, swarm }),
        2 => (0..CONNS, 0..PEERS, 0..PEERS, 0..SWARMS)
            .prop_map(|(conn, from, to, swarm)| Op::Answer { conn, from, to, swarm }),
        1 => (0..CONNS).prop_map(|conn| Op::Disconnect { conn }),
        1 => (0..30u8).prop_map(|secs| Op::Advance { secs }),
        1 => Just(Op::Expire),
    ]
}

#[derive(Default)]
struct ModelPeer {
    conn: u8,
    /// swarm → (last_seen, completed)
    swarms: BTreeMap<u8, (u32, bool)>,
}

#[derive(Default)]
struct Model {
    now: u32,
    peers: BTreeMap<u8, ModelPeer>,
    removed: Vec<(Vec<u8>, ConnId)>,
}

impl Model {
    fn remove_peer(&mut self, peer: u8) {
        let p = self.peers.remove(&peer).unwrap();
        self.removed.push((peer_id(peer), ConnId(p.conn as u64)));
    }

    fn apply(&mut self, op: &Op) {
        match *op {
            Op::Announce {
                conn,
                peer,
                swarm,
                completed,
            } => {
                if self.peers.get(&peer).is_some_and(|p| p.conn != conn) {
                    self.remove_peer(peer);
                }
                let now = self.now;
                let p = self.peers.entry(peer).or_insert_with(|| ModelPeer {
                    conn,
                    ..Default::default()
                });
                let entry = p.swarms.entry(swarm).or_insert((now, false));
                entry.0 = now;
                entry.1 |= completed;
            }
            // Only the peer's own connection can stop it (spec §5.4).
            Op::Stop { conn, peer, swarm } => {
                if let Some(p) = self.peers.get_mut(&peer)
                    && p.conn == conn
                    && p.swarms.remove(&swarm).is_some()
                    && p.swarms.is_empty()
                {
                    self.remove_peer(peer);
                }
            }
            Op::Answer { .. } => {}
            Op::Disconnect { conn } => {
                let gone: Vec<u8> = self
                    .peers
                    .iter()
                    .filter(|(_, p)| p.conn == conn)
                    .map(|(&id, _)| id)
                    .collect();
                for peer in gone {
                    self.remove_peer(peer);
                }
            }
            Op::Advance { secs } => self.now += secs as u32,
            Op::Expire => {
                let now = self.now;
                let mut empty = Vec::new();
                for (&id, p) in &mut self.peers {
                    p.swarms
                        .retain(|_, (last_seen, _)| now.saturating_sub(*last_seen) <= 2 * INTERVAL);
                    if p.swarms.is_empty() {
                        empty.push(id);
                    }
                }
                for peer in empty {
                    self.remove_peer(peer);
                }
            }
        }
        self.removed.sort();
    }

    /// Where an answer goes (spec §5.3): the target's connection if the sender is a peer of
    /// `conn` in `swarm` and the target is in `swarm` too; `None`: dropped.
    fn answer_target(&self, conn: u8, from: u8, to: u8, swarm: u8) -> Option<ConnId> {
        let from = self.peers.get(&from)?;
        let to = self.peers.get(&to)?;
        (from.conn == conn && from.swarms.contains_key(&swarm) && to.swarms.contains_key(&swarm))
            .then_some(ConnId(to.conn as u64))
    }

    /// swarm → (sorted peer_ids, completed count)
    fn swarms(&self) -> BTreeMap<u8, (Vec<Vec<u8>>, u32)> {
        let mut swarms: BTreeMap<u8, (Vec<Vec<u8>>, u32)> = BTreeMap::new();
        for (&id, p) in &self.peers {
            for (&s, &(_, completed)) in &p.swarms {
                let e = swarms.entry(s).or_default();
                e.0.push(peer_id(id));
                e.1 += completed as u32;
            }
        }
        swarms
    }
}

fn peer_id(peer: u8) -> Vec<u8> {
    format!("peer{peer}").into_bytes()
}

fn info_hash(swarm: u8) -> Vec<u8> {
    format!("swarm{swarm}").into_bytes()
}

fn apply_to_shard(shard: &mut Shard, now: u32, op: &Op, out: &mut Recorder) {
    match *op {
        Op::Announce {
            conn,
            peer,
            swarm,
            completed,
        } => {
            let event = if completed {
                AnnounceEvent::Completed
            } else {
                AnnounceEvent::None
            };
            shard
                .handle(
                    now,
                    ConnId(conn as u64),
                    Request::Announce {
                        info_hash: &info_hash(swarm),
                        peer_id: &peer_id(peer),
                        event,
                        left_zero: false,
                        numwant: Some(5),
                        offers: Some(&[1u32, 2, 3, 4, 5]),
                    },
                    out,
                )
                .unwrap();
        }
        Op::Stop { conn, peer, swarm } => {
            shard
                .handle(
                    now,
                    ConnId(conn as u64),
                    Request::Stop::<u32> {
                        info_hash: &info_hash(swarm),
                        peer_id: &peer_id(peer),
                    },
                    out,
                )
                .unwrap();
        }
        Op::Answer {
            conn,
            from,
            to,
            swarm,
        } => {
            shard
                .handle(
                    now,
                    ConnId(conn as u64),
                    Request::Answer {
                        info_hash: &info_hash(swarm),
                        peer_id: &peer_id(from),
                        to_peer_id: &peer_id(to),
                        answer: &7u32,
                    },
                    out,
                )
                .unwrap();
        }
        Op::Disconnect { conn } => shard.disconnect(ConnId(conn as u64), out),
        Op::Advance { .. } => {}
        Op::Expire => {
            shard.expire(now, out);
        }
    }
}

fn check(shard: &Shard, model: &Model, out: &Recorder, op: &Op) -> Result<(), TestCaseError> {
    shard.check_invariants().map_err(TestCaseError::fail)?;
    prop_assert_eq!(
        &out.removed(),
        &model.removed,
        "removed peers after {:?}",
        op
    );

    let expected = model.swarms();
    prop_assert_eq!(shard.swarm_count(), expected.len());
    for s in 0..SWARMS {
        let actual = shard.swarm_peer_ids(&info_hash(s)).map(|ids| {
            let mut ids: Vec<Vec<u8>> = ids.iter().map(|k| k.as_bytes().to_vec()).collect();
            ids.sort();
            (ids, shard.swarm_stats(&info_hash(s)).unwrap().complete)
        });
        prop_assert_eq!(
            actual.as_ref(),
            expected.get(&s),
            "swarm {} after {:?}",
            s,
            op
        );
    }
    for peer in 0..PEERS {
        let expected_conn = model.peers.get(&peer).map(|p| ConnId(p.conn as u64));
        prop_assert_eq!(shard.peer_connection(&peer_id(peer)), expected_conn);
    }

    if let Op::Answer {
        conn,
        from,
        to,
        swarm,
    } = *op
    {
        let expected = match model.answer_target(conn, from, to, swarm) {
            Some(to) => Event::Answer { to, answer: 7 },
            None => Event::AnswerDropped,
        };
        prop_assert_eq!(&out.events, &vec![expected], "after {:?}", op);
    }

    // Offers of an announce: right count, distinct receivers in the swarm, never to self.
    if let Op::Announce { conn, swarm, .. } = *op {
        let peers_in_swarm = expected[&swarm].0.len();
        let offers = out.offers();
        prop_assert_eq!(offers.len(), (peers_in_swarm - 1).min(5));
        let swarm_conns: Vec<ConnId> = model
            .peers
            .values()
            .filter(|p| p.swarms.contains_key(&swarm))
            .map(|p| ConnId(p.conn as u64))
            .collect();
        for (to, _) in &offers {
            prop_assert!(swarm_conns.contains(to));
        }
        let own_conn_peers = swarm_conns
            .iter()
            .filter(|c| **c == ConnId(conn as u64))
            .count();
        let to_own_conn = offers
            .iter()
            .filter(|(to, _)| *to == ConnId(conn as u64))
            .count();
        // Offers may reach the announcer's connection only via *other* peer_ids on it.
        prop_assert!(to_own_conn < own_conn_peers.max(1));
        let reply = out.events.iter().find(|e| matches!(e, Event::Reply { .. }));
        let (_, completed) = &expected[&swarm];
        prop_assert_eq!(
            reply,
            Some(&Event::Reply {
                to: ConnId(conn as u64),
                info_hash: info_hash(swarm),
                interval: INTERVAL,
                complete: *completed,
                incomplete: peers_in_swarm as u32 - completed,
            })
        );
    }
    Ok(())
}

fn run(ops: &[Op], offer_selection: OfferSelection) -> Result<(), TestCaseError> {
    let settings = Settings {
        max_offers: 20,
        announce_interval: INTERVAL,
        offer_selection,
    };
    let mut shard = Shard::new(settings, 42);
    let mut model = Model::default();
    for op in ops {
        let mut out = Recorder::default();
        model.removed.clear();
        model.apply(op);
        apply_to_shard(&mut shard, model.now, op, &mut out);
        check(&shard, &model, &out, op)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn shard_matches_model_random(ops in prop::collection::vec(op(), 1..200)) {
        run(&ops, OfferSelection::RandomWindow)?;
    }

    #[test]
    fn shard_matches_model_round_robin(ops in prop::collection::vec(op(), 1..200)) {
        run(&ops, OfferSelection::RoundRobin)?;
    }

    #[test]
    fn shard_matches_model_random_sample(ops in prop::collection::vec(op(), 1..200)) {
        run(&ops, OfferSelection::RandomSample)?;
    }
}
