//! Benchmark scenarios shared by the timing (`wt-bench`) and memory (`wt-bench-mem`) binaries.
//! Every scenario has a twin in `bench/js/bench.ts` with identical parameters.

use std::hint::black_box;

use wt_core::{AnnounceEvent, ConnId, Key, Outbox, Request, Settings, Shard};

pub const OFFERS_PER_ANNOUNCE: usize = 10;
pub const NUMWANT: u32 = 10;

/// Opaque offer payload (the real server will carry `Bytes` slices here).
#[derive(Clone, Copy)]
pub struct Offer(pub u32);

pub static OFFERS: [Offer; OFFERS_PER_ANNOUNCE] = {
    let mut offers = [Offer(0); OFFERS_PER_ANNOUNCE];
    let mut i = 0;
    while i < OFFERS_PER_ANNOUNCE {
        offers[i] = Offer(i as u32);
        i += 1;
    }
    offers
};

/// Counts outgoing messages (same as the JS benchmark's `sendMessage`).
#[derive(Default, Clone, Copy, Debug)]
pub struct Counter {
    pub replies: u64,
    pub offers: u64,
    pub answers: u64,
    pub removed: u64,
    /// Protocol scenarios: bytes parsed (parse) or sent (encode, pipeline).
    pub bytes: u64,
}

impl Outbox<Offer> for Counter {
    #[inline]
    fn announce_reply(&mut self, to: ConnId, _: &Key, _: u32, _: u32, _: u32) {
        black_box(to);
        self.replies += 1;
    }

    #[inline]
    fn offer(&mut self, to: ConnId, _: &Key, _: &Key, offer: &Offer) {
        black_box((to, offer));
        self.offers += 1;
    }

    #[inline]
    fn answer(&mut self, to: ConnId, answer: &Offer) {
        black_box((to, answer));
        self.answers += 1;
    }

    #[inline]
    fn peer_removed(&mut self, _: &Key, conn: ConnId) {
        black_box(conn);
        self.removed += 1;
    }
}

/// 20-byte ids, identical to the JS benchmark: prefix + 19-digit zero-padded index.
pub fn make_ids(prefix: char, count: usize) -> Vec<[u8; 20]> {
    (0..count)
        .map(|i| {
            let s = format!("{prefix}{i:019}");
            s.as_bytes().try_into().unwrap()
        })
        .collect()
}

pub struct Ids {
    pub peers: Vec<[u8; 20]>,
    pub swarms: Vec<[u8; 20]>,
}

impl Ids {
    pub fn new(peers: usize, swarms: usize) -> Self {
        Self {
            peers: make_ids('p', peers),
            swarms: make_ids('h', swarms),
        }
    }
}

pub fn new_shard() -> Shard {
    new_shard_with(Settings::default())
}

pub fn new_shard_with(settings: Settings) -> Shard {
    Shard::new(settings, 0x5eed)
}

#[inline]
#[allow(clippy::too_many_arguments)]
pub fn announce(
    shard: &mut Shard,
    out: &mut Counter,
    now: u32,
    conn: u64,
    info_hash: &[u8],
    peer_id: &[u8],
    event: AnnounceEvent,
    offers: bool,
) {
    shard
        .handle(
            now,
            ConnId(conn),
            Request::Announce {
                info_hash,
                peer_id,
                event,
                left_zero: false,
                numwant: Some(NUMWANT),
                offers: offers.then_some(&OFFERS[..]),
            },
            out,
        )
        .unwrap();
}

// ---- scenario shapes ----

/// #1: N peers (one connection each) join a single swarm.
pub const ONE_SWARM_PEERS: usize = 100_000;

/// #2: N peers (one connection each) join M swarms, `peer % M`.
pub const MANY_SWARMS_PEERS: usize = 1_000_000;
pub const MANY_SWARMS_SWARMS: usize = 100_000;

/// #3: C connections × K peer_ids × S swarms each, out of W swarms.
pub const MP_CONNS: usize = 100_000;
pub const MP_PEERS_PER_CONN: usize = 3;
pub const MP_SWARMS_PER_PEER: usize = 2;
pub const MP_SWARMS: usize = 10_000;
pub const MP_PEERS: usize = MP_CONNS * MP_PEERS_PER_CONN;
pub const MP_MEMBERSHIPS: usize = MP_PEERS * MP_SWARMS_PER_PEER;

/// Swarm of membership `j` of peer `p` in the multi-peer scenario.
#[inline]
pub fn mp_swarm(p: usize, j: usize) -> usize {
    (p * 7 + j * 5003) % MP_SWARMS
}

/// Iterates the multi-peer scenario: `(conn, peer, swarm)` for every membership.
pub fn mp_memberships() -> impl Iterator<Item = (usize, usize, usize)> {
    (0..MP_CONNS).flat_map(|c| {
        (0..MP_PEERS_PER_CONN).flat_map(move |k| {
            let p = c * MP_PEERS_PER_CONN + k;
            (0..MP_SWARMS_PER_PEER).map(move |j| (c, p, mp_swarm(p, j)))
        })
    })
}

/// Memberships owned by shard `shard` of `shards` (routing by `swarm % shards`), for the
/// strong-scaling run: one fixed workload split across shards.
pub fn mp_memberships_of_shard(shard: usize, shards: usize) -> Vec<(u32, u32, u32)> {
    mp_memberships()
        .filter(|&(_, _, s)| s % shards == shard)
        .map(|(c, p, s)| (c as u32, p as u32, s as u32))
        .collect()
}

pub fn run_announce_list(
    shard: &mut Shard,
    out: &mut Counter,
    ids: &Ids,
    list: &[(u32, u32, u32)],
    event: AnnounceEvent,
) {
    for &(c, p, s) in list {
        announce(
            shard,
            out,
            0,
            c as u64,
            &ids.swarms[s as usize],
            &ids.peers[p as usize],
            event,
            true,
        );
    }
}

pub fn run_join_one_swarm(shard: &mut Shard, out: &mut Counter, ids: &Ids) {
    let info_hash = &ids.swarms[0];
    for (i, peer_id) in ids.peers[..ONE_SWARM_PEERS].iter().enumerate() {
        announce(
            shard,
            out,
            0,
            i as u64,
            info_hash,
            peer_id,
            AnnounceEvent::Started,
            true,
        );
    }
}

/// Every peer of the single-swarm state of #1 re-announces with offers.
pub fn run_reannounce_one_swarm(shard: &mut Shard, out: &mut Counter, ids: &Ids) {
    let info_hash = &ids.swarms[0];
    for (i, peer_id) in ids.peers[..ONE_SWARM_PEERS].iter().enumerate() {
        announce(
            shard,
            out,
            0,
            i as u64,
            info_hash,
            peer_id,
            AnnounceEvent::None,
            true,
        );
    }
}

pub fn run_join_many_swarms(shard: &mut Shard, out: &mut Counter, ids: &Ids) {
    for (i, peer_id) in ids.peers[..MANY_SWARMS_PEERS].iter().enumerate() {
        let info_hash = &ids.swarms[i % MANY_SWARMS_SWARMS];
        announce(
            shard,
            out,
            0,
            i as u64,
            info_hash,
            peer_id,
            AnnounceEvent::Started,
            true,
        );
    }
}

/// #3 and the setup of #4–#8.
pub fn run_multi_peer_join(shard: &mut Shard, out: &mut Counter, ids: &Ids, now: u32) {
    for (c, p, s) in mp_memberships() {
        announce(
            shard,
            out,
            now,
            c as u64,
            &ids.swarms[s],
            &ids.peers[p],
            AnnounceEvent::Started,
            true,
        );
    }
}

/// #4: every membership re-announces (no event) with offers.
pub fn run_reannounce(shard: &mut Shard, out: &mut Counter, ids: &Ids, now: u32) {
    for (c, p, s) in mp_memberships() {
        announce(
            shard,
            out,
            now,
            c as u64,
            &ids.swarms[s],
            &ids.peers[p],
            AnnounceEvent::None,
            true,
        );
    }
}

pub const ANSWERS: usize = 1_000_000;

/// #5: answers between pseudo-random peers of one swarm, each from the sender's connection
/// (all delivered, spec §5.3).
pub fn run_answers(shard: &mut Shard, out: &mut Counter, ids: &Ids) {
    let answer = Offer(1);
    for i in 0..ANSWERS {
        let (c, from, to, s) = answer_pair(i);
        shard
            .handle(
                0,
                ConnId(c as u64),
                Request::Answer {
                    info_hash: &ids.swarms[s],
                    peer_id: &ids.peers[from],
                    to_peer_id: &ids.peers[to],
                    answer: &answer,
                },
                out,
            )
            .unwrap();
    }
}

/// #6: every membership stops.
pub fn run_stop_all(shard: &mut Shard, out: &mut Counter, ids: &Ids) {
    for (c, p, s) in mp_memberships() {
        shard
            .handle(
                0,
                ConnId(c as u64),
                Request::Stop::<Offer> {
                    info_hash: &ids.swarms[s],
                    peer_id: &ids.peers[p],
                },
                out,
            )
            .unwrap();
    }
}

/// #7: every connection disconnects.
pub fn run_disconnect_all(shard: &mut Shard, out: &mut Counter) {
    for c in 0..MP_CONNS {
        shard.disconnect(ConnId(c as u64), out);
    }
}

/// #8 setup: memberships of even peers are refreshed at t=30 (no offers).
pub fn refresh_even_peers(shard: &mut Shard, out: &mut Counter, ids: &Ids) {
    for (c, p, s) in mp_memberships() {
        if p % 2 == 0 {
            announce(
                shard,
                out,
                30,
                c as u64,
                &ids.swarms[s],
                &ids.peers[p],
                AnnounceEvent::None,
                false,
            );
        }
    }
}

/// #8: one sweep at t=45: odd peers (idle 45s > 40s) are removed.
pub const EXPIRE_NOW: u32 = 45;

// ---- protocol scenarios (frames are byte-identical to bench/js) ----

/// Realistic WebRTC data-channel offer (CRLF lines); `{session}` varies per frame.
pub const SDP_FIXTURE: &str = include_str!("../../../bench/fixtures/offer.sdp");
/// Distinct frames per protocol scenario, cycled through.
pub const PROTO_FRAMES: usize = 1000;
/// Messages per protocol scenario run.
pub const PROTO_MSGS: usize = 20_000;

pub fn sdp_json(n: usize) -> String {
    serde_json::to_string(&SDP_FIXTURE.replace("{session}", &format!("{n:019}"))).unwrap()
}

fn id_str(id: &[u8; 20]) -> &str {
    std::str::from_utf8(id).unwrap()
}

/// Re-announce of a member of `multi_peer_join` with 10 offers.
pub fn announce_frame(info_hash: &[u8; 20], peer_id: &[u8; 20], n: usize) -> String {
    let offers: Vec<String> = (0..OFFERS_PER_ANNOUNCE)
        .map(|k| {
            format!(
                r#"{{"offer":{{"type":"offer","sdp":{}}},"offer_id":"o{:09}{k:010}"}}"#,
                sdp_json(n * OFFERS_PER_ANNOUNCE + k),
                n
            )
        })
        .collect();
    format!(
        r#"{{"action":"announce","info_hash":"{}","peer_id":"{}","numwant":{NUMWANT},"uploaded":0,"downloaded":0,"offers":[{}]}}"#,
        id_str(info_hash),
        id_str(peer_id),
        offers.join(",")
    )
}

pub fn answer_frame(
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    to_peer_id: &[u8; 20],
    n: usize,
) -> String {
    format!(
        r#"{{"action":"announce","info_hash":"{}","peer_id":"{}","to_peer_id":"{}","answer":{{"type":"answer","sdp":{}}},"offer_id":"o{n:019}"}}"#,
        id_str(info_hash),
        id_str(peer_id),
        id_str(to_peer_id),
        sdp_json(n)
    )
}

/// `PROTO_FRAMES` memberships spread over `multi_peer_join`: `(conn, peer, swarm)`.
pub fn proto_memberships() -> Vec<(usize, usize, usize)> {
    mp_memberships()
        .step_by(MP_MEMBERSHIPS / PROTO_FRAMES)
        .take(PROTO_FRAMES)
        .collect()
}

/// Answer `n` of the multi-peer scenario: `(conn, from, to, swarm)`. `to` is a pseudo-random
/// peer, `swarm` its first swarm, and `from` the peer `MP_SWARMS` further on: `mp_swarm(p, 0)`
/// depends only on `p % MP_SWARMS` (which divides `MP_PEERS`), so both are members of it.
pub fn answer_pair(n: usize) -> (usize, usize, usize, usize) {
    let to = (n * 2_654_435_761) % MP_PEERS;
    let from = (to + MP_SWARMS) % MP_PEERS;
    (from / MP_PEERS_PER_CONN, from, to, mp_swarm(to, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answer_pairs_are_members_of_one_swarm_on_the_senders_connection() {
        for n in [0, 1, 12_345, ANSWERS - 1] {
            let (c, from, to, s) = answer_pair(n);
            assert_ne!(from, to);
            assert_eq!(c, from / MP_PEERS_PER_CONN);
            let swarms = |p: usize| (0..MP_SWARMS_PER_PEER).map(move |j| mp_swarm(p, j));
            assert!(swarms(from).any(|x| x == s) && swarms(to).any(|x| x == s));
        }
        // Same as `answerPair(12345)` in bench/js/scenarios.ts.
        assert_eq!(answer_pair(12_345), (59_848, 179_545, 169_545, 6_815));
    }
}
