mod common;

use std::collections::BTreeSet;

use common::{Event, OFFERS, Recorder, announce, announce_with, sorted_strings, stop, swarm_peers};
use wt_core::{
    AnnounceEvent, ConnId, MAX_KEY_LEN, OfferSelection, Request, ScrapeTarget, Settings, Shard,
    SwarmStats, TrackerError,
};

fn shard() -> Shard {
    Shard::new(Settings::default(), 7)
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn one_connection_many_peer_ids_in_many_swarms() {
    let mut shard = shard();
    let mut out = Recorder::default();

    for peer in ["a", "b", "c"] {
        for swarm in ["X", "Y"] {
            announce(
                &mut shard,
                &mut out,
                0,
                1,
                swarm,
                peer,
                AnnounceEvent::Started,
            )
            .unwrap();
        }
    }
    shard.check_invariants().unwrap();
    assert_eq!(
        sorted_strings(shard.connection_peer_ids(ConnId(1))),
        ["a", "b", "c"]
    );
    assert_eq!(sorted_strings(shard.peer_swarms(b"a").unwrap()), ["X", "Y"]);
    assert_eq!(swarm_peers(&shard, "X"), ["a", "b", "c"]);
    assert_eq!(swarm_peers(&shard, "Y"), ["a", "b", "c"]);
    assert_eq!(shard.membership_count(), 6);
    assert_eq!(shard.connection_count(), 1);
    out.take();

    // Leaving one swarm keeps the peer.
    stop(&mut shard, &mut out, 1, "X", "a");
    assert_eq!(swarm_peers(&shard, "X"), ["b", "c"]);
    assert_eq!(sorted_strings(shard.peer_swarms(b"a").unwrap()), ["Y"]);
    assert!(out.removed().is_empty());

    // Leaving the last swarm removes the peer.
    stop(&mut shard, &mut out, 1, "Y", "a");
    assert_eq!(shard.peer_swarms(b"a"), None);
    assert_eq!(out.removed(), [(b"a".to_vec(), ConnId(1))]);
    assert_eq!(
        sorted_strings(shard.connection_peer_ids(ConnId(1))),
        ["b", "c"]
    );
    shard.check_invariants().unwrap();
    out.take();

    // Disconnect removes every remaining peer of the connection.
    shard.disconnect(ConnId(1), &mut out);
    assert_eq!(
        out.removed(),
        [(b"b".to_vec(), ConnId(1)), (b"c".to_vec(), ConnId(1))]
    );
    assert_eq!(shard.swarm_count(), 0);
    assert_eq!(shard.peer_count(), 0);
    assert_eq!(shard.membership_count(), 0);
    assert_eq!(shard.connection_count(), 0);
    shard.check_invariants().unwrap();
}

#[test]
fn disconnect_leaves_other_connections_alone() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "b", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "Y", "c", AnnounceEvent::Started).unwrap();

    shard.disconnect(ConnId(1), &mut out);
    shard.disconnect(ConnId(99), &mut out); // unknown: no-op
    assert_eq!(swarm_peers(&shard, "X"), ["b"]);
    assert_eq!(swarm_peers(&shard, "Y"), ["c"]);
    shard.check_invariants().unwrap();
}

#[test]
fn peer_id_moving_to_another_connection_is_recreated() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 1, "Y", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 1, "X", "b", AnnounceEvent::Started).unwrap();
    out.take();

    announce(&mut shard, &mut out, 0, 2, "X", "a", AnnounceEvent::None).unwrap();
    assert_eq!(out.removed(), [(b"a".to_vec(), ConnId(1))]);
    assert_eq!(shard.peer_connection(b"a"), Some(ConnId(2)));
    assert_eq!(sorted_strings(shard.peer_swarms(b"a").unwrap()), ["X"]);
    assert_eq!(swarm_peers(&shard, "Y"), Vec::<String>::new());
    assert_eq!(sorted_strings(shard.connection_peer_ids(ConnId(1))), ["b"]);
    shard.check_invariants().unwrap();
}

#[test]
fn announce_reply_has_interval_and_counts() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(
        &mut shard,
        &mut out,
        0,
        1,
        "X",
        "a",
        AnnounceEvent::Completed,
    )
    .unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "b", AnnounceEvent::Started).unwrap();
    let reply = out
        .take()
        .into_iter()
        .rfind(|e| matches!(e, Event::Reply { .. }))
        .unwrap();
    assert_eq!(
        reply,
        Event::Reply {
            to: ConnId(2),
            info_hash: b"X".to_vec(),
            interval: 20,
            complete: 1,
            incomplete: 1,
        }
    );
}

#[test]
fn completed_is_counted_once_and_never_reverts() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(
        &mut shard,
        &mut out,
        0,
        1,
        "X",
        "a",
        AnnounceEvent::Completed,
    )
    .unwrap();
    announce(
        &mut shard,
        &mut out,
        0,
        1,
        "X",
        "a",
        AnnounceEvent::Completed,
    )
    .unwrap();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::None).unwrap();
    assert_eq!(
        shard.swarm_stats(b"X"),
        Some(SwarmStats {
            peers: 1,
            complete: 1
        })
    );

    // left == 0 also marks the peer completed.
    shard
        .handle(
            0,
            ConnId(2),
            Request::Announce {
                info_hash: b"X",
                peer_id: b"b",
                event: AnnounceEvent::None,
                left_zero: true,
                numwant: None,
                offers: None::<&[u32]>,
            },
            &mut out,
        )
        .unwrap();
    assert_eq!(
        shard.swarm_stats(b"X"),
        Some(SwarmStats {
            peers: 2,
            complete: 2
        })
    );

    stop(&mut shard, &mut out, 1, "X", "a");
    assert_eq!(
        shard.swarm_stats(b"X"),
        Some(SwarmStats {
            peers: 1,
            complete: 1
        })
    );
    shard.check_invariants().unwrap();
}

#[test]
fn offers_go_to_everyone_else_when_there_are_enough() {
    let mut shard = shard();
    let mut out = Recorder::default();
    for (conn, peer) in ["p0", "p1", "p2", "p3"].iter().enumerate() {
        announce_with(
            &mut shard,
            &mut out,
            0,
            conn as u64,
            "X",
            peer,
            AnnounceEvent::Started,
            None,
            None,
        )
        .unwrap();
    }
    out.take();

    announce(
        &mut shard,
        &mut out,
        0,
        10,
        "X",
        "me",
        AnnounceEvent::Started,
    )
    .unwrap();
    let offers = out.offers();
    let receivers: BTreeSet<_> = offers.iter().map(|(to, _)| *to).collect();
    assert_eq!(receivers, (0..4).map(ConnId).collect());
    assert_eq!(
        offers.iter().map(|(_, o)| *o).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert!(out.events.iter().all(|e| match e {
        Event::Offer {
            from, info_hash, ..
        } => from == b"me" && info_hash == b"X",
        _ => true,
    }));
}

fn swarm_of_30(offer_selection: OfferSelection) -> (Shard, Recorder) {
    let settings = Settings {
        offer_selection,
        ..Settings::default()
    };
    let mut shard = Shard::new(settings, 3);
    let mut out = Recorder::default();
    for conn in 0..30u64 {
        announce_with(
            &mut shard,
            &mut out,
            0,
            conn,
            "X",
            &format!("p{conn}"),
            AnnounceEvent::Started,
            None,
            None,
        )
        .unwrap();
    }
    out.take();
    (shard, out)
}

fn check_partial_fan_out(offer_selection: OfferSelection) {
    let (mut shard, mut out) = swarm_of_30(offer_selection);
    let mut all_receivers = BTreeSet::new();
    for round in 0..100 {
        out.take();
        // "p5" lives on connection 5.
        announce(&mut shard, &mut out, 0, 5, "X", "p5", AnnounceEvent::None).unwrap();
        let offers = out.offers();
        assert_eq!(offers.len(), 10, "round {round}");
        assert_eq!(offers.iter().map(|(_, o)| *o).collect::<Vec<_>>(), OFFERS);
        let receivers: BTreeSet<_> = offers.iter().map(|(to, _)| *to).collect();
        assert_eq!(receivers.len(), 10, "receivers must be distinct");
        assert!(!receivers.contains(&ConnId(5)), "no offer to self");
        all_receivers.extend(receivers);
    }
    assert_eq!(all_receivers.len(), 29, "every other peer is reached");
}

#[test]
fn partial_fan_out_random_window() {
    check_partial_fan_out(OfferSelection::RandomWindow);
}

#[test]
fn partial_fan_out_round_robin() {
    check_partial_fan_out(OfferSelection::RoundRobin);
}

#[test]
fn partial_fan_out_random_sample() {
    check_partial_fan_out(OfferSelection::RandomSample);
}

/// How often two neighbouring swarm entries (p10 and p11, i.e. peers that joined one after the
/// other) receive offers from the same announce.
fn neighbour_co_selection(offer_selection: OfferSelection) -> f64 {
    const ROUNDS: usize = 20_000;
    let (mut shard, mut out) = swarm_of_30(offer_selection);
    let mut together = 0;
    let mut either = 0;
    for _ in 0..ROUNDS {
        out.take();
        announce(&mut shard, &mut out, 0, 5, "X", "p5", AnnounceEvent::None).unwrap();
        let receivers: BTreeSet<_> = out.offers().iter().map(|(to, _)| *to).collect();
        let (a, b) = (
            receivers.contains(&ConnId(10)),
            receivers.contains(&ConnId(11)),
        );
        together += (a && b) as usize;
        either += (a || b) as usize;
    }
    together as f64 / either as f64
}

#[test]
fn random_sample_does_not_cluster_neighbours() {
    // 10 receivers of 29 others. Window: a neighbour pair is almost always picked together.
    // Uniform sample: P(both | either) = (10·9)/(29·28) / (1 − (19·18)/(29·28)) ≈ 0.20.
    let window = neighbour_co_selection(OfferSelection::RandomWindow);
    let sample = neighbour_co_selection(OfferSelection::RandomSample);
    let round_robin = neighbour_co_selection(OfferSelection::RoundRobin);
    eprintln!(
        "neighbour co-selection: window {window:.3}, round-robin {round_robin:.3}, sample {sample:.3}"
    );
    assert!(window > 0.8, "window co-selection {window}");
    assert!((sample - 0.20).abs() < 0.03, "sample co-selection {sample}");
}

#[test]
fn offer_count_is_capped() {
    let settings = Settings {
        max_offers: 3,
        ..Settings::default()
    };
    let mut shard = Shard::new(settings, 1);
    let mut out = Recorder::default();
    for conn in 0..20u64 {
        announce_with(
            &mut shard,
            &mut out,
            0,
            conn,
            "X",
            &format!("p{conn}"),
            AnnounceEvent::Started,
            None,
            None,
        )
        .unwrap();
    }

    let mut count = |offers: Option<&[u32]>, numwant: Option<u32>| {
        out.take();
        announce_with(
            &mut shard,
            &mut out,
            0,
            0,
            "X",
            "p0",
            AnnounceEvent::None,
            offers,
            numwant,
        )
        .unwrap();
        out.offers().len()
    };
    assert_eq!(count(Some(&OFFERS), Some(100)), 3); // max_offers
    assert_eq!(count(Some(&OFFERS), Some(2)), 2); // numwant
    assert_eq!(count(Some(&OFFERS[..1]), Some(100)), 1); // offers available
    assert_eq!(count(Some(&OFFERS), Some(0)), 0);
    assert_eq!(count(Some(&OFFERS), None), 0); // numwant missing / not an integer
    assert_eq!(count(None, Some(100)), 0); // no offers
}

#[test]
fn no_offers_in_a_swarm_of_one() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    assert!(out.offers().is_empty());
}

/// An answer from `conn` as `from` to `to` in swarm `info_hash`.
fn answer(shard: &mut Shard, out: &mut Recorder, conn: u64, info_hash: &str, from: &str, to: &str) {
    shard
        .handle(
            0,
            ConnId(conn),
            Request::Answer {
                info_hash: info_hash.as_bytes(),
                peer_id: from.as_bytes(),
                to_peer_id: to.as_bytes(),
                answer: &42u32,
            },
            out,
        )
        .unwrap();
}

/// Spec §5.3: delivered only from a peer of the requesting connection to a peer, both in the
/// answer's swarm; anything else is dropped (counted), never an error.
#[test]
fn answer_goes_only_between_members_of_its_swarm() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "b", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 3, "Y", "c", AnnounceEvent::Started).unwrap();
    out.take();

    answer(&mut shard, &mut out, 2, "X", "b", "a");
    assert_eq!(
        out.take(),
        [Event::Answer {
            to: ConnId(1),
            answer: 42
        }]
    );
    for (conn, info_hash, from, to, why) in [
        (2, "Z", "b", "a", "unknown swarm"),
        (2, "X", "nobody", "a", "unknown sender"),
        (1, "X", "b", "a", "sender of another connection"),
        (3, "X", "c", "a", "sender not in the swarm"),
        (2, "X", "b", "c", "target not in the swarm"),
        (2, "X", "b", "zzz", "unknown target"),
    ] {
        answer(&mut shard, &mut out, conn, info_hash, from, to);
        assert_eq!(out.take(), [Event::AnswerDropped], "{why}");
    }
    shard.check_invariants().unwrap();
}

/// Spec §5.4: only the peer's own connection can stop it.
#[test]
fn stop_from_another_connection_is_ignored() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "b", AnnounceEvent::Started).unwrap();
    out.take();
    stop(&mut shard, &mut out, 2, "X", "a");
    assert_eq!(swarm_peers(&shard, "X"), ["a", "b"]);
    assert!(out.events.is_empty());
    stop(&mut shard, &mut out, 1, "X", "a");
    assert_eq!(swarm_peers(&shard, "X"), ["b"]);
    shard.check_invariants().unwrap();
}

/// A peer_id that moved to a new connection: a late stop from the old one is ignored.
#[test]
fn late_stop_from_a_previous_connection_is_ignored() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "a", AnnounceEvent::Started).unwrap();
    stop(&mut shard, &mut out, 1, "X", "a");
    assert_eq!(swarm_peers(&shard, "X"), ["a"]);
    assert_eq!(shard.peer_connection(b"a"), Some(ConnId(2)));
    shard.check_invariants().unwrap();
}

#[test]
fn stop_of_unknown_swarm_or_peer_is_a_no_op() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 1, "Y", "b", AnnounceEvent::Started).unwrap();
    stop(&mut shard, &mut out, 1, "Z", "a");
    stop(&mut shard, &mut out, 1, "X", "zzz");
    stop(&mut shard, &mut out, 1, "Y", "a"); // peer exists, but not in that swarm
    assert_eq!(swarm_peers(&shard, "X"), ["a"]);
    assert_eq!(swarm_peers(&shard, "Y"), ["b"]);
    shard.check_invariants().unwrap();
}

#[test]
fn expire_removes_stale_memberships_and_empty_peers() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 1, "Y", "a", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "b", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 30, 1, "X", "a", AnnounceEvent::None).unwrap();
    out.take();

    // Timeout is 2 × 20s, strictly greater.
    assert_eq!(shard.expire(40, &mut out), 0);
    assert_eq!(shard.expire(41, &mut out), 2);
    assert_eq!(out.removed(), [(b"b".to_vec(), ConnId(2))]);
    assert_eq!(swarm_peers(&shard, "X"), ["a"]);
    assert_eq!(shard.swarm_stats(b"Y"), None);
    assert_eq!(sorted_strings(shard.peer_swarms(b"a").unwrap()), ["X"]);
    shard.check_invariants().unwrap();

    assert_eq!(shard.expire(71, &mut out), 1);
    assert_eq!(shard.peer_count(), 0);
    assert_eq!(shard.connection_count(), 0);
    shard.check_invariants().unwrap();
}

#[test]
fn scrape_all_one_and_many() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(
        &mut shard,
        &mut out,
        0,
        1,
        "X",
        "a",
        AnnounceEvent::Completed,
    )
    .unwrap();
    announce(&mut shard, &mut out, 0, 2, "X", "b", AnnounceEvent::Started).unwrap();
    announce(&mut shard, &mut out, 0, 2, "Y", "b", AnnounceEvent::Started).unwrap();
    out.take();

    let mut scrape = |target: ScrapeTarget<'_>| {
        shard
            .handle(0, ConnId(9), Request::Scrape::<u32> { target }, &mut out)
            .unwrap();
        out.take()
    };
    let entry = |info_hash: &[u8], complete, incomplete| Event::ScrapeEntry {
        to: ConnId(9),
        info_hash: info_hash.to_vec(),
        complete,
        incomplete,
        downloaded: complete,
    };
    let end = Event::ScrapeEnd { to: ConnId(9) };

    let mut all = scrape(ScrapeTarget::All);
    all.sort_by_key(|e| format!("{e:?}"));
    assert_eq!(all, [end.clone(), entry(b"X", 1, 1), entry(b"Y", 0, 1)]);

    assert_eq!(
        scrape(ScrapeTarget::One(b"X")),
        [entry(b"X", 1, 1), end.clone()]
    );
    assert_eq!(
        scrape(ScrapeTarget::One(b"nope")),
        [entry(b"nope", 0, 0), end.clone()]
    );
    assert_eq!(
        scrape(ScrapeTarget::Many(&[b"Y", b"nope"])),
        [entry(b"Y", 0, 1), entry(b"nope", 0, 0), end.clone()]
    );
}

#[test]
fn keys_longer_than_max_are_rejected() {
    let mut shard = shard();
    let mut out = Recorder::default();
    let ok = "x".repeat(MAX_KEY_LEN);
    let long = "x".repeat(MAX_KEY_LEN + 1);
    announce(&mut shard, &mut out, 0, 1, &ok, &ok, AnnounceEvent::Started).unwrap();
    assert_eq!(
        announce(
            &mut shard,
            &mut out,
            0,
            1,
            &long,
            "a",
            AnnounceEvent::Started
        ),
        Err(TrackerError::KeyTooLong)
    );
    assert_eq!(
        announce(
            &mut shard,
            &mut out,
            0,
            1,
            "X",
            &long,
            AnnounceEvent::Started
        ),
        Err(TrackerError::KeyTooLong)
    );
    assert_eq!(swarm_peers(&shard, &ok), ids(&[&ok]));
    assert_eq!(shard.swarm_count(), 1);
    shard.check_invariants().unwrap();
}

#[test]
fn swarm_is_deleted_when_last_peer_leaves_and_recreated_on_join() {
    let mut shard = shard();
    let mut out = Recorder::default();
    announce(
        &mut shard,
        &mut out,
        0,
        1,
        "X",
        "a",
        AnnounceEvent::Completed,
    )
    .unwrap();
    stop(&mut shard, &mut out, 1, "X", "a");
    assert_eq!(shard.swarm_count(), 0);
    announce(&mut shard, &mut out, 0, 1, "X", "a", AnnounceEvent::Started).unwrap();
    assert_eq!(
        shard.swarm_stats(b"X"),
        Some(SwarmStats {
            peers: 1,
            complete: 0
        })
    );
    shard.check_invariants().unwrap();
}

#[test]
fn settings_defaults_match_spec() {
    let s = Settings::default();
    assert_eq!(s.max_offers, 20);
    assert_eq!(s.announce_interval, 20);
    assert_eq!(s.offer_selection, OfferSelection::RandomSample);
}
