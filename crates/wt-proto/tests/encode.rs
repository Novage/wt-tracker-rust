//! Output bytes must equal the JS tracker's `JSON.stringify` output (spec §7.2). The expected
//! strings were produced by node from the same message objects.

mod common;

use common::{HandleFn, backends, texts};
use wt_core::{ConnId, OfferSelection, Settings, Shard};
use wt_proto::{Encoder, ProtoError};

/// `~u` stands for a JSON `\u` escape (keeps the escapes literal in source).
fn j(s: &str) -> String {
    s.replace("~u", "\\u")
}

fn shard() -> Shard {
    Shard::new(
        Settings {
            offer_selection: OfferSelection::RandomWindow,
            ..Settings::default()
        },
        1,
    )
}

/// Runs `frames` from their connections; returns the messages of the last frame.
fn run(handle: HandleFn, frames: &[(u64, &str)]) -> Result<Vec<(u64, String)>, ProtoError> {
    let mut shard = shard();
    let mut out = Encoder::new();
    let mut result = Ok(());
    for (conn, frame) in frames {
        out.clear();
        result = handle(&mut shard, 0, ConnId(*conn), j(frame).as_bytes(), &mut out);
    }
    result.map(|_| texts(&out))
}

fn check(frames: &[(u64, &str)], expected: &[(u64, &str)]) {
    let expected: Vec<(u64, String)> = expected.iter().map(|(c, s)| (*c, j(s))).collect();
    for (name, _, handle) in backends() {
        assert_eq!(run(handle, frames).unwrap(), expected, "{name}");
    }
}

#[test]
fn announce_reply() {
    check(
        &[
            (
                1,
                r#"{"action":"announce","event":"completed","info_hash":"h~u0001~u0022~u005c~u00e9","peer_id":"a"}"#,
            ),
            (
                2,
                r#"{"action":"announce","info_hash":"h~u0001~u0022~u005c~u00e9","peer_id":"b"}"#,
            ),
            (
                3,
                r#"{"action":"announce","info_hash":"h~u0001~u0022~u005c~u00e9","peer_id":"c"}"#,
            ),
        ],
        &[(
            3,
            "{\"action\":\"announce\",\"interval\":20,\"info_hash\":\"h~u0001\\\"\\\\\u{e9}\",\"complete\":1,\"incomplete\":2}",
        )],
    );
}

#[test]
fn offer_with_offer_id_and_sdp() {
    check(
        &[
            (1, r#"{"action":"announce","info_hash":"h1","peer_id":"a"}"#),
            (
                2,
                r#"{"action":"announce","info_hash":"h1","peer_id":"p~u0000~u00ff","numwant":1,"offers":[{"offer":{"type":"offer","sdp":"v=0\r\na=x \"q\"","extra":1},"offer_id":"o1"}]}"#,
            ),
        ],
        &[
            (
                2,
                r#"{"action":"announce","interval":20,"info_hash":"h1","complete":0,"incomplete":2}"#,
            ),
            (
                1,
                "{\"action\":\"announce\",\"info_hash\":\"h1\",\"offer_id\":\"o1\",\"peer_id\":\"p~u0000\u{ff}\",\"offer\":{\"type\":\"offer\",\"sdp\":\"v=0\\r\\na=x \\\"q\\\"\"}}",
            ),
        ],
    );
}

#[test]
fn offer_without_offer_id_or_sdp() {
    check(
        &[
            (1, r#"{"action":"announce","info_hash":"h1","peer_id":"a"}"#),
            (
                2,
                r#"{"action":"announce","info_hash":"h1","peer_id":"p1","numwant":1,"offers":[{"offer":{"type":"offer","sdp":"x"}}]}"#,
            ),
        ],
        &[
            (
                2,
                r#"{"action":"announce","interval":20,"info_hash":"h1","complete":0,"incomplete":2}"#,
            ),
            (
                1,
                r#"{"action":"announce","info_hash":"h1","peer_id":"p1","offer":{"type":"offer","sdp":"x"}}"#,
            ),
        ],
    );
    check(
        &[
            (1, r#"{"action":"announce","info_hash":"h1","peer_id":"a"}"#),
            (
                2,
                r#"{"action":"announce","info_hash":"h1","peer_id":"p1","numwant":1,"offers":[{"offer":{},"offer_id":7}]}"#,
            ),
        ],
        &[
            (
                2,
                r#"{"action":"announce","interval":20,"info_hash":"h1","complete":0,"incomplete":2}"#,
            ),
            (
                1,
                r#"{"action":"announce","info_hash":"h1","offer_id":7,"peer_id":"p1","offer":{"type":"offer"}}"#,
            ),
        ],
    );
}

#[test]
fn answer_is_forwarded_without_to_peer_id() {
    let join = (
        1,
        r#"{"action":"announce","info_hash":"h1","peer_id":"p1"}"#,
    );
    check(
        &[
            join,
            (
                2,
                r#"{"action":"announce","info_hash":"h1","peer_id":"p2","to_peer_id":"p1","answer":{"type":"answer","sdp":"y"},"offer_id":"o1","extra":[1,2]}"#,
            ),
        ],
        &[(
            1,
            r#"{"action":"announce","info_hash":"h1","peer_id":"p2","answer":{"type":"answer","sdp":"y"},"offer_id":"o1","extra":[1,2]}"#,
        )],
    );
    check(
        &[
            join,
            (
                2,
                r#"{"to_peer_id":"p1","action":"announce","peer_id":"p2","answer":null}"#,
            ),
        ],
        &[(1, r#"{"action":"announce","peer_id":"p2","answer":null}"#)],
    );
}

#[test]
fn answer_to_unknown_peer_fails() {
    for (name, _, handle) in backends() {
        let r = run(
            handle,
            &[(
                2,
                r#"{"action":"announce","peer_id":"p2","to_peer_id":"nobody","answer":{}}"#,
            )],
        );
        assert_eq!(
            r,
            Err(ProtoError::Tracker(wt_core::TrackerError::UnknownPeer)),
            "{name}"
        );
    }
}

#[test]
fn scrape_files() {
    let join = (
        1,
        r#"{"action":"announce","event":"completed","info_hash":"h1","peer_id":"p1"}"#,
    );
    let join2 = (
        2,
        r#"{"action":"announce","info_hash":"h1","peer_id":"p2"}"#,
    );
    check(
        &[
            join,
            join2,
            (9, r#"{"action":"scrape","info_hash":["h1","nope","h1"]}"#),
        ],
        &[(
            9,
            r#"{"action":"scrape","files":{"h1":{"complete":1,"incomplete":1,"downloaded":1},"nope":{"complete":0,"incomplete":0,"downloaded":0}}}"#,
        )],
    );
    check(
        &[join, (9, r#"{"action":"scrape","info_hash":5}"#)],
        &[(9, r#"{"action":"scrape","files":{}}"#)],
    );
    check(
        &[join, (9, r#"{"action":"scrape"}"#)],
        &[(
            9,
            r#"{"action":"scrape","files":{"h1":{"complete":1,"incomplete":0,"downloaded":1}}}"#,
        )],
    );
}

#[test]
fn stop_with_unmatchable_ids_is_a_no_op() {
    check(
        &[
            (
                1,
                r#"{"action":"announce","info_hash":"h1","peer_id":"p1"}"#,
            ),
            (
                1,
                r#"{"action":"announce","event":"stopped","info_hash":5,"peer_id":"p1"}"#,
            ),
            (9, r#"{"action":"scrape"}"#),
        ],
        &[(
            9,
            r#"{"action":"scrape","files":{"h1":{"complete":0,"incomplete":1,"downloaded":0}}}"#,
        )],
    );
}

#[test]
fn counters_count_messages_and_bytes_per_kind_across_clears() {
    let mut shard = shard();
    let mut out = Encoder::new();
    let frames = [
        (
            1,
            r#"{"action":"announce","info_hash":"h0000000000000000001","peer_id":"pa","numwant":5,"offers":[]}"#,
        ),
        (
            2,
            r#"{"action":"announce","info_hash":"h0000000000000000001","peer_id":"pb","numwant":5,"offers":[{"offer":{"type":"offer","sdp":"x"},"offer_id":"o1"}]}"#,
        ),
        (
            1,
            r#"{"action":"announce","info_hash":"h0000000000000000001","peer_id":"pa","to_peer_id":"pb","answer":{"type":"answer","sdp":"y"},"offer_id":"o1"}"#,
        ),
        (
            3,
            r#"{"action":"scrape","info_hash":"h0000000000000000001"}"#,
        ),
    ];
    let mut bytes = [0u64; 4];
    for (i, (conn, frame)) in frames.iter().enumerate() {
        out.clear();
        wt_proto::handle(&mut shard, 0, ConnId(*conn), frame.as_bytes(), &mut out).unwrap();
        // Frame 2 produces a reply and an offer; the others one message each.
        for (_, text) in out.messages() {
            let kind = match (i, text.windows(7).any(|w| w == b"\"offer\"")) {
                (1, true) => 1,
                (2, _) => 2,
                (3, _) => 3,
                _ => 0,
            };
            bytes[kind] += text.len() as u64;
        }
    }
    let c = out.counters();
    assert_eq!(
        [
            c.announce_replies.messages,
            c.offers.messages,
            c.answers.messages,
            c.scrapes.messages
        ],
        [2, 1, 1, 1]
    );
    assert_eq!(
        [
            c.announce_replies.bytes,
            c.offers.bytes,
            c.answers.bytes,
            c.scrapes.bytes
        ],
        bytes
    );
    // take() keeps them too.
    let _ = out.take();
    assert_eq!(out.counters(), c);
}
