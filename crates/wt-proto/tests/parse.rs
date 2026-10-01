//! Parsing rules, one test per rule of the JS FastTracker / uws-tracker (spec §7.1).
//! Every test runs against every compiled-in backend.

mod common;

use common::backends;
use wt_core::AnnounceEvent;
use wt_proto::{Message, Payload, ProtoError};

/// `~u` stands for a JSON `\\u` escape (keeps the escapes readable and literal in source).
fn j(s: &str) -> String {
    s.replace("~u", "\\u")
}

/// Runs `check` on the parse result of `frame` for every backend.
fn each(frame: &str, check: impl Fn(&str, Result<Message<'_>, ProtoError>)) {
    let frame = j(frame);
    for (name, parse, _) in backends() {
        check(name, parse(frame.as_bytes()));
    }
}

fn err(frame: &str, expected: ProtoError) {
    each(frame, |b, r| {
        assert_eq!(r.err(), Some(expected), "{b}: {frame}")
    });
}

fn announce(frame: &str, check: impl Fn(&str, AnnounceEvent, bool, Option<u32>)) {
    each(frame, |b, r| match r {
        Ok(Message::Announce {
            event,
            left_zero,
            numwant,
            ..
        }) => check(b, event, left_zero, numwant),
        other => panic!("{b}: {frame}: {other:?}"),
    });
}

const A: &str = r#""action":"announce","info_hash":"h1","peer_id":"p1""#;

#[test]
fn announce_ids_are_decoded() {
    each(
        r#"{"action":"announce","info_hash":"h~u0000~u00ff\"","peer_id":"p~ud83d~ude00"}"#,
        |b, r| match r {
            Ok(Message::Announce {
                info_hash, peer_id, ..
            }) => {
                assert_eq!(info_hash.as_bytes(), "h\u{0}\u{ff}\"".as_bytes(), "{b}");
                assert_eq!(peer_id.as_bytes(), "p😀".as_bytes(), "{b}");
            }
            other => panic!("{b}: {other:?}"),
        },
    );
}

#[test]
fn event_values() {
    announce(&format!("{{{A}}}"), |b, e, _, _| {
        assert_eq!(e, AnnounceEvent::None, "{b}")
    });
    announce(&format!(r#"{{{A},"event":"started"}}"#), |b, e, _, _| {
        assert_eq!(e, AnnounceEvent::Started, "{b}")
    });
    announce(&format!(r#"{{{A},"event":"completed"}}"#), |b, e, _, _| {
        assert_eq!(e, AnnounceEvent::Completed, "{b}")
    });
    // Escaped spelling is the same string.
    announce(
        &format!(r#"{{{A},"event":"st~u0061rted"}}"#),
        |b, e, _, _| assert_eq!(e, AnnounceEvent::Started, "{b}"),
    );
    for bad in [r#""paused""#, "null", "5", "{}"] {
        err(
            &format!(r#"{{{A},"event":{bad}}}"#),
            ProtoError::UnknownEvent,
        );
    }
}

#[test]
fn stopped_is_a_stop() {
    each(
        &format!(r#"{{{A},"event":"stopped","answer":{{}}}}"#),
        |b, r| {
            assert!(
                matches!(
                    r,
                    Ok(Message::Stop {
                        info_hash: Some(_),
                        peer_id: Some(_)
                    })
                ),
                "{b}: {r:?}"
            )
        },
    );
    // Non-string or too long ids cannot match anything: a no-op, not an error.
    each(
        &format!(
            r#"{{"action":"announce","event":"stopped","info_hash":5,"peer_id":"{}"}}"#,
            "x".repeat(41)
        ),
        |b, r| {
            assert!(
                matches!(
                    r,
                    Ok(Message::Stop {
                        info_hash: None,
                        peer_id: None
                    })
                ),
                "{b}: {r:?}"
            )
        },
    );
    err(
        r#"{"action":"announce","event":"stopped","info_hash":"h1","peer_id":5}"#,
        ProtoError::BadField("peer_id"),
    );
    err(
        r#"{"action":"announce","event":"stopped","info_hash":"h1"}"#,
        ProtoError::BadField("peer_id"),
    );
}

#[test]
fn left_zero_is_strict_number_zero() {
    for (left, zero) in [
        ("0", true),
        ("-0", true),
        ("0.0", true),
        ("0e5", true),
        (r#""0""#, false),
        ("7", false),
        ("null", false),
    ] {
        announce(&format!(r#"{{{A},"left":{left}}}"#), |b, _, l, _| {
            assert_eq!(l, zero, "{b}: left {left}")
        });
    }
    announce(&format!("{{{A}}}"), |b, _, l, _| assert!(!l, "{b}"));
}

#[test]
fn numwant_must_be_an_integer() {
    for (numwant, expected) in [
        ("10", Some(10)),
        ("10.0", Some(10)),
        ("1e1", Some(10)),
        ("2.5", None),
        (r#""10""#, None),
        ("null", None),
        ("-3", Some(0)),
        ("1e300", Some(u32::MAX)),
    ] {
        announce(&format!(r#"{{{A},"numwant":{numwant}}}"#), |b, _, _, n| {
            assert_eq!(n, expected, "{b}: numwant {numwant}")
        });
    }
    announce(&format!("{{{A}}}"), |b, _, _, n| assert_eq!(n, None, "{b}"));
}

#[test]
fn action_must_be_announce_or_scrape() {
    err(
        r#"{"info_hash":"h1","peer_id":"p1"}"#,
        ProtoError::UnknownAction,
    );
    err(r#"{"action":5}"#, ProtoError::UnknownAction);
    err(r#"{"action":"foo"}"#, ProtoError::UnknownAction);
    announce(
        r#"{"action":"annou~u006ece","info_hash":"h1","peer_id":"p1"}"#,
        |_, _, _, _| {},
    );
}

#[test]
fn announce_ids_must_be_strings_that_fit() {
    err(
        r#"{"action":"announce","info_hash":5,"peer_id":"p1"}"#,
        ProtoError::BadField("info_hash"),
    );
    err(
        r#"{"action":"announce","info_hash":"h1"}"#,
        ProtoError::BadField("peer_id"),
    );
    err(
        &format!(
            r#"{{"action":"announce","info_hash":"h1","peer_id":"{}"}}"#,
            "x".repeat(41)
        ),
        ProtoError::KeyTooLong,
    );
    // Lone surrogate: serde_json rejects the id, sonic-rs the whole frame. Either closes.
    each(
        r#"{"action":"announce","info_hash":"h1","peer_id":"~ud800"}"#,
        |b, r| assert!(r.is_err(), "{b}: {r:?}"),
    );
}

#[test]
fn answer_with_any_value_and_no_event() {
    for answer in ["null", "5", r#"{"type":"answer","sdp":"y"}"#] {
        each(
            &format!(r#"{{{A},"to_peer_id":"p2","answer":{answer}}}"#),
            |b, r| {
                assert!(
                    matches!(r, Ok(Message::Answer { .. })),
                    "{b}: {answer}: {r:?}"
                )
            },
        );
    }
    // With an event it is an announce.
    announce(
        &format!(r#"{{{A},"event":"started","to_peer_id":"p2","answer":{{}}}}"#),
        |_, _, _, _| {},
    );
    err(
        r#"{"action":"announce","peer_id":"p1","answer":{}}"#,
        ProtoError::BadField("to_peer_id"),
    );
    err(
        r#"{"action":"announce","peer_id":5,"to_peer_id":"p2","answer":{}}"#,
        ProtoError::BadField("peer_id"),
    );
    // info_hash is not checked.
    each(
        r#"{"action":"announce","peer_id":"p1","to_peer_id":"p2","answer":{}}"#,
        |b, r| {
            assert!(
                matches!(
                    r,
                    Ok(Message::Answer {
                        info_hash: None,
                        ..
                    })
                ),
                "{b}: {r:?}"
            )
        },
    );
}

fn answer_text(frame: &str) -> Vec<(String, String)> {
    let frame = j(frame);
    let mut out = Vec::new();
    for (b, parse, _) in backends() {
        match parse(frame.as_bytes()) {
            Ok(Message::Answer {
                answer: Payload::Answer { head, tail },
                ..
            }) => out.push((
                b.to_string(),
                format!(
                    "{}{}",
                    std::str::from_utf8(head).unwrap(),
                    std::str::from_utf8(tail).unwrap()
                ),
            )),
            other => panic!("{b}: {frame}: {other:?}"),
        }
    }
    out
}

#[test]
fn answer_body_is_the_frame_without_to_peer_id() {
    for (frame, expected) in [
        (
            r#"{"action":"announce","peer_id":"p2","to_peer_id":"p1","answer":{"sdp":"y"},"x":[1]}"#,
            r#"{"action":"announce","peer_id":"p2","answer":{"sdp":"y"},"x":[1]}"#,
        ),
        (
            r#"{"to_peer_id":"p1","action":"announce","peer_id":"p2","answer":null}"#,
            r#"{"action":"announce","peer_id":"p2","answer":null}"#,
        ),
        (
            r#"{"action":"announce","peer_id":"p2","answer":1,"to_peer_id":"p1"}"#,
            r#"{"action":"announce","peer_id":"p2","answer":1}"#,
        ),
        (
            "{ \"action\" : \"announce\" , \"peer_id\":\"p2\",\n \"to_peer_id\" :\t\"p1\" , \"answer\":1 }",
            "{ \"action\" : \"announce\" , \"peer_id\":\"p2\" , \"answer\":1 }",
        ),
    ] {
        for (b, text) in answer_text(frame) {
            assert_eq!(text, expected, "{b}");
        }
    }
    // A key spelled with escapes is not produced by JSON.stringify: rejected.
    err(
        r#"{"action":"announce","peer_id":"p2","to_peer~u005fid":"p1","answer":1}"#,
        ProtoError::BadField("to_peer_id"),
    );
}

#[test]
fn scrape_targets() {
    let hashes = |frame: &str| {
        let frame = j(frame);
        let mut out = Vec::new();
        for (b, parse, _) in backends() {
            match parse(frame.as_bytes()) {
                Ok(Message::Scrape { info_hashes }) => out.push(info_hashes.map(|v| {
                    v.iter()
                        .map(|h| String::from_utf8(h.to_vec()).unwrap())
                        .collect::<Vec<_>>()
                })),
                other => panic!("{b}: {other:?}"),
            }
        }
        out
    };
    for h in hashes(r#"{"action":"scrape"}"#) {
        assert_eq!(h, None);
    }
    for h in hashes(r#"{"action":"scrape","info_hash":"h~u00e9"}"#) {
        assert_eq!(h, Some(vec!["hé".to_string()]));
    }
    for h in hashes(r#"{"action":"scrape","info_hash":["h1",5,null,"h2","h1"]}"#) {
        assert_eq!(h, Some(vec!["h1".into(), "h2".into(), "h1".into()]));
    }
    for other in ["5", "null", "{}"] {
        for h in hashes(&format!(r#"{{"action":"scrape","info_hash":{other}}}"#)) {
            assert_eq!(h, Some(vec![]), "info_hash {other}");
        }
    }
}

#[test]
fn offers_keep_raw_offer_id_and_sdp() {
    let frame = format!(
        r#"{{{A},"numwant":5,"offers":[{{"offer":{{"type":"offer","sdp":"v=0\r\n\"x\""}},"offer_id":"o~u0031","x":1}},{{"offer":{{}},"offer_id":null}},{{"offer":[]}}]}}"#
    );
    each(&frame, |b, r| match r {
        Ok(Message::Announce {
            offers: Some(offers),
            ..
        }) => {
            let raw = |s: Option<&[u8]>| s.map(|s| std::str::from_utf8(s).unwrap().to_string());
            let got: Vec<_> = offers
                .iter()
                .map(|p| match p {
                    Payload::Offer { offer_id, sdp } => (raw(*offer_id), raw(*sdp)),
                    _ => panic!(),
                })
                .collect();
            assert_eq!(
                got,
                [
                    (Some(j(r#""o~u0031""#)), Some(r#""v=0\r\n\"x\"""#.into())),
                    (Some("null".into()), None),
                    (None, None),
                ],
                "{b}"
            );
        }
        other => panic!("{b}: {other:?}"),
    });
}

#[test]
fn malformed_offers_are_rejected() {
    for offers in [
        r#"{}"#,
        r#""x""#,
        "[5]",
        "[null]",
        "[[]]",
        r#"[{"offer_id":1}]"#,
        r#"[{"offer":null}]"#,
        r#"[{"offer":"x"}]"#,
    ] {
        err(
            &format!(r#"{{{A},"offers":{offers}}}"#),
            ProtoError::BadField("offers"),
        );
    }
}

#[test]
fn invalid_json_and_non_objects() {
    for frame in [
        "",
        "{",
        r#"{"action":"announce""#,
        "{\"action\":\"announce\",\"x\":\"a\u{1}b\"}",
        r#"{"action":"announce","x":"\q"}"#,
        r#"{"action":"scrape"} x"#,
        r#"{"action":"scrape"}{}"#,
    ] {
        err(frame, ProtoError::InvalidJson);
    }
    for frame in ["null", "[]", r#""x""#, "5", " [1] "] {
        err(frame, ProtoError::NotAnObject);
    }
    for (b, parse, _) in backends() {
        assert_eq!(
            parse(b"{\"action\":\"announce\",\"x\":\"\xff\"}").err(),
            Some(ProtoError::InvalidJson),
            "{b}"
        );
    }
}

#[test]
fn whitespace_around_the_object_is_fine() {
    announce(&format!(" \n{{ {A} }}\t"), |_, _, _, _| {});
}

#[test]
fn duplicate_keys_last_wins_like_json_parse() {
    each(r#"{"action":"announce","action":"scrape"}"#, |b, r| {
        assert!(
            matches!(r, Ok(Message::Scrape { info_hashes: None })),
            "{b}: {r:?}"
        )
    });
    announce(
        &format!(r#"{{{A},"numwant":1,"numwant":7}}"#),
        |b, _, _, n| assert_eq!(n, Some(7), "{b}"),
    );
    // Two to_peer_id members: JS drops both, a cut would forward one. Rejected.
    err(
        r#"{"action":"announce","peer_id":"p2","to_peer_id":"p1","to_peer_id":"p1","answer":1}"#,
        ProtoError::BadField("to_peer_id"),
    );
}

#[test]
fn escaped_key_names_match() {
    // JSON.parse decodes member names too.
    each(
        r#"{"action":"announce","info_hash":"h1","peer~u005fid":"p1"}"#,
        |b, r| assert!(matches!(r, Ok(Message::Announce { .. })), "{b}: {r:?}"),
    );
}
