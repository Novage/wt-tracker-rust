//! `OwnedMessage` (cross-thread form) and `Encoder::take` (shared output buffer).

mod common;

use bytes::Bytes;
use common::{backends, texts};
use wt_core::{ConnId, Settings, Shard};
use wt_proto::{Encoder, OwnedMessage, apply};

const FRAMES: [&str; 6] = [
    r#"{"action":"announce","info_hash":"h1","peer_id":"p1","event":"completed","numwant":5,"offers":[{"offer":{"type":"offer","sdp":"v=0\r\n"},"offer_id":"o1"},{"offer":{}},{"offer":[],"offer_id":7}]}"#,
    r#"{"action":"announce","info_hash":"h1","peer_id":"p2","left":0}"#,
    r#"{"action":"announce","info_hash":"h1","peer_id":"p2","to_peer_id":"p1","answer":{"sdp":"y"},"x":1}"#,
    r#"{"action":"scrape","info_hash":["h1","nope","h1"]}"#,
    r#"{"action":"scrape"}"#,
    r#"{"action":"announce","event":"stopped","info_hash":"h1","peer_id":"p2"}"#,
];

/// The connection each frame comes from: p2's answer and stop from p2's own connection (1), so
/// they take effect (spec §5.3, §5.4).
const CONNS: [u64; 6] = [0, 1, 1, 0, 0, 1];

#[test]
fn owned_message_round_trips_and_applies_identically() {
    for (name, parse, _) in backends() {
        let (mut direct, mut via_owned) = (
            Shard::new(Settings::default(), 1),
            Shard::new(Settings::default(), 1),
        );
        let (mut out_direct, mut out_owned) = (Encoder::new(), Encoder::new());
        for (i, text) in FRAMES.iter().enumerate() {
            let frame = Bytes::copy_from_slice(text.as_bytes());
            let message = parse(&frame).unwrap();
            let owned = OwnedMessage::new(frame.clone(), &message);
            assert_eq!(
                format!("{:?}", owned.message()),
                format!("{message:?}"),
                "{name} frame {i}"
            );

            // The owned form is Send: apply it on another thread.
            let owned = std::thread::spawn(move || owned).join().unwrap();
            let conn = ConnId(CONNS[i]);
            out_direct.clear();
            out_owned.clear();
            apply(&mut direct, 0, conn, &message, &mut out_direct).unwrap();
            apply(&mut via_owned, 0, conn, &owned.message(), &mut out_owned).unwrap();
            assert_eq!(texts(&out_owned), texts(&out_direct), "{name} frame {i}");
            if i == 2 {
                assert_eq!(
                    texts(&out_direct).len(),
                    1,
                    "{name}: the answer is delivered"
                );
            }
        }
    }
}

#[test]
fn take_moves_messages_into_one_shared_buffer() {
    let mut shard = Shard::new(Settings::default(), 1);
    let mut out = Encoder::new();
    for (i, text) in FRAMES[..3].iter().enumerate() {
        wt_proto::handle(&mut shard, 0, ConnId(CONNS[i]), text.as_bytes(), &mut out).unwrap();
    }
    let expected = texts(&out);
    let batch = out.take();
    assert_eq!(out.messages().len(), 0, "take clears the encoder");
    let got: Vec<(u64, String)> = batch
        .iter()
        .map(|(to, m)| (to.0, String::from_utf8(m.to_vec()).unwrap()))
        .collect();
    assert_eq!(got, expected);
    assert_eq!(batch.len(), expected.len());
}

#[test]
fn copy_from_a_borrowed_buffer_gives_the_same_message() {
    for (name, parse, _) in backends() {
        for text in FRAMES {
            let mut buffer = text.as_bytes().to_vec();
            let message = parse(&buffer).unwrap();
            let expected = format!("{message:?}");
            let owned = OwnedMessage::copy_from(&buffer, &message);
            drop(message);
            buffer.fill(b' '); // the owned copy no longer depends on the buffer
            assert_eq!(format!("{:?}", owned.message()), expected, "{name}");
        }
    }
}
