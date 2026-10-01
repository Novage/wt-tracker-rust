//! Id decoding and string escaping against `serde_json` as the oracle (its string escaping is
//! the same as JS `JSON.stringify` for valid Unicode).

mod common;

use common::{backends, texts};
use proptest::prelude::*;
use wt_core::{ConnId, MAX_KEY_LEN, Settings, Shard};
use wt_proto::{Encoder, Message};

/// Every char as a `\uXXXX` escape (surrogate pairs for astral chars).
fn ascii_escaped(s: &str) -> String {
    let mut out = String::from("\"");
    for unit in s.encode_utf16() {
        out.push_str(&format!("\\u{unit:04x}"));
    }
    out.push('"');
    out
}

fn id() -> impl Strategy<Value = String> {
    prop::collection::vec(any::<char>(), 0..12)
        .prop_map(String::from_iter)
        .prop_filter("fits a Key", |s| s.len() <= MAX_KEY_LEN)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn ids_decode_and_reencode_like_json_stringify(s in id(), ascii in any::<bool>()) {
        let token = if ascii { ascii_escaped(&s) } else { serde_json::to_string(&s).unwrap() };
        let frame = format!(r#"{{"action":"announce","info_hash":{token},"peer_id":{token}}}"#);
        for (name, parse, handle) in backends() {
            match parse(frame.as_bytes()) {
                Ok(Message::Announce { info_hash, peer_id, .. }) => {
                    prop_assert_eq!(info_hash.as_bytes(), s.as_bytes(), "{}", name);
                    prop_assert_eq!(peer_id.as_bytes(), s.as_bytes(), "{}", name);
                }
                other => prop_assert!(false, "{}: {:?}", name, other),
            }
            let mut shard = Shard::new(Settings::default(), 1);
            let mut out = Encoder::new();
            handle(&mut shard, 0, ConnId(1), frame.as_bytes(), &mut out).unwrap();
            let expected = format!(
                r#"{{"action":"announce","interval":20,"info_hash":{},"complete":0,"incomplete":1}}"#,
                serde_json::to_string(&s).unwrap()
            );
            prop_assert_eq!(texts(&out), vec![(1, expected)], "{}", name);
        }
    }
}
