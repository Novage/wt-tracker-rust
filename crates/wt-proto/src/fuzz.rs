//! Fuzz target (`fuzz/`, spec §9): client frames parsed and applied to a tracker shard, as the
//! server does. Must never panic; after every step the shard's invariants hold and every
//! outgoing message is valid JSON for a known connection. The same frames applied through
//! [`OwnedMessage`] (the cross-worker path) to a second shard give identical output. Also run by
//! `cargo test` over the seed corpus and mutations of it.

use wt_core::{ConnId, OfferSelection, Settings, Shard};

use crate::{Backend, DefaultBackend, Encoder, OwnedMessage, apply};

/// Frames are separated by `0xFF` (never in UTF-8 text). `data[0]`: offer selection, `data[1]`:
/// `max_offers`. Each frame's first byte: connection (bits 0–2) and kind (bits 5–7: 7 =
/// disconnect, 6 = expiry after 30 s, else the rest of the frame is a message).
pub fn protocol(data: &[u8]) {
    let [selection, max_offers, frames @ ..] = data else {
        return;
    };
    let settings = Settings {
        max_offers: 1 + u32::from(*max_offers % 20),
        announce_interval: 20,
        offer_selection: [
            OfferSelection::RandomSample,
            OfferSelection::RandomWindow,
            OfferSelection::RoundRobin,
        ][(*selection % 3) as usize],
    };
    let (mut direct, mut owned) = (Shard::new(settings, 7), Shard::new(settings, 7));
    let (mut out_direct, mut out_owned) = (Encoder::new(), Encoder::new());
    let mut now = 0;
    for frame in frames.split(|&b| b == 0xFF) {
        let Some((&control, text)) = frame.split_first() else {
            continue;
        };
        let conn = ConnId(u64::from(control & 7));
        match control >> 5 {
            7 => {
                direct.disconnect(conn, &mut out_direct);
                owned.disconnect(conn, &mut out_owned);
            }
            6 => {
                now += 30;
                direct.expire(now, &mut out_direct);
                owned.expire(now, &mut out_owned);
            }
            _ => {
                if let Ok(message) = DefaultBackend::parse(text) {
                    let a = apply(&mut direct, now, conn, &message, &mut out_direct);
                    let copy = OwnedMessage::copy_from(text, &message);
                    let b = apply(&mut owned, now, conn, &copy.message(), &mut out_owned);
                    assert_eq!(a.is_ok(), b.is_ok());
                }
            }
        }
        direct.check_invariants().unwrap();
        let (a, b) = (out_direct.take(), out_owned.take());
        let a: Vec<_> = a.iter().collect();
        let b: Vec<_> = b.iter().collect();
        assert_eq!(a, b, "owned path differs");
        for (to, message) in a {
            assert!(to.0 < 8, "message for unknown connection {to:?}");
            // `IgnoredAny`, not `Value`: with serde_json's `raw_value` feature a `Value` rejects
            // the (valid) key "$serde_json::private::RawValue", which clients may send.
            if let Err(e) = serde_json::from_slice::<serde::de::IgnoredAny>(&message) {
                panic!("invalid JSON ({e}): {}", String::from_utf8_lossy(&message));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CI fuzz crash (protocol, 2026-10-05): a scrape of the info_hash
    /// `"$serde_json::private::RawValue"` gives a reply with that string as a key. The reply is
    /// valid JSON, but serde_json reserves that key for `RawValue`, and the check parsed the
    /// reply into a `Value`, which rejects it.
    #[test]
    fn a_reply_keyed_by_serde_jsons_reserved_name_is_valid_json() {
        let mut input = vec![0, 5, 0x02];
        input.extend_from_slice(
            br#"{"action":"scrape","info_hash":["$serde_json::private::RawValue"]}"#,
        );
        protocol(&input);
    }

    #[test]
    fn protocol_survives_the_corpus_and_mutations() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/protocol");
        let seeds: Vec<Vec<u8>> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|f| std::fs::read(f.unwrap().path()).unwrap())
            .collect();
        assert!(!seeds.is_empty());
        // xorshift: wt-proto has no rng dependency.
        let mut state = 0x5eed_u64;
        let mut next = move |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n.max(1) as u64) as usize
        };
        for seed in &seeds {
            protocol(seed);
        }
        for _ in 0..2000 {
            let mut input = seeds[next(seeds.len())].clone();
            for _ in 0..1 + next(3) {
                if input.is_empty() {
                    break;
                }
                let i = next(input.len());
                match next(4) {
                    0 => input[i] ^= 1 << next(8),
                    1 => input[i] = next(256) as u8,
                    2 => input.truncate(i),
                    _ => {
                        let copy = input[i..].to_vec();
                        input.extend_from_slice(&copy);
                    }
                }
            }
            protocol(&input);
        }
    }
}
