//! Fuzz targets (`fuzz/`, spec §9): bytes from the network through the WebSocket codec,
//! permessage-deflate and the HTTP upgrade. Each target must never panic for any input; the
//! assertions check invariants that hold for every input. Also run by `cargo test` over the seed
//! corpus and mutations of it.

use crate::http;
use crate::ws::codec::{self, Data, Fragments, OpCode, Parsed};
use crate::ws::deflate;

/// A client byte stream through frame parsing, reassembly and inflating, fed in chunks like the
/// driver reads it. `data[0]`: bit 0 = permessage-deflate negotiated, bits 1–2 = the payload
/// limit, bits 3–7 = the chunk size.
pub fn ws_frames(data: &[u8]) {
    let Some((&control, stream)) = data.split_first() else {
        return;
    };
    let deflate = control & 1 != 0;
    let max = [125, 1000, 65_536, 1 << 20][((control >> 1) & 3) as usize];
    let chunk = 1 + (control >> 3) as usize * 7;
    let mut pending = Vec::new();
    let mut fragments = Fragments::default();
    for piece in stream.chunks(chunk) {
        pending.extend_from_slice(piece);
        let mut pos = 0;
        loop {
            match codec::parse_frame(&mut pending[pos..], max, deflate) {
                Parsed::Frame(frame, used) => {
                    assert!(used <= pending.len() - pos && frame.payload.end == used);
                    assert!(frame.payload.len() <= max);
                    let payload = &pending[pos + frame.payload.start..pos + frame.payload.end];
                    if frame.opcode.is_control() {
                        assert!(frame.fin && payload.len() <= 125);
                        if frame.opcode == OpCode::Close {
                            let _ = codec::close_code(payload);
                        }
                    } else {
                        match fragments.push(&frame, payload, max) {
                            Err(_) => return,
                            Ok(Data::Partial) => {}
                            Ok(Data::Message(text, compressed, message)) => {
                                assert!(message.len() <= max);
                                let check = |m: &[u8]| {
                                    assert!(m.len() <= max);
                                    if text {
                                        let _ = std::str::from_utf8(m);
                                    }
                                };
                                if compressed {
                                    assert!(deflate);
                                    if deflate::inflate(message, max, check).is_err() {
                                        return;
                                    }
                                } else {
                                    check(message);
                                }
                            }
                        }
                        fragments.reset();
                    }
                    pos += used;
                }
                Parsed::Incomplete(total) => {
                    if let Some(total) = total {
                        assert!(total > pending.len() - pos);
                    }
                    break;
                }
                Parsed::Error(_) => return,
            }
        }
        pending.drain(..pos);
    }
}

/// Compressing any message and inflating it gives it back. `data[0]` picks the window.
pub fn deflate_roundtrip(data: &[u8]) {
    let Some((&control, message)) = data.split_first() else {
        return;
    };
    let window = 9 + control % 7;
    let compressed = deflate::deflate(message, window);
    let back = deflate::inflate(&compressed, message.len(), |m| m.to_vec());
    assert_eq!(back.as_deref(), Ok(message));
}

/// An HTTP request head through parsing, the upgrade response and the permessage-deflate
/// negotiation: the response must be well-formed (no header injection from echoed values).
pub fn http_upgrade(data: &[u8]) {
    if let Ok(Some((head, len))) = http::parse_head(data) {
        assert!(len <= data.len());
        for compression in [false, true] {
            if let Ok((response, negotiated)) = http::upgrade_response(&head, compression) {
                assert!(response.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
                assert!(response.ends_with("\r\n\r\n"));
                let lines: Vec<&str> = response.trim_end_matches("\r\n").split("\r\n").collect();
                for line in &lines[1..] {
                    assert!(
                        line.contains(": ") && !line.contains(['\r', '\n']),
                        "{line:?}"
                    );
                }
                let extension = lines
                    .iter()
                    .any(|l| l.starts_with("Sec-WebSocket-Extensions:"));
                assert_eq!(extension, negotiated.is_some());
                assert!(compression || negotiated.is_none());
            }
        }
        let _ = http::path_matches(&head.path, &head.path);
        let _ = http::path_matches("/announce/*", &head.path);
    }
    if let Ok(header) = std::str::from_utf8(data)
        && let Some((negotiated, response)) = deflate::negotiate(header)
    {
        assert!(response.starts_with("permessage-deflate; client_no_context_takeover"));
        assert!(negotiated.out_window.is_none_or(|w| (9..=15).contains(&w)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The seed corpus of a target, plus mutations of it (bytes flipped, cut, duplicated).
    pub(crate) fn inputs(target: &str) -> Vec<Vec<u8>> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fuzz/corpus")
            .join(target);
        let seeds: Vec<Vec<u8>> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|f| std::fs::read(f.unwrap().path()).unwrap())
            .collect();
        assert!(!seeds.is_empty());
        let mut rng = fastrand::Rng::with_seed(0x5eed);
        let mut inputs = seeds.clone();
        for _ in 0..2000 {
            let mut input = seeds[rng.usize(..seeds.len())].clone();
            for _ in 0..rng.usize(1..4) {
                if input.is_empty() {
                    break;
                }
                let i = rng.usize(..input.len());
                match rng.u8(..4) {
                    0 => input[i] ^= 1 << rng.u8(..8),
                    1 => input[i] = rng.u8(..),
                    2 => input.truncate(i),
                    _ => {
                        let copy = input[i..].to_vec();
                        input.extend_from_slice(&copy);
                    }
                }
            }
            inputs.push(input);
        }
        inputs
    }

    #[test]
    fn ws_frames_survives_the_corpus_and_mutations() {
        inputs("ws_frames").iter().for_each(|i| ws_frames(i));
    }

    #[test]
    fn deflate_roundtrip_survives_the_corpus_and_mutations() {
        inputs("deflate_roundtrip")
            .iter()
            .for_each(|i| deflate_roundtrip(i));
    }

    #[test]
    fn http_upgrade_survives_the_corpus_and_mutations() {
        inputs("http_upgrade").iter().for_each(|i| http_upgrade(i));
    }
}
