//! permessage-deflate (RFC 7692) without context takeover in either direction, like the JS
//! tracker's uWebSockets `SHARED_COMPRESSOR` (spec §13.2): every message is compressed on its
//! own, so one inflater and one deflater per worker thread serve all connections and a
//! connection keeps no zlib state.

use std::cell::RefCell;

use bytes::Bytes;
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

use super::codec::close;

/// What was agreed with a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Negotiated {
    /// Window bits for compressing outgoing messages; `None`: never compress them (the client
    /// asked for `server_max_window_bits=8`, which zlib cannot produce for raw streams).
    pub out_window: Option<u8>,
}

/// The extension response, before any `server_max_window_bits`. Same as uWebSockets for its
/// shared compressor.
const RESPONSE: &str = "permessage-deflate; client_no_context_takeover; server_no_context_takeover";

/// Picks the first acceptable `permessage-deflate` offer of a `Sec-WebSocket-Extensions`
/// header: what was agreed and the response header value. An offer with an unknown, repeated
/// or invalid parameter is declined (RFC 7692 §5); other extensions are ignored.
pub(crate) fn negotiate(header: &str) -> Option<(Negotiated, String)> {
    header.split(',').find_map(offer)
}

fn offer(offer: &str) -> Option<(Negotiated, String)> {
    let mut parts = offer.split(';').map(str::trim);
    if !parts.next()?.eq_ignore_ascii_case("permessage-deflate") {
        return None;
    }
    let mut seen = [false; 4];
    let mut server_max = None;
    for param in parts {
        let (name, value) = match param.split_once('=') {
            Some((n, v)) => (n.trim(), Some(v.trim().trim_matches('"'))),
            None => (param, None),
        };
        let index = [
            "server_no_context_takeover",
            "client_no_context_takeover",
            "server_max_window_bits",
            "client_max_window_bits",
        ]
        .iter()
        .position(|known| name.eq_ignore_ascii_case(known))?;
        if std::mem::replace(&mut seen[index], true) {
            return None;
        }
        match (index, value) {
            (0 | 1, None) => {}
            (2, Some(v)) => server_max = Some(window_bits(v)?),
            (3, None) => {}
            (3, Some(v)) => {
                window_bits(v)?;
            }
            _ => return None,
        }
    }
    let mut response = RESPONSE.to_string();
    if let Some(bits) = server_max {
        // Accepting an offer with server_max_window_bits requires echoing it (RFC 7692 §7.1.2.1).
        response.push_str(&format!("; server_max_window_bits={bits}"));
    }
    let out_window = match server_max {
        None => Some(15),
        Some(8) => None,
        bits => bits,
    };
    Some((Negotiated { out_window }, response))
}

/// `8`..`15`, digits only, no leading zero.
fn window_bits(value: &str) -> Option<u8> {
    if value.is_empty() || value.starts_with('0') || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok().filter(|bits| (8..=15).contains(bits))
}

/// Inflated messages larger than this keep their buffer only until the next message.
const KEEP_CAPACITY: usize = 256 * 1024;
/// Output grows in steps of at most this much.
const GROW: usize = 64 * 1024;
/// Appended to every compressed message before inflating (RFC 7692 §7.2.2).
const TAIL: [u8; 4] = [0x00, 0x00, 0xff, 0xff];

thread_local! {
    static INFLATER: RefCell<Decompress> = RefCell::new(Decompress::new(false));
    /// The inflated message, shared by all connections of the worker thread.
    static INFLATED: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// One deflater per window size (index = window bits).
    static DEFLATERS: RefCell<[Option<Compress>; 16]> = RefCell::new(Default::default());
}

/// Inflates one compressed message (at most `max` bytes) and passes it to `f`, which runs
/// while the worker's inflate buffer is borrowed. `Err`: the close code (1009 too big, 1007
/// corrupt).
pub(crate) fn inflate<R>(
    compressed: &[u8],
    max: usize,
    f: impl FnOnce(&[u8]) -> R,
) -> Result<R, u16> {
    INFLATER.with_borrow_mut(|d| {
        INFLATED.with_borrow_mut(|out| {
            d.reset(false);
            out.clear();
            let ended = feed(d, compressed, out, max, FlushDecompress::None)?;
            if !ended {
                feed(d, &TAIL, out, max, FlushDecompress::Sync)?;
            }
            let result = f(out);
            if out.capacity() > KEEP_CAPACITY {
                *out = Vec::new();
            }
            Ok(result)
        })
    })
}

/// Feeds `input` to the inflater; `Ok(true)` if the deflate stream ended (a final block).
fn feed(
    d: &mut Decompress,
    input: &[u8],
    out: &mut Vec<u8>,
    max: usize,
    flush: FlushDecompress,
) -> Result<bool, u16> {
    let mut pos = 0;
    loop {
        if out.len() == out.capacity() {
            out.reserve_exact((max + 1 - out.len()).clamp(1, GROW));
        }
        let (in0, out0) = (d.total_in(), out.len());
        let status = d
            .decompress_vec(&input[pos..], out, flush)
            .map_err(|_| close::INVALID_DATA)?;
        pos += (d.total_in() - in0) as usize;
        if out.len() > max {
            return Err(close::TOO_BIG);
        }
        if status == Status::StreamEnd {
            return Ok(true);
        }
        let full = out.len() == out.capacity();
        if pos == input.len() && !full {
            return Ok(false);
        }
        if d.total_in() == in0 && out.len() == out0 && !full {
            // No progress although input and room are left.
            return Err(close::INVALID_DATA);
        }
    }
}

/// Compresses one outgoing message with `window` bits (9..=15): the payload of a frame with
/// RSV1 set.
pub(crate) fn deflate(data: &[u8], window: u8) -> Bytes {
    DEFLATERS.with_borrow_mut(|deflaters| {
        let c = deflaters[window as usize].get_or_insert_with(|| {
            Compress::new_with_window_bits(Compression::fast(), false, window)
        });
        c.reset();
        let mut out = Vec::with_capacity(data.len() / 2 + 64);
        let mut pos = 0;
        loop {
            if out.len() == out.capacity() {
                out.reserve(out.capacity().max(64));
            }
            let in0 = c.total_in();
            c.compress_vec(&data[pos..], &mut out, FlushCompress::Sync)
                .expect("deflate of an in-memory buffer");
            pos += (c.total_in() - in0) as usize;
            if pos == data.len() && out.len() < out.capacity() {
                break;
            }
        }
        // A sync flush ends with an empty stored block; RFC 7692 §7.2.1 drops its 4 bytes.
        debug_assert!(out.ends_with(&TAIL));
        out.truncate(out.len().saturating_sub(4));
        Bytes::from(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agreed(header: &str) -> Option<(Option<u8>, String)> {
        negotiate(header).map(|(n, r)| (n.out_window, r))
    }

    #[test]
    fn negotiation_follows_uwebsockets_for_browser_offers() {
        // Chrome
        assert_eq!(
            agreed("permessage-deflate; client_max_window_bits"),
            Some((Some(15), RESPONSE.to_string()))
        );
        // Firefox / Safari
        assert_eq!(
            agreed("permessage-deflate"),
            Some((Some(15), RESPONSE.to_string()))
        );
        assert_eq!(
            agreed("PerMessage-Deflate; Client_No_Context_Takeover; server_no_context_takeover"),
            Some((Some(15), RESPONSE.to_string()))
        );
    }

    #[test]
    fn server_max_window_bits_is_echoed_and_8_disables_outgoing_compression() {
        assert_eq!(
            agreed("permessage-deflate; server_max_window_bits=10"),
            Some((Some(10), format!("{RESPONSE}; server_max_window_bits=10")))
        );
        assert_eq!(
            agreed("permessage-deflate; server_max_window_bits=\"9\""),
            Some((Some(9), format!("{RESPONSE}; server_max_window_bits=9")))
        );
        assert_eq!(
            agreed("permessage-deflate; server_max_window_bits=8"),
            Some((None, format!("{RESPONSE}; server_max_window_bits=8")))
        );
    }

    #[test]
    fn bad_offers_are_declined_and_the_next_one_is_tried() {
        for bad in [
            "permessage-deflate; server_max_window_bits",
            "permessage-deflate; server_max_window_bits=16",
            "permessage-deflate; server_max_window_bits=7",
            "permessage-deflate; server_max_window_bits=010",
            "permessage-deflate; client_max_window_bits=20",
            "permessage-deflate; server_no_context_takeover=1",
            "permessage-deflate; client_no_context_takeover; client_no_context_takeover",
            "permessage-deflate; unknown",
            "x-webkit-deflate-frame",
            "",
        ] {
            assert_eq!(agreed(bad), None, "{bad}");
        }
        assert_eq!(
            agreed("permessage-deflate; foo, permessage-deflate; server_max_window_bits=12"),
            Some((Some(12), format!("{RESPONSE}; server_max_window_bits=12")))
        );
        assert_eq!(
            agreed("x-webkit-deflate-frame, permessage-deflate"),
            Some((Some(15), RESPONSE.to_string()))
        );
    }

    fn inflated(compressed: &[u8], max: usize) -> Result<Vec<u8>, u16> {
        inflate(compressed, max, |m| m.to_vec())
    }

    #[test]
    fn rfc_7692_examples() {
        // §7.2.3.1: "Hello" in one compressed frame.
        let hello = [0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
        assert_eq!(inflated(&hello, 100).unwrap(), b"Hello");
        // §7.2.3.3: no compression (a stored block).
        assert_eq!(
            inflated(
                &[
                    0x00, 0x05, 0x00, 0xfa, 0xff, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00
                ],
                100
            )
            .unwrap(),
            b"Hello"
        );
        // §7.2.3.5: two deflate blocks in one message.
        assert_eq!(
            inflated(
                &[
                    0xf2, 0x48, 0x05, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0xc9, 0xc9, 0x07, 0x00
                ],
                100
            )
            .unwrap(),
            b"Hello"
        );
        // An empty message.
        assert_eq!(inflated(&[0x00], 100).unwrap(), b"");
        assert_eq!(inflated(&[], 100).unwrap(), b"");
    }

    #[test]
    fn inflate_limits_and_errors() {
        let big = deflate(&vec![b'a'; 100_000], 15);
        assert!(big.len() < 1000);
        assert_eq!(inflated(&big, 100_000).unwrap().len(), 100_000);
        assert_eq!(inflated(&big, 99_999), Err(close::TOO_BIG));
        assert_eq!(
            inflated(&[0xff, 0xff, 0xff, 0xff], 100),
            Err(close::INVALID_DATA)
        );
        // The inflater is reset after an error.
        assert_eq!(
            inflated(&[0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00], 100).unwrap(),
            b"Hello"
        );
    }

    #[test]
    fn deflate_round_trips_for_every_window() {
        let text: Vec<u8> = (0..50_000u32)
            .flat_map(|i| (i % 251).to_le_bytes())
            .collect();
        for window in 9..=15 {
            for data in [&b""[..], b"Hello", &text] {
                let compressed = deflate(data, window);
                assert_eq!(
                    inflated(&compressed, usize::MAX - 1).unwrap(),
                    data,
                    "window {window}"
                );
                // Independent decoder: miniz_oxide with the tail appended.
                let mut tail = compressed.to_vec();
                tail.extend_from_slice(&TAIL);
                let mut decoder =
                    miniz_oxide::inflate::stream::InflateState::new(miniz_oxide::DataFormat::Raw);
                let mut out = vec![0; data.len() + 16];
                let r = miniz_oxide::inflate::stream::inflate(
                    &mut decoder,
                    &tail,
                    &mut out,
                    miniz_oxide::MZFlush::Sync,
                );
                assert_eq!(&out[..r.bytes_written], data);
            }
        }
    }

    mod prop {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// Messages compressed by an independent encoder (miniz_oxide, ending with a final
            /// block) inflate to the original.
            #[test]
            fn independent_encoder(data in proptest::collection::vec(any::<u8>(), 0..20_000),
                                   repeat in 1usize..20, level in 0u8..10) {
                let data: Vec<u8> = data.iter().copied().cycle().take(data.len() * repeat).collect();
                let compressed = miniz_oxide::deflate::compress_to_vec(&data, level);
                prop_assert_eq!(inflated(&compressed, data.len()).unwrap(), data.clone());
                if !data.is_empty() {
                    prop_assert_eq!(inflated(&compressed, data.len() - 1), Err(close::TOO_BIG));
                }
            }
        }
    }
}
