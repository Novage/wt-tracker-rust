//! Sans-IO WebSocket (RFC 6455) server framing: parse client frames from a byte slice, unmask
//! them in place, reassemble fragmented messages, and encode server frame headers.

use std::ops::Range;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpCode {
    Continuation = 0,
    Text = 1,
    Binary = 2,
    Close = 8,
    Ping = 9,
    Pong = 10,
}

impl OpCode {
    fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0 => Self::Continuation,
            1 => Self::Text,
            2 => Self::Binary,
            8 => Self::Close,
            9 => Self::Ping,
            10 => Self::Pong,
            _ => return None,
        })
    }

    pub fn is_control(self) -> bool {
        (self as u8) & 0x8 != 0
    }
}

/// Close status codes we send.
pub mod close {
    pub const NORMAL: u16 = 1000;
    pub const PROTOCOL_ERROR: u16 = 1002;
    pub const INVALID_DATA: u16 = 1007;
    pub const POLICY: u16 = 1008;
    pub const TOO_BIG: u16 = 1009;
}

/// One complete frame; `payload` is a range of the parsed buffer, already unmasked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub fin: bool,
    pub opcode: OpCode,
    pub payload: Range<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    /// A frame, and the bytes it occupied.
    Frame(Frame, usize),
    /// Not enough bytes yet. `Some(total)`: the frame needs `total` bytes in all.
    Incomplete(Option<usize>),
    /// The connection must be closed with this code.
    Error(u16),
}

/// Parses one client frame at the start of `buf` (unmasking its payload in place).
/// `max_payload`: the largest acceptable frame payload.
pub fn parse_frame(buf: &mut [u8], max_payload: usize) -> Parsed {
    if buf.len() < 2 {
        return Parsed::Incomplete(None);
    }
    let (b0, b1) = (buf[0], buf[1]);
    if b0 & 0x70 != 0 {
        // RSV bits: no extensions are negotiated.
        return Parsed::Error(close::PROTOCOL_ERROR);
    }
    let Some(opcode) = OpCode::from_u8(b0 & 0x0f) else {
        return Parsed::Error(close::PROTOCOL_ERROR);
    };
    let fin = b0 & 0x80 != 0;
    if b1 & 0x80 == 0 {
        // Client frames must be masked.
        return Parsed::Error(close::PROTOCOL_ERROR);
    }
    let (len, header) = match b1 & 0x7f {
        126 => {
            if buf.len() < 4 {
                return Parsed::Incomplete(None);
            }
            (u16::from_be_bytes([buf[2], buf[3]]) as u64, 4)
        }
        127 => {
            if buf.len() < 10 {
                return Parsed::Incomplete(None);
            }
            let len = u64::from_be_bytes(buf[2..10].try_into().unwrap());
            if len >> 63 != 0 {
                return Parsed::Error(close::PROTOCOL_ERROR);
            }
            (len, 10)
        }
        n => (n as u64, 2),
    };
    if opcode.is_control() && (!fin || len > 125) {
        return Parsed::Error(close::PROTOCOL_ERROR);
    }
    if len > max_payload as u64 {
        return Parsed::Error(close::TOO_BIG);
    }
    let start = header + 4;
    let total = start + len as usize;
    if buf.len() < total {
        return Parsed::Incomplete(Some(total));
    }
    let mask: [u8; 4] = buf[header..start].try_into().unwrap();
    unmask(&mut buf[start..total], mask);
    Parsed::Frame(
        Frame {
            fin,
            opcode,
            payload: start..total,
        },
        total,
    )
}

/// XORs `data` with the repeating 4-byte `mask`, 8 bytes at a time.
pub fn unmask(data: &mut [u8], mask: [u8; 4]) {
    let word = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    let (chunks, rest) = data.as_chunks_mut::<8>();
    for chunk in chunks {
        *chunk = (u64::from_ne_bytes(*chunk) ^ word).to_ne_bytes();
    }
    for (i, b) in rest.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
}

/// Header of an unmasked server frame: bytes and length.
pub fn header(opcode: OpCode, len: usize) -> ([u8; 10], usize) {
    let mut h = [0u8; 10];
    h[0] = 0x80 | opcode as u8;
    let n = if len < 126 {
        h[1] = len as u8;
        2
    } else if len < 65536 {
        h[1] = 126;
        h[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        4
    } else {
        h[1] = 127;
        h[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    (h, n)
}

/// The status code of a received close frame payload, or the code to fail with.
/// `Ok(None)`: no code given.
pub fn close_code(payload: &[u8]) -> Result<Option<u16>, u16> {
    match payload.len() {
        0 => Ok(None),
        1 => Err(close::PROTOCOL_ERROR),
        _ => {
            let code = u16::from_be_bytes([payload[0], payload[1]]);
            let valid = matches!(code, 1000..=1003 | 1007..=1011 | 3000..=4999);
            if !valid {
                return Err(close::PROTOCOL_ERROR);
            }
            if std::str::from_utf8(&payload[2..]).is_err() {
                return Err(close::INVALID_DATA);
            }
            Ok(Some(code))
        }
    }
}

/// State of a fragmented message being reassembled. Empty (no allocation) between messages.
#[derive(Default)]
pub struct Fragments {
    opcode: Option<OpCode>,
    data: Vec<u8>,
}

/// What a data frame means for the current message.
pub enum Data<'a> {
    /// A complete message: `(text, payload)`, borrowed from the frame or the reassembled data.
    Message(bool, &'a [u8]),
    /// A fragment was buffered; the message is not complete yet.
    Partial,
}

impl Fragments {
    #[cfg(test)]
    pub fn in_progress(&self) -> bool {
        self.opcode.is_some()
    }

    /// Feeds a data frame (text, binary or continuation) with its payload.
    pub fn push<'a>(
        &'a mut self,
        frame: &Frame,
        payload: &'a [u8],
        max_message: usize,
    ) -> Result<Data<'a>, u16> {
        match (frame.opcode, self.opcode) {
            (OpCode::Text | OpCode::Binary, None) if frame.fin => {
                Ok(Data::Message(frame.opcode == OpCode::Text, payload))
            }
            (OpCode::Text | OpCode::Binary, None) => {
                self.opcode = Some(frame.opcode);
                self.data.extend_from_slice(payload);
                Ok(Data::Partial)
            }
            (OpCode::Continuation, Some(opcode)) => {
                if self.data.len() + payload.len() > max_message {
                    return Err(close::TOO_BIG);
                }
                self.data.extend_from_slice(payload);
                if !frame.fin {
                    return Ok(Data::Partial);
                }
                self.opcode = None;
                Ok(Data::Message(opcode == OpCode::Text, &self.data))
            }
            // A new data message inside a fragmented one, or a continuation of nothing.
            _ => Err(close::PROTOCOL_ERROR),
        }
    }

    /// Frees the reassembly buffer after a completed message was handled.
    pub fn reset(&mut self) {
        if self.opcode.is_none() {
            self.data = Vec::new();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A masked client frame (the test-side encoder).
    pub fn client_frame(fin: bool, opcode: u8, payload: &[u8], mask: [u8; 4]) -> Vec<u8> {
        let mut out = vec![(fin as u8) << 7 | opcode];
        match payload.len() {
            n if n < 126 => out.push(0x80 | n as u8),
            n if n < 65536 => {
                out.push(0x80 | 126);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            }
            n => {
                out.push(0x80 | 127);
                out.extend_from_slice(&(n as u64).to_be_bytes());
            }
        }
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        out
    }

    #[test]
    fn rfc_6455_masked_hello() {
        // RFC 6455 §5.7: a single-frame masked text message "Hello".
        let mut buf = vec![
            0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
        ];
        match parse_frame(&mut buf, 1000) {
            Parsed::Frame(f, 11) => {
                assert!(f.fin);
                assert_eq!(f.opcode, OpCode::Text);
                assert_eq!(&buf[f.payload], b"Hello");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn lengths_at_every_boundary_and_split_points() {
        for len in [0, 1, 7, 8, 9, 125, 126, 127, 65535, 65536, 70000] {
            let payload: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
            let frame = client_frame(true, 2, &payload, [9, 8, 7, 6]);
            // Every prefix is incomplete.
            for cut in [0, 1, 2, 3, 9, frame.len() - 1] {
                if cut < frame.len() {
                    let mut part = frame[..cut].to_vec();
                    assert!(
                        matches!(parse_frame(&mut part, 1 << 20), Parsed::Incomplete(_)),
                        "len {len} cut {cut}"
                    );
                }
            }
            let mut buf = frame.clone();
            buf.extend_from_slice(b"next");
            match parse_frame(&mut buf, 1 << 20) {
                Parsed::Frame(f, used) => {
                    assert_eq!(used, frame.len());
                    assert_eq!(&buf[f.payload], &payload[..], "len {len}");
                }
                other => panic!("len {len}: {other:?}"),
            }
        }
    }

    #[test]
    fn incomplete_reports_total_once_the_header_is_known() {
        let frame = client_frame(true, 1, &[b'x'; 300], [1, 2, 3, 4]);
        let mut part = frame[..10].to_vec();
        assert_eq!(
            parse_frame(&mut part, 1000),
            Parsed::Incomplete(Some(frame.len()))
        );
    }

    #[test]
    fn protocol_errors() {
        let ok = client_frame(true, 1, b"x", [1, 2, 3, 4]);
        let mut rsv = ok.clone();
        rsv[0] |= 0x40;
        let mut unmasked = vec![0x81, 0x01, b'x'];
        let mut bad_opcode = ok.clone();
        bad_opcode[0] = 0x83;
        let mut long_ping = client_frame(true, 9, &[0; 126], [1, 2, 3, 4]);
        let mut fragmented_ping = client_frame(false, 9, b"x", [1, 2, 3, 4]);
        for (name, buf) in [
            ("rsv", &mut rsv),
            ("unmasked", &mut unmasked),
            ("opcode", &mut bad_opcode),
            ("long ping", &mut long_ping),
            ("fragmented ping", &mut fragmented_ping),
        ] {
            assert_eq!(
                parse_frame(buf, 1000),
                Parsed::Error(close::PROTOCOL_ERROR),
                "{name}"
            );
        }
        let mut big = client_frame(true, 1, &[0; 1001], [1, 2, 3, 4]);
        assert_eq!(parse_frame(&mut big, 1000), Parsed::Error(close::TOO_BIG));
    }

    #[test]
    fn fragments_reassemble_and_reject_misuse() {
        let mut f = Fragments::default();
        let frame = |fin, opcode| Frame {
            fin,
            opcode,
            payload: 0..0,
        };
        assert!(matches!(
            f.push(&frame(false, OpCode::Text), b"he", 100),
            Ok(Data::Partial)
        ));
        assert!(matches!(
            f.push(&frame(false, OpCode::Continuation), b"ll", 100),
            Ok(Data::Partial)
        ));
        match f.push(&frame(true, OpCode::Continuation), b"o", 100) {
            Ok(Data::Message(true, m)) => assert_eq!(m, b"hello"),
            _ => panic!(),
        }
        f.reset();
        assert!(!f.in_progress());
        assert!(matches!(
            f.push(&frame(true, OpCode::Continuation), b"x", 100),
            Err(close::PROTOCOL_ERROR)
        ));
        assert!(matches!(
            f.push(&frame(false, OpCode::Binary), b"x", 100),
            Ok(Data::Partial)
        ));
        assert!(matches!(
            f.push(&frame(true, OpCode::Text), b"x", 100),
            Err(close::PROTOCOL_ERROR)
        ));
        let mut f = Fragments::default();
        assert!(matches!(
            f.push(&frame(false, OpCode::Text), &[0; 60], 100),
            Ok(Data::Partial)
        ));
        assert!(matches!(
            f.push(&frame(true, OpCode::Continuation), &[0; 60], 100),
            Err(close::TOO_BIG)
        ));
    }

    #[test]
    fn server_headers() {
        assert_eq!(
            header(OpCode::Text, 5),
            ([0x81, 5, 0, 0, 0, 0, 0, 0, 0, 0], 2)
        );
        assert_eq!(header(OpCode::Binary, 300).0[..4], [0x82, 126, 1, 44]);
        assert_eq!(header(OpCode::Text, 70000).1, 10);
    }

    #[test]
    fn close_codes() {
        assert_eq!(close_code(b""), Ok(None));
        assert_eq!(close_code(&[3]), Err(close::PROTOCOL_ERROR));
        assert_eq!(close_code(&1000u16.to_be_bytes()), Ok(Some(1000)));
        assert_eq!(
            close_code(&1005u16.to_be_bytes()),
            Err(close::PROTOCOL_ERROR)
        );
        assert_eq!(
            close_code(&999u16.to_be_bytes()),
            Err(close::PROTOCOL_ERROR)
        );
        assert_eq!(close_code(&4000u16.to_be_bytes()), Ok(Some(4000)));
        assert_eq!(close_code(&[3, 232, 0xff]), Err(close::INVALID_DATA));
    }

    #[test]
    fn unmask_matches_bytewise_for_all_lengths_and_masks() {
        for len in 0..40 {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let mask = [0xa1, 0x02, 0xff, 0x30];
            let mut fast = data.clone();
            unmask(&mut fast, mask);
            let slow: Vec<u8> = data
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ mask[i % 4])
                .collect();
            assert_eq!(fast, slow, "len {len}");
        }
    }

    mod oracle {
        //! tungstenite (an independent implementation) encodes masked client frames; our parser
        //! and reassembly, fed the byte stream in random chunks, must recover the messages.
        use super::super::*;
        use proptest::prelude::*;
        use tokio_tungstenite::tungstenite::protocol::frame::Frame as TFrame;
        use tokio_tungstenite::tungstenite::protocol::frame::coding::{
            Data as TData, OpCode as TOp,
        };

        #[derive(Debug, Clone)]
        struct Msg {
            data: Vec<u8>,
            text: bool,
            parts: usize,
            ping_between: bool,
        }

        fn msg() -> impl Strategy<Value = Msg> {
            let size = prop_oneof![
                6 => 0usize..200,
                2 => 120usize..140,
                1 => 65530usize..65542,
            ];
            (size, any::<bool>(), 1usize..5, any::<bool>(), any::<u64>()).prop_map(
                |(len, text, parts, ping_between, seed)| {
                    let mut x = seed | 1;
                    let data = (0..len)
                        .map(|_| {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            x as u8
                        })
                        .collect();
                    Msg {
                        data,
                        text,
                        parts,
                        ping_between,
                    }
                },
            )
        }

        fn encode(msgs: &[Msg], mask_seed: u32) -> Vec<u8> {
            let mut out = Vec::new();
            let mut mask = mask_seed;
            let mut write = |mut frame: TFrame, out: &mut Vec<u8>| {
                mask = mask.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                frame.header_mut().mask = Some(mask.to_be_bytes());
                frame.format(out).unwrap();
            };
            for m in msgs {
                let chunk = m.data.len().div_ceil(m.parts).max(1);
                let pieces: Vec<&[u8]> = if m.data.is_empty() {
                    vec![&[][..]]
                } else {
                    m.data.chunks(chunk).collect()
                };
                for (i, piece) in pieces.iter().enumerate() {
                    let opcode = match (i, m.text) {
                        (0, true) => TOp::Data(TData::Text),
                        (0, false) => TOp::Data(TData::Binary),
                        _ => TOp::Data(TData::Continue),
                    };
                    write(
                        TFrame::message(piece.to_vec(), opcode, i + 1 == pieces.len()),
                        &mut out,
                    );
                    if m.ping_between && i + 1 < pieces.len() {
                        write(TFrame::ping(b"p".to_vec()), &mut out);
                    }
                }
            }
            out
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(128))]

            #[test]
            fn decodes_what_tungstenite_encodes(
                msgs in prop::collection::vec(msg(), 1..8),
                mask_seed in any::<u32>(),
                chunk_sizes in prop::collection::vec(1usize..3000, 1..64),
            ) {
                let stream = encode(&msgs, mask_seed);
                let expected_pings = msgs.iter().filter(|m| m.ping_between).map(|m| {
                    let chunk = m.data.len().div_ceil(m.parts).max(1);
                    m.data.len().div_ceil(chunk).max(1) - 1
                }).sum::<usize>();

                let (mut got, mut pings) = (Vec::new(), 0);
                let mut fragments = Fragments::default();
                let mut pending: Vec<u8> = Vec::new();
                let (mut at, mut c) = (0, 0);
                while at < stream.len() {
                    let n = chunk_sizes[c % chunk_sizes.len()].min(stream.len() - at);
                    c += 1;
                    let mut buf = std::mem::take(&mut pending);
                    buf.extend_from_slice(&stream[at..at + n]);
                    at += n;
                    let mut pos = 0;
                    loop {
                        match parse_frame(&mut buf[pos..], 1 << 20) {
                            Parsed::Frame(frame, used) => {
                                let payload = pos + frame.payload.start..pos + frame.payload.end;
                                pos += used;
                                if frame.opcode == OpCode::Ping {
                                    pings += 1;
                                    continue;
                                }
                                match fragments.push(&frame, &buf[payload], 1 << 20) {
                                    Ok(Data::Message(text, data)) => got.push((text, data.to_vec())),
                                    Ok(Data::Partial) => {}
                                    Err(code) => prop_assert!(false, "close {}", code),
                                }
                                fragments.reset();
                            }
                            Parsed::Incomplete(_) => break,
                            Parsed::Error(code) => prop_assert!(false, "close {}", code),
                        }
                    }
                    pending = buf[pos..].to_vec();
                }
                prop_assert!(pending.is_empty());
                let expected: Vec<(bool, Vec<u8>)> = msgs.iter().map(|m| (m.text, m.data.clone())).collect();
                prop_assert_eq!(got, expected);
                prop_assert_eq!(pings, expected_pings);
            }
        }
    }
}
