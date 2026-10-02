//! Owned, `Send` form of a parsed [`Message`]: the frame as [`Bytes`] plus offsets into it, so a
//! message parsed on one thread can be applied on another without copying or re-parsing.

use std::borrow::Cow;

use bytes::Bytes;
use smallvec::SmallVec;
use wt_core::{AnnounceEvent, Key};

use crate::{INLINE_OFFERS, Message, Payload};

/// A slice of the frame as an offset range.
#[derive(Clone, Copy, Debug)]
struct Span {
    start: u32,
    len: u32,
}

#[derive(Clone, Copy, Debug)]
enum OwnedPayload {
    Offer {
        offer_id: Option<Span>,
        sdp: Option<Span>,
    },
    Answer {
        head: Span,
        tail: Span,
    },
}

#[allow(clippy::large_enum_variant)] // inline on purpose: no allocation per message
#[derive(Debug)]
enum Kind {
    Announce {
        info_hash: Key,
        peer_id: Key,
        event: AnnounceEvent,
        left_zero: bool,
        numwant: Option<u32>,
        offers: Option<SmallVec<[OwnedPayload; INLINE_OFFERS]>>,
    },
    Answer {
        info_hash: Option<Span>,
        to_peer_id: Key,
        answer: OwnedPayload,
    },
    Stop {
        info_hash: Option<Key>,
        peer_id: Option<Key>,
    },
    Scrape {
        info_hashes: Option<SmallVec<[Vec<u8>; 4]>>,
    },
}

/// A [`Message`] that owns its frame. Built with [`OwnedMessage::new`], read back with
/// [`OwnedMessage::message`].
#[derive(Debug)]
pub struct OwnedMessage {
    frame: Bytes,
    kind: Kind,
}

impl OwnedMessage {
    /// `message` must have been parsed from `frame` (its slices point into it). No copy.
    pub fn new(frame: Bytes, message: &Message<'_>) -> Self {
        let (base, len) = (frame.as_ptr() as usize, frame.len());
        Self::build(frame, base, len, message)
    }

    /// `message` was parsed from the borrowed `frame` (e.g. a shared read buffer): copies the
    /// frame once into owned `Bytes`.
    pub fn copy_from(frame: &[u8], message: &Message<'_>) -> Self {
        Self::build(
            Bytes::copy_from_slice(frame),
            frame.as_ptr() as usize,
            frame.len(),
            message,
        )
    }

    /// `base` / `base_len`: the buffer the message slices point into; `frame` holds the same
    /// bytes.
    fn build(frame: Bytes, base: usize, base_len: usize, message: &Message<'_>) -> Self {
        let span = |slice: &[u8]| -> Span {
            let start = (slice.as_ptr() as usize)
                .checked_sub(base)
                .filter(|&s| s + slice.len() <= base_len)
                .expect("message slice outside of its frame");
            Span {
                start: start as u32,
                len: slice.len() as u32,
            }
        };
        let payload = |p: &Payload<'_>| match *p {
            Payload::Offer { offer_id, sdp } => OwnedPayload::Offer {
                offer_id: offer_id.map(span),
                sdp: sdp.map(span),
            },
            Payload::Answer { head, tail } => OwnedPayload::Answer {
                head: span(head),
                tail: span(tail),
            },
        };
        let kind = match message {
            Message::Announce {
                info_hash,
                peer_id,
                event,
                left_zero,
                numwant,
                offers,
            } => Kind::Announce {
                info_hash: *info_hash,
                peer_id: *peer_id,
                event: *event,
                left_zero: *left_zero,
                numwant: *numwant,
                offers: offers.as_ref().map(|o| o.iter().map(payload).collect()),
            },
            Message::Answer {
                info_hash,
                to_peer_id,
                answer,
            } => Kind::Answer {
                info_hash: info_hash.map(span),
                to_peer_id: *to_peer_id,
                answer: payload(answer),
            },
            Message::Stop { info_hash, peer_id } => Kind::Stop {
                info_hash: *info_hash,
                peer_id: *peer_id,
            },
            Message::Scrape { info_hashes } => Kind::Scrape {
                info_hashes: info_hashes
                    .as_ref()
                    .map(|hashes| hashes.iter().map(|h| h.to_vec()).collect()),
            },
        };
        Self { frame, kind }
    }

    pub fn frame(&self) -> &Bytes {
        &self.frame
    }

    /// The message, borrowing from the owned frame.
    pub fn message(&self) -> Message<'_> {
        let slice = |s: Span| &self.frame[s.start as usize..(s.start + s.len) as usize];
        let payload = |p: &OwnedPayload| match *p {
            OwnedPayload::Offer { offer_id, sdp } => Payload::Offer {
                offer_id: offer_id.map(slice),
                sdp: sdp.map(slice),
            },
            OwnedPayload::Answer { head, tail } => Payload::Answer {
                head: slice(head),
                tail: slice(tail),
            },
        };
        match &self.kind {
            Kind::Announce {
                info_hash,
                peer_id,
                event,
                left_zero,
                numwant,
                offers,
            } => Message::Announce {
                info_hash: *info_hash,
                peer_id: *peer_id,
                event: *event,
                left_zero: *left_zero,
                numwant: *numwant,
                offers: offers.as_ref().map(|o| o.iter().map(payload).collect()),
            },
            Kind::Answer {
                info_hash,
                to_peer_id,
                answer,
            } => Message::Answer {
                info_hash: info_hash.map(slice),
                to_peer_id: *to_peer_id,
                answer: payload(answer),
            },
            Kind::Stop { info_hash, peer_id } => Message::Stop {
                info_hash: *info_hash,
                peer_id: *peer_id,
            },
            Kind::Scrape { info_hashes } => Message::Scrape {
                info_hashes: info_hashes
                    .as_ref()
                    .map(|hashes| hashes.iter().map(|h| Cow::Borrowed(h.as_slice())).collect()),
            },
        }
    }
}
