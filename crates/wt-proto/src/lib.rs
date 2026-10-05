//! Zero-copy wire protocol of the WebTorrent tracker: JSON text frames → [`wt_core::Request`],
//! [`wt_core::Outbox`] events → JSON frames, byte-compatible with the JS wt-tracker.
//!
//! SDPs, `offer_id`s and answer bodies are never decoded or re-encoded: they travel as raw
//! slices of the received frame ([`Payload`]) and are copied verbatim into outgoing messages.

mod encode;
#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod fuzz;
mod json;
mod owned;
mod parse;

use std::fmt;

use smallvec::SmallVec;
use wt_core::{ConnId, Request, ScrapeTarget, Shard, TrackerError};

pub use encode::{Batch, Count, Counters, Encoder};
pub use owned::OwnedMessage;
#[cfg(feature = "sonic")]
pub use parse::Sonic;
pub use parse::{Backend, INLINE_OFFERS, Message, SerdeJson};

/// The parser used by [`handle`].
pub type DefaultBackend = SerdeJson;

/// Offer / answer payload: raw JSON text slices of the received frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Payload<'a> {
    /// `offer_id` value and `offer.sdp` value of one offer item, verbatim (absent → `None`).
    Offer {
        offer_id: Option<&'a [u8]>,
        sdp: Option<&'a [u8]>,
    },
    /// The answer message with its `to_peer_id` member removed: `head` followed by `tail`.
    Answer { head: &'a [u8], tail: &'a [u8] },
}

/// Why a frame was rejected. The JS tracker closes the connection in every case, and so should
/// the caller (then call [`Shard::disconnect`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtoError {
    /// Not valid JSON (or not valid UTF-8).
    InvalidJson,
    /// Valid JSON, but the top level is not an object.
    NotAnObject,
    /// `action` missing, not a string, or not `announce` / `scrape`.
    UnknownAction,
    /// `event` present but not `started`, `completed` or `stopped`.
    UnknownEvent,
    /// A field has the wrong type or an unsupported form.
    BadField(&'static str),
    /// `info_hash` / `peer_id` / `to_peer_id` longer than [`wt_core::MAX_KEY_LEN`] bytes.
    KeyTooLong,
    /// Rejected by the tracker core.
    Tracker(TrackerError),
}

impl From<TrackerError> for ProtoError {
    fn from(e: TrackerError) -> Self {
        Self::Tracker(e)
    }
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson => f.write_str("invalid JSON"),
            Self::NotAnObject => f.write_str("message is not a JSON object"),
            Self::UnknownAction => f.write_str("unknown action"),
            Self::UnknownEvent => f.write_str("unknown announce event"),
            Self::BadField(field) => write!(f, "{field} field is missing or wrong"),
            Self::KeyTooLong => f.write_str("info_hash or peer_id is too long"),
            Self::Tracker(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ProtoError {}

/// Parses one frame from `conn` with the [`DefaultBackend`] and applies it to `shard`;
/// outgoing messages are appended to `out`.
pub fn handle(
    shard: &mut Shard,
    now: u32,
    conn: ConnId,
    frame: &[u8],
    out: &mut Encoder,
) -> Result<(), ProtoError> {
    handle_with::<DefaultBackend>(shard, now, conn, frame, out)
}

/// [`handle`] with an explicit parser backend: [`Backend::parse`] then [`apply`].
pub fn handle_with<B: Backend>(
    shard: &mut Shard,
    now: u32,
    conn: ConnId,
    frame: &[u8],
    out: &mut Encoder,
) -> Result<(), ProtoError> {
    apply(shard, now, conn, &B::parse(frame)?, out)
}

/// Applies a parsed message from `conn` to `shard`; outgoing messages are appended to `out`.
pub fn apply(
    shard: &mut Shard,
    now: u32,
    conn: ConnId,
    message: &Message<'_>,
    out: &mut Encoder,
) -> Result<(), ProtoError> {
    match message {
        Message::Announce {
            info_hash,
            peer_id,
            event,
            left_zero,
            numwant,
            offers,
        } => shard.handle(
            now,
            conn,
            Request::Announce {
                info_hash: info_hash.as_bytes(),
                peer_id: peer_id.as_bytes(),
                event: *event,
                left_zero: *left_zero,
                numwant: *numwant,
                offers: offers.as_deref(),
            },
            out,
        )?,
        Message::Answer {
            info_hash: Some(info_hash),
            peer_id: Some(peer_id),
            to_peer_id,
            answer,
        } => shard.handle(
            now,
            conn,
            Request::Answer {
                info_hash: info_hash.as_bytes(),
                peer_id: peer_id.as_bytes(),
                to_peer_id: to_peer_id.as_bytes(),
                answer,
            },
            out,
        )?,
        // An id that cannot match anything: not delivered (spec §5.3).
        Message::Answer { .. } => wt_core::Outbox::<Payload<'_>>::answer_dropped(out),
        Message::Stop {
            info_hash: Some(info_hash),
            peer_id: Some(peer_id),
        } => shard.handle(
            now,
            conn,
            Request::Stop::<Payload<'_>> {
                info_hash: info_hash.as_bytes(),
                peer_id: peer_id.as_bytes(),
            },
            out,
        )?,
        // An id that cannot match anything: nothing to stop (same as JS).
        Message::Stop { .. } => {}
        Message::Scrape { info_hashes } => {
            let slices: SmallVec<[&[u8]; 4]>;
            let target = match info_hashes {
                None => ScrapeTarget::All,
                Some(hashes) => {
                    slices = hashes.iter().map(|h| h.as_ref()).collect();
                    ScrapeTarget::Many(&slices)
                }
            };
            shard.handle(now, conn, Request::Scrape::<Payload<'_>> { target }, out)?;
        }
    }
    Ok(())
}
