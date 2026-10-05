use std::fmt;

use crate::Key;

/// Opaque connection handle supplied by the I/O layer. One connection may carry many peer_ids.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct ConnId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AnnounceEvent {
    /// No `event` field: a periodic re-announce.
    None,
    Started,
    Completed,
}

#[derive(Clone, Copy, Debug)]
pub enum ScrapeTarget<'a> {
    /// No `info_hash` field: scrape every swarm.
    All,
    One(&'a [u8]),
    Many(&'a [&'a [u8]]),
}

/// A request already decoded from the wire. `O` is the opaque offer/answer payload; the core
/// never inspects it, only hands it back through [`Outbox`].
///
/// Mapping from the JSON protocol (done by the protocol layer):
/// - `action: "announce"`, `event` absent, `answer` present → [`Request::Answer`]
/// - `action: "announce"`, `event` absent / `"started"` / `"completed"` → [`Request::Announce`]
/// - `action: "announce"`, `event: "stopped"` → [`Request::Stop`]
/// - `action: "scrape"` → [`Request::Scrape`]
#[derive(Clone, Copy, Debug)]
pub enum Request<'a, O> {
    Announce {
        info_hash: &'a [u8],
        peer_id: &'a [u8],
        event: AnnounceEvent,
        /// `left == 0`: the peer has the whole content.
        left_zero: bool,
        /// `None` when `numwant` is missing or not an integer: no offers are sent.
        numwant: Option<u32>,
        offers: Option<&'a [O]>,
    },
    /// Delivered only if `info_hash` names a swarm, the sender `peer_id` is a peer of the
    /// requesting connection in it, and `to_peer_id` is in it too; otherwise dropped
    /// ([`Outbox::answer_dropped`], spec §5.3).
    Answer {
        info_hash: &'a [u8],
        peer_id: &'a [u8],
        to_peer_id: &'a [u8],
        answer: &'a O,
    },
    Stop {
        info_hash: &'a [u8],
        peer_id: &'a [u8],
    },
    Scrape {
        target: ScrapeTarget<'a>,
    },
}

/// Receives everything the shard wants to send. Called synchronously while a request is
/// processed; borrowed arguments must be copied/serialized before returning.
#[allow(unused_variables)]
pub trait Outbox<O> {
    fn announce_reply(
        &mut self,
        to: ConnId,
        info_hash: &Key,
        interval: u32,
        complete: u32,
        incomplete: u32,
    ) {
    }

    fn offer(&mut self, to: ConnId, from_peer_id: &Key, info_hash: &Key, offer: &O) {}

    fn answer(&mut self, to: ConnId, answer: &O) {}

    /// An answer was not delivered (spec §5.3): unknown swarm or peer, a sender of another
    /// connection, or a sender or target outside the swarm.
    fn answer_dropped(&mut self) {}

    fn scrape_entry(
        &mut self,
        to: ConnId,
        info_hash: &[u8],
        complete: u32,
        incomplete: u32,
        downloaded: u32,
    ) {
    }

    /// Ends a scrape reply; sent even when there were no entries.
    fn scrape_end(&mut self, to: ConnId) {}

    /// A peer was removed (disconnect, stop of its last swarm, expiry, or connection change).
    fn peer_removed(&mut self, peer_id: &Key, conn: ConnId) {}
}

/// An [`Outbox`] that drops everything.
pub struct NullOutbox;

impl<O> Outbox<O> for NullOutbox {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TrackerError {
    /// `info_hash` or `peer_id` is longer than [`crate::MAX_KEY_LEN`].
    KeyTooLong,
}

impl fmt::Display for TrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::KeyTooLong => "info_hash or peer_id is too long",
        })
    }
}

impl std::error::Error for TrackerError {}
