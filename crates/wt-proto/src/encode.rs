//! [`Outbox`] → JSON messages, in exactly the layout the JS tracker produces with
//! `JSON.stringify` of its message objects.

use bytes::Bytes;
use wt_core::{ConnId, Key, Outbox};

use crate::Payload;
use crate::json::{write_string, write_u32};

#[derive(Clone, Copy, Debug)]
struct OutMessage {
    to: ConnId,
    start: u32,
    end: u32,
}

/// Messages taken out of an [`Encoder`]: one buffer, messages as slices of it.
#[derive(Debug, Default)]
pub struct Batch {
    bytes: Bytes,
    messages: Vec<OutMessage>,
}

impl Batch {
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// `(receiver, message)` in emission order; each message shares the batch buffer.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (ConnId, Bytes)> + '_ {
        self.messages
            .iter()
            .map(|m| (m.to, self.bytes.slice(m.start as usize..m.end as usize)))
    }
}

struct OpenScrape {
    to: ConnId,
    start: usize,
}

/// Messages and their bytes (JSON text).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Count {
    pub messages: u64,
    pub bytes: u64,
}

impl Count {
    pub fn add(&mut self, bytes: usize) {
        self.messages += 1;
        self.bytes += bytes as u64;
    }
}

impl std::ops::AddAssign for Count {
    fn add_assign(&mut self, other: Self) {
        self.messages += other.messages;
        self.bytes += other.bytes;
    }
}

/// Everything an [`Encoder`] produced since it was created, per kind of message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub announce_replies: Count,
    pub offers: Count,
    pub answers: Count,
    pub scrapes: Count,
    /// Answers not delivered (spec §5.3).
    pub answers_dropped: u64,
}

impl std::ops::AddAssign for Counters {
    fn add_assign(&mut self, other: Self) {
        self.announce_replies += other.announce_replies;
        self.offers += other.offers;
        self.answers += other.answers;
        self.scrapes += other.scrapes;
        self.answers_dropped += other.answers_dropped;
    }
}

#[derive(Clone, Copy)]
enum Kind {
    AnnounceReply,
    Offer,
    Answer,
    Scrape,
}

/// Collects outgoing messages into one reusable buffer. Call [`Encoder::clear`] after the
/// messages were sent; buffers keep their capacity, so steady state does not allocate.
#[derive(Default)]
pub struct Encoder {
    buf: Vec<u8>,
    messages: Vec<OutMessage>,
    removed: Vec<(Key, ConnId)>,
    scrape: Option<OpenScrape>,
    /// Escaped `files` keys of the open scrape (ranges in `buf`), for de-duplication.
    scrape_keys: Vec<(u32, u32)>,
    /// Not reset by [`Encoder::clear`] / [`Encoder::take`].
    counters: Counters,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.messages.clear();
        self.removed.clear();
        self.scrape = None;
        self.scrape_keys.clear();
    }

    /// Messages in emission order: the receiving connection and one complete JSON text.
    pub fn messages(&self) -> impl ExactSizeIterator<Item = (ConnId, &[u8])> {
        self.messages
            .iter()
            .map(|m| (m.to, &self.buf[m.start as usize..m.end as usize]))
    }

    /// Peers removed while processing (JS `onRemovePeer`).
    pub fn removed(&self) -> &[(Key, ConnId)] {
        &self.removed
    }

    /// Total bytes of all messages.
    pub fn bytes(&self) -> usize {
        self.buf.len()
    }

    /// Messages and bytes produced since the encoder was created, per kind.
    pub fn counters(&self) -> Counters {
        self.counters
    }

    /// Moves the messages out as one shared buffer (no copy of message bytes) and clears the
    /// encoder. Costs one buffer allocation per batch instead of one per message.
    pub fn take(&mut self) -> Batch {
        let capacity = self.buf.capacity();
        let buf = std::mem::replace(&mut self.buf, Vec::with_capacity(capacity));
        let messages = std::mem::take(&mut self.messages);
        self.clear();
        Batch {
            bytes: Bytes::from(buf),
            messages,
        }
    }

    #[inline]
    fn finish(&mut self, to: ConnId, start: usize, kind: Kind) {
        let bytes = self.buf.len() - start;
        let c = &mut self.counters;
        match kind {
            Kind::AnnounceReply => c.announce_replies.add(bytes),
            Kind::Offer => c.offers.add(bytes),
            Kind::Answer => c.answers.add(bytes),
            Kind::Scrape => c.scrapes.add(bytes),
        }
        self.messages.push(OutMessage {
            to,
            start: start as u32,
            end: self.buf.len() as u32,
        });
    }

    fn open_scrape(&mut self, to: ConnId) {
        if self.scrape.is_none() {
            let start = self.buf.len();
            self.buf
                .extend_from_slice(b"{\"action\":\"scrape\",\"files\":{");
            self.scrape = Some(OpenScrape { to, start });
            self.scrape_keys.clear();
        }
    }
}

impl<'a> Outbox<Payload<'a>> for Encoder {
    fn announce_reply(
        &mut self,
        to: ConnId,
        info_hash: &Key,
        interval: u32,
        complete: u32,
        incomplete: u32,
    ) {
        let start = self.buf.len();
        let b = &mut self.buf;
        b.extend_from_slice(b"{\"action\":\"announce\",\"interval\":");
        write_u32(b, interval);
        b.extend_from_slice(b",\"info_hash\":");
        write_string(b, info_hash.as_bytes());
        b.extend_from_slice(b",\"complete\":");
        write_u32(b, complete);
        b.extend_from_slice(b",\"incomplete\":");
        write_u32(b, incomplete);
        b.push(b'}');
        self.finish(to, start, Kind::AnnounceReply);
    }

    fn offer(&mut self, to: ConnId, from_peer_id: &Key, info_hash: &Key, offer: &Payload<'a>) {
        let Payload::Offer { offer_id, sdp } = offer else {
            debug_assert!(false, "answer payload passed as an offer");
            return;
        };
        let start = self.buf.len();
        let b = &mut self.buf;
        b.extend_from_slice(b"{\"action\":\"announce\",\"info_hash\":");
        write_string(b, info_hash.as_bytes());
        if let Some(offer_id) = offer_id {
            b.extend_from_slice(b",\"offer_id\":");
            b.extend_from_slice(offer_id);
        }
        b.extend_from_slice(b",\"peer_id\":");
        write_string(b, from_peer_id.as_bytes());
        b.extend_from_slice(b",\"offer\":{\"type\":\"offer\"");
        if let Some(sdp) = sdp {
            b.extend_from_slice(b",\"sdp\":");
            b.extend_from_slice(sdp);
        }
        b.extend_from_slice(b"}}");
        self.finish(to, start, Kind::Offer);
    }

    fn answer(&mut self, to: ConnId, answer: &Payload<'a>) {
        let Payload::Answer { head, tail } = answer else {
            debug_assert!(false, "offer payload passed as an answer");
            return;
        };
        let start = self.buf.len();
        self.buf.extend_from_slice(head);
        self.buf.extend_from_slice(tail);
        self.finish(to, start, Kind::Answer);
    }

    fn answer_dropped(&mut self) {
        self.counters.answers_dropped += 1;
    }

    fn scrape_entry(
        &mut self,
        to: ConnId,
        info_hash: &[u8],
        complete: u32,
        incomplete: u32,
        downloaded: u32,
    ) {
        self.open_scrape(to);
        let mark = self.buf.len();
        let first = self.scrape_keys.is_empty();
        if !first {
            self.buf.push(b',');
        }
        let key_start = self.buf.len();
        write_string(&mut self.buf, info_hash);
        let key = &self.buf[key_start..];
        // A repeated hash keeps its first entry (JS object key semantics).
        if self
            .scrape_keys
            .iter()
            .any(|&(s, e)| &self.buf[s as usize..e as usize] == key)
        {
            self.buf.truncate(mark);
            return;
        }
        self.scrape_keys
            .push((key_start as u32, self.buf.len() as u32));
        let b = &mut self.buf;
        b.extend_from_slice(b":{\"complete\":");
        write_u32(b, complete);
        b.extend_from_slice(b",\"incomplete\":");
        write_u32(b, incomplete);
        b.extend_from_slice(b",\"downloaded\":");
        write_u32(b, downloaded);
        b.push(b'}');
    }

    fn scrape_end(&mut self, to: ConnId) {
        self.open_scrape(to);
        self.buf.extend_from_slice(b"}}");
        let open = self.scrape.take().expect("opened above");
        self.scrape_keys.clear();
        self.finish(open.to, open.start, Kind::Scrape);
    }

    fn peer_removed(&mut self, peer_id: &Key, conn: ConnId) {
        self.removed.push((*peer_id, conn));
    }
}
