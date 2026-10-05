#![allow(dead_code)]

use wt_core::{AnnounceEvent, ConnId, Key, Outbox, Request, Shard, TrackerError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Reply {
        to: ConnId,
        info_hash: Vec<u8>,
        interval: u32,
        complete: u32,
        incomplete: u32,
    },
    Offer {
        to: ConnId,
        from: Vec<u8>,
        info_hash: Vec<u8>,
        offer: u32,
    },
    Answer {
        to: ConnId,
        answer: u32,
    },
    AnswerDropped,
    ScrapeEntry {
        to: ConnId,
        info_hash: Vec<u8>,
        complete: u32,
        incomplete: u32,
        downloaded: u32,
    },
    ScrapeEnd {
        to: ConnId,
    },
    Removed {
        peer_id: Vec<u8>,
        conn: ConnId,
    },
}

#[derive(Default)]
pub struct Recorder {
    pub events: Vec<Event>,
}

impl Recorder {
    pub fn take(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    pub fn offers(&self) -> Vec<(ConnId, u32)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                Event::Offer { to, offer, .. } => Some((*to, *offer)),
                _ => None,
            })
            .collect()
    }

    pub fn removed(&self) -> Vec<(Vec<u8>, ConnId)> {
        let mut removed: Vec<_> = self
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Removed { peer_id, conn } => Some((peer_id.clone(), *conn)),
                _ => None,
            })
            .collect();
        removed.sort();
        removed
    }
}

impl Outbox<u32> for Recorder {
    fn announce_reply(
        &mut self,
        to: ConnId,
        info_hash: &Key,
        interval: u32,
        complete: u32,
        incomplete: u32,
    ) {
        self.events.push(Event::Reply {
            to,
            info_hash: info_hash.as_bytes().to_vec(),
            interval,
            complete,
            incomplete,
        });
    }

    fn offer(&mut self, to: ConnId, from_peer_id: &Key, info_hash: &Key, offer: &u32) {
        self.events.push(Event::Offer {
            to,
            from: from_peer_id.as_bytes().to_vec(),
            info_hash: info_hash.as_bytes().to_vec(),
            offer: *offer,
        });
    }

    fn answer(&mut self, to: ConnId, answer: &u32) {
        self.events.push(Event::Answer {
            to,
            answer: *answer,
        });
    }

    fn answer_dropped(&mut self) {
        self.events.push(Event::AnswerDropped);
    }

    fn scrape_entry(
        &mut self,
        to: ConnId,
        info_hash: &[u8],
        complete: u32,
        incomplete: u32,
        downloaded: u32,
    ) {
        self.events.push(Event::ScrapeEntry {
            to,
            info_hash: info_hash.to_vec(),
            complete,
            incomplete,
            downloaded,
        });
    }

    fn scrape_end(&mut self, to: ConnId) {
        self.events.push(Event::ScrapeEnd { to });
    }

    fn peer_removed(&mut self, peer_id: &Key, conn: ConnId) {
        self.events.push(Event::Removed {
            peer_id: peer_id.as_bytes().to_vec(),
            conn,
        });
    }
}

pub const OFFERS: [u32; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

pub fn announce(
    shard: &mut Shard,
    out: &mut Recorder,
    now: u32,
    conn: u64,
    info_hash: &str,
    peer_id: &str,
    event: AnnounceEvent,
) -> Result<(), TrackerError> {
    announce_with(
        shard,
        out,
        now,
        conn,
        info_hash,
        peer_id,
        event,
        Some(&OFFERS),
        Some(100),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn announce_with(
    shard: &mut Shard,
    out: &mut Recorder,
    now: u32,
    conn: u64,
    info_hash: &str,
    peer_id: &str,
    event: AnnounceEvent,
    offers: Option<&[u32]>,
    numwant: Option<u32>,
) -> Result<(), TrackerError> {
    shard.handle(
        now,
        ConnId(conn),
        Request::Announce {
            info_hash: info_hash.as_bytes(),
            peer_id: peer_id.as_bytes(),
            event,
            left_zero: false,
            numwant,
            offers,
        },
        out,
    )
}

pub fn stop(shard: &mut Shard, out: &mut Recorder, conn: u64, info_hash: &str, peer_id: &str) {
    shard
        .handle(
            0,
            ConnId(conn),
            Request::Stop {
                info_hash: info_hash.as_bytes(),
                peer_id: peer_id.as_bytes(),
            },
            out,
        )
        .unwrap();
}

/// Sorted peer_ids of a swarm as strings; empty if the swarm does not exist.
pub fn swarm_peers(shard: &Shard, info_hash: &str) -> Vec<String> {
    sorted_strings(
        shard
            .swarm_peer_ids(info_hash.as_bytes())
            .unwrap_or_default(),
    )
}

pub fn sorted_strings(keys: Vec<Key>) -> Vec<String> {
    let mut ids: Vec<String> = keys
        .iter()
        .map(|k| String::from_utf8(k.as_bytes().to_vec()).unwrap())
        .collect();
    ids.sort();
    ids
}
