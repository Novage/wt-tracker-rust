use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::BuildHasher;

use foldhash::fast::RandomState;
use hashbrown::HashTable;
use slab::Slab;
use smallvec::SmallVec;

use crate::{AnnounceEvent, ConnId, Key, Outbox, Request, ScrapeTarget, TrackerError};

/// How receivers are chosen when the swarm has more peers than offers to send.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OfferSelection {
    /// Contiguous window from a random start index; one random number per announce (same as
    /// the JS tracker). Neighbouring swarm entries (peers that joined at similar times) tend to
    /// receive offers together.
    RandomWindow,
    /// Contiguous window from a per-swarm cursor that continues where the previous announce
    /// stopped. No RNG; spreads incoming offers evenly over the swarm.
    RoundRobin,
    /// Uniformly random set of distinct peers (Floyd's algorithm): one random number per offer.
    /// The default.
    RandomSample,
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Maximum number of offers forwarded per announce.
    pub max_offers: u32,
    /// Announce interval in seconds sent to clients; memberships idle for more than twice
    /// this are removed by [`Shard::expire`].
    pub announce_interval: u32,
    pub offer_selection: OfferSelection,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_offers: 20,
            announce_interval: 20,
            offer_selection: OfferSelection::RandomSample,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SwarmStats {
    pub peers: u32,
    pub complete: u32,
}

impl SwarmStats {
    pub fn incomplete(&self) -> u32 {
        self.peers - self.complete
    }
}

type Idx = u32;

/// Swarm member entry. Holds the receiver's connection so offer fan-out reads only this array.
#[derive(Clone, Copy)]
struct Slot {
    conn: ConnId,
    member: Idx,
}

struct Swarm {
    slots: Vec<Slot>,
    completed: u32,
    cursor: u32,
    info_hash: Key,
}

#[derive(Clone, Copy)]
struct PeerMembership {
    swarm: Idx,
    member: Idx,
}

struct Peer {
    conn: ConnId,
    members: SmallVec<[PeerMembership; 2]>,
    peer_id: Key,
}

/// A peer's presence in one swarm.
struct Member {
    peer: Idx,
    swarm: Idx,
    /// Index into `Swarm::slots`, kept up to date on swap-removal.
    pos: u32,
    last_seen: u32,
    completed: bool,
}

/// Single-threaded tracker state for a set of swarms. No I/O: requests come in through
/// [`Shard::handle`], everything to send goes out through an [`Outbox`].
///
/// A connection may carry many peer_ids, and a peer_id may be in many swarms.
pub struct Shard {
    settings: Settings,
    swarms: Slab<Swarm>,
    peers: Slab<Peer>,
    members: Slab<Member>,
    // Index tables store slab indices only; keys are read from the slabs.
    swarm_by_hash: HashTable<Idx>,
    peer_by_id: HashTable<Idx>,
    conn_peers: HashMap<ConnId, SmallVec<[Idx; 2]>, RandomState>,
    hasher: RandomState,
    rng: fastrand::Rng,
    scratch: Vec<Idx>,
}

impl Shard {
    pub fn new(settings: Settings, seed: u64) -> Self {
        Self {
            settings,
            swarms: Slab::new(),
            peers: Slab::new(),
            members: Slab::new(),
            swarm_by_hash: HashTable::new(),
            peer_by_id: HashTable::new(),
            conn_peers: HashMap::default(),
            hasher: RandomState::default(),
            rng: fastrand::Rng::with_seed(seed),
            scratch: Vec::new(),
        }
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Processes one request from `conn`. `now` is a monotonic clock in seconds.
    pub fn handle<O>(
        &mut self,
        now: u32,
        conn: ConnId,
        request: Request<'_, O>,
        out: &mut impl Outbox<O>,
    ) -> Result<(), TrackerError> {
        match request {
            Request::Announce {
                info_hash,
                peer_id,
                event,
                left_zero,
                numwant,
                offers,
            } => {
                let completed = event == AnnounceEvent::Completed || left_zero;
                self.announce(
                    now, conn, info_hash, peer_id, completed, numwant, offers, out,
                )
            }
            Request::Answer { to_peer_id, answer } => {
                let p = self
                    .find_peer(to_peer_id)
                    .ok_or(TrackerError::UnknownPeer)?;
                out.answer(self.peers[p as usize].conn, answer);
                Ok(())
            }
            Request::Stop { info_hash, peer_id } => {
                self.stop(info_hash, peer_id, out);
                Ok(())
            }
            Request::Scrape { target } => {
                self.scrape(conn, target, out);
                Ok(())
            }
        }
    }

    /// The connection closed: removes all of its peers.
    pub fn disconnect<O>(&mut self, conn: ConnId, out: &mut impl Outbox<O>) {
        if let Some(peers) = self.conn_peers.remove(&conn) {
            for p in peers {
                self.remove_peer(p, false, out);
            }
        }
    }

    /// Removes memberships not refreshed for more than `2 * announce_interval` seconds, and
    /// peers left without memberships. Returns the number of memberships removed.
    pub fn expire<O>(&mut self, now: u32, out: &mut impl Outbox<O>) -> usize {
        let timeout = self.settings.announce_interval.saturating_mul(2);
        let mut expired = std::mem::take(&mut self.scratch);
        expired.clear();
        expired.extend(
            self.members
                .iter()
                .filter(|(_, m)| now.saturating_sub(m.last_seen) > timeout)
                .map(|(i, _)| i as Idx),
        );

        for &m in &expired {
            let p = self.members[m as usize].peer;
            self.remove_membership(m, p);
            if self.peers[p as usize].members.is_empty() {
                self.remove_peer(p, true, out);
            }
        }

        let count = expired.len();
        self.scratch = expired;
        count
    }

    #[allow(clippy::too_many_arguments)]
    fn announce<O>(
        &mut self,
        now: u32,
        conn: ConnId,
        info_hash: &[u8],
        peer_id: &[u8],
        completed: bool,
        numwant: Option<u32>,
        offers: Option<&[O]>,
        out: &mut impl Outbox<O>,
    ) -> Result<(), TrackerError> {
        let info_hash = Key::new(info_hash).ok_or(TrackerError::KeyTooLong)?;
        let peer_id = Key::new(peer_id).ok_or(TrackerError::KeyTooLong)?;

        let peer_hash = self.hasher.hash_one(peer_id.as_bytes());
        let mut peer = self.find_peer_hashed(peer_hash, peer_id.as_bytes());

        if let Some(p) = peer
            && self.peers[p as usize].conn != conn
        {
            // The peer_id moved to another connection: drop it and start over.
            self.remove_peer(p, true, out);
            peer = None;
        }

        let swarm = self.get_or_create_swarm(&info_hash);

        let member = match peer {
            None => {
                let p = self.insert_peer(peer_hash, peer_id, conn);
                self.add_member(p, swarm, now, completed)
            }
            Some(p) => {
                let existing = self.peers[p as usize]
                    .members
                    .iter()
                    .find(|pm| pm.swarm == swarm)
                    .map(|pm| pm.member);
                match existing {
                    Some(m) => {
                        let member = &mut self.members[m as usize];
                        member.last_seen = now;
                        if completed && !member.completed {
                            member.completed = true;
                            self.swarms[swarm as usize].completed += 1;
                        }
                        m
                    }
                    None => self.add_member(p, swarm, now, completed),
                }
            }
        };

        let s = &self.swarms[swarm as usize];
        out.announce_reply(
            conn,
            &info_hash,
            self.settings.announce_interval,
            s.completed,
            s.slots.len() as u32 - s.completed,
        );

        if let Some(offers) = offers
            && let Some(numwant) = numwant
        {
            self.send_offers(swarm, member, &peer_id, &info_hash, offers, numwant, out);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn send_offers<O>(
        &mut self,
        swarm: Idx,
        me: Idx,
        from_peer_id: &Key,
        info_hash: &Key,
        offers: &[O],
        numwant: u32,
        out: &mut impl Outbox<O>,
    ) {
        let swarm = &mut self.swarms[swarm as usize];
        let len = swarm.slots.len();
        if len <= 1 {
            return;
        }

        let others = len - 1;
        let n = others
            .min(offers.len())
            .min(self.settings.max_offers as usize)
            .min(numwant as usize);

        if n == others {
            // Enough offers for everyone else in the swarm.
            let mut offers = offers.iter();
            for slot in &swarm.slots {
                if slot.member != me
                    && let Some(offer) = offers.next()
                {
                    out.offer(slot.conn, from_peer_id, info_hash, offer);
                }
            }
            return;
        }

        if n == 0 {
            return;
        }

        let mut idx = match self.settings.offer_selection {
            OfferSelection::RandomWindow => self.rng.usize(..len),
            OfferSelection::RoundRobin => swarm.cursor as usize % len,
            OfferSelection::RandomSample => {
                // Floyd's algorithm: n distinct indices out of the `others` peers, which are
                // the slots with the announcer's own slot cut out.
                let my_pos = self.members[me as usize].pos as usize;
                let mut chosen: SmallVec<[usize; 32]> = SmallVec::new();
                for j in others - n..others {
                    let t = self.rng.usize(..=j);
                    chosen.push(if chosen.contains(&t) { j } else { t });
                }
                for (offer, &k) in offers.iter().zip(&chosen) {
                    let slot = swarm.slots[if k >= my_pos { k + 1 } else { k }];
                    out.offer(slot.conn, from_peer_id, info_hash, offer);
                }
                return;
            }
        };
        let mut sent = 0;
        while sent < n {
            let slot = swarm.slots[idx];
            if slot.member != me {
                out.offer(slot.conn, from_peer_id, info_hash, &offers[sent]);
                sent += 1;
            }
            idx += 1;
            if idx == len {
                idx = 0;
            }
        }
        swarm.cursor = idx as u32;
    }

    fn stop<O>(&mut self, info_hash: &[u8], peer_id: &[u8], out: &mut impl Outbox<O>) {
        let Some(swarm) = self.find_swarm(info_hash) else {
            return;
        };
        let Some(p) = self.find_peer(peer_id) else {
            return;
        };
        let Some(m) = self.peers[p as usize]
            .members
            .iter()
            .find(|pm| pm.swarm == swarm)
            .map(|pm| pm.member)
        else {
            return;
        };

        self.remove_membership(m, p);
        if self.peers[p as usize].members.is_empty() {
            self.remove_peer(p, true, out);
        }
    }

    fn scrape<O>(&self, conn: ConnId, target: ScrapeTarget<'_>, out: &mut impl Outbox<O>) {
        match target {
            ScrapeTarget::All => {
                for (_, s) in &self.swarms {
                    let incomplete = s.slots.len() as u32 - s.completed;
                    out.scrape_entry(
                        conn,
                        s.info_hash.as_bytes(),
                        s.completed,
                        incomplete,
                        s.completed,
                    );
                }
            }
            ScrapeTarget::One(info_hash) => self.scrape_one(conn, info_hash, out),
            ScrapeTarget::Many(info_hashes) => {
                for info_hash in info_hashes {
                    self.scrape_one(conn, info_hash, out);
                }
            }
        }
        out.scrape_end(conn);
    }

    fn scrape_one<O>(&self, conn: ConnId, info_hash: &[u8], out: &mut impl Outbox<O>) {
        let (complete, incomplete) = match self.find_swarm(info_hash) {
            Some(s) => {
                let s = &self.swarms[s as usize];
                (s.completed, s.slots.len() as u32 - s.completed)
            }
            None => (0, 0),
        };
        out.scrape_entry(conn, info_hash, complete, incomplete, complete);
    }

    // ---- index helpers ----

    #[inline]
    fn find_swarm(&self, info_hash: &[u8]) -> Option<Idx> {
        let hash = self.hasher.hash_one(info_hash);
        self.swarm_by_hash
            .find(hash, |&s| self.swarms[s as usize].info_hash == *info_hash)
            .copied()
    }

    #[inline]
    fn find_peer(&self, peer_id: &[u8]) -> Option<Idx> {
        self.find_peer_hashed(self.hasher.hash_one(peer_id), peer_id)
    }

    #[inline]
    fn find_peer_hashed(&self, hash: u64, peer_id: &[u8]) -> Option<Idx> {
        self.peer_by_id
            .find(hash, |&p| self.peers[p as usize].peer_id == *peer_id)
            .copied()
    }

    fn get_or_create_swarm(&mut self, info_hash: &Key) -> Idx {
        let hash = self.hasher.hash_one(info_hash.as_bytes());
        let swarms = &mut self.swarms;
        let hasher = &self.hasher;
        let entry = self.swarm_by_hash.entry(
            hash,
            |&s| swarms[s as usize].info_hash == *info_hash,
            |&s| hasher.hash_one(swarms[s as usize].info_hash.as_bytes()),
        );
        match entry {
            hashbrown::hash_table::Entry::Occupied(e) => *e.get(),
            hashbrown::hash_table::Entry::Vacant(e) => {
                let s = swarms.insert(Swarm {
                    slots: Vec::new(),
                    completed: 0,
                    cursor: 0,
                    info_hash: *info_hash,
                }) as Idx;
                e.insert(s);
                s
            }
        }
    }

    fn insert_peer(&mut self, hash: u64, peer_id: Key, conn: ConnId) -> Idx {
        let p = self.peers.insert(Peer {
            conn,
            members: SmallVec::new(),
            peer_id,
        }) as Idx;
        let peers = &self.peers;
        let hasher = &self.hasher;
        self.peer_by_id.insert_unique(hash, p, |&p| {
            hasher.hash_one(peers[p as usize].peer_id.as_bytes())
        });
        self.conn_peers.entry(conn).or_default().push(p);
        p
    }

    fn add_member(&mut self, p: Idx, s: Idx, now: u32, completed: bool) -> Idx {
        let swarm = &mut self.swarms[s as usize];
        let peer = &mut self.peers[p as usize];
        let m = self.members.insert(Member {
            peer: p,
            swarm: s,
            pos: swarm.slots.len() as u32,
            last_seen: now,
            completed,
        }) as Idx;
        swarm.slots.push(Slot {
            conn: peer.conn,
            member: m,
        });
        if completed {
            swarm.completed += 1;
        }
        peer.members.push(PeerMembership {
            swarm: s,
            member: m,
        });
        m
    }

    /// Removes member `m` of peer `p` from both its swarm and the peer.
    fn remove_membership(&mut self, m: Idx, p: Idx) {
        self.unlink_member(m);
        let members = &mut self.peers[p as usize].members;
        if let Some(i) = members.iter().position(|pm| pm.member == m) {
            members.swap_remove(i);
        }
    }

    /// Removes member `m` from its swarm (deleting the swarm if it becomes empty). Does not
    /// touch the owning peer's membership list.
    fn unlink_member(&mut self, m: Idx) {
        let member = self.members.remove(m as usize);
        let swarm = &mut self.swarms[member.swarm as usize];
        if member.completed {
            swarm.completed -= 1;
        }

        let pos = member.pos as usize;
        swarm.slots.swap_remove(pos);
        if let Some(moved) = swarm.slots.get(pos) {
            self.members[moved.member as usize].pos = pos as u32;
        }

        if swarm.slots.is_empty() {
            let hash = self.hasher.hash_one(swarm.info_hash.as_bytes());
            if let Ok(e) = self.swarm_by_hash.find_entry(hash, |&s| s == member.swarm) {
                e.remove();
            }
            self.swarms.remove(member.swarm as usize);
        }
    }

    fn remove_peer<O>(&mut self, p: Idx, unlink_conn: bool, out: &mut impl Outbox<O>) {
        let peer = self.peers.remove(p as usize);
        for pm in &peer.members {
            self.unlink_member(pm.member);
        }

        let hash = self.hasher.hash_one(peer.peer_id.as_bytes());
        if let Ok(e) = self.peer_by_id.find_entry(hash, |&i| i == p) {
            e.remove();
        }

        if unlink_conn && let Entry::Occupied(mut e) = self.conn_peers.entry(peer.conn) {
            let peers = e.get_mut();
            if let Some(i) = peers.iter().position(|&i| i == p) {
                peers.swap_remove(i);
            }
            if peers.is_empty() {
                e.remove();
            }
        }

        out.peer_removed(&peer.peer_id, peer.conn);
    }

    // ---- introspection ----

    pub fn swarm_count(&self) -> usize {
        self.swarms.len()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn membership_count(&self) -> usize {
        self.members.len()
    }

    pub fn connection_count(&self) -> usize {
        self.conn_peers.len()
    }

    /// All swarms with their counters (e.g. for /stats.json).
    pub fn swarms(&self) -> impl Iterator<Item = (&Key, SwarmStats)> {
        self.swarms.iter().map(|(_, s)| {
            (
                &s.info_hash,
                SwarmStats {
                    peers: s.slots.len() as u32,
                    complete: s.completed,
                },
            )
        })
    }

    pub fn swarm_stats(&self, info_hash: &[u8]) -> Option<SwarmStats> {
        let s = &self.swarms[self.find_swarm(info_hash)? as usize];
        Some(SwarmStats {
            peers: s.slots.len() as u32,
            complete: s.completed,
        })
    }

    /// peer_ids of a swarm, in swarm order.
    pub fn swarm_peer_ids(&self, info_hash: &[u8]) -> Option<Vec<Key>> {
        let s = &self.swarms[self.find_swarm(info_hash)? as usize];
        Some(
            s.slots
                .iter()
                .map(|slot| {
                    let peer = self.members[slot.member as usize].peer;
                    self.peers[peer as usize].peer_id
                })
                .collect(),
        )
    }

    pub fn peer_swarms(&self, peer_id: &[u8]) -> Option<Vec<Key>> {
        let p = &self.peers[self.find_peer(peer_id)? as usize];
        Some(
            p.members
                .iter()
                .map(|pm| self.swarms[pm.swarm as usize].info_hash)
                .collect(),
        )
    }

    pub fn peer_connection(&self, peer_id: &[u8]) -> Option<ConnId> {
        Some(self.peers[self.find_peer(peer_id)? as usize].conn)
    }

    pub fn connection_peer_ids(&self, conn: ConnId) -> Vec<Key> {
        self.conn_peers
            .get(&conn)
            .map(|peers| {
                peers
                    .iter()
                    .map(|&p| self.peers[p as usize].peer_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Verifies every cross-reference between swarms, peers, members, connections and index
    /// tables. Intended for tests.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut slot_total = 0;
        for (s, swarm) in &self.swarms {
            let s = s as Idx;
            if swarm.slots.is_empty() {
                return Err(format!("swarm {:?} is empty", swarm.info_hash));
            }
            if self.find_swarm(swarm.info_hash.as_bytes()) != Some(s) {
                return Err(format!("swarm {:?} not indexed", swarm.info_hash));
            }
            let mut completed = 0;
            for (pos, slot) in swarm.slots.iter().enumerate() {
                let m = self
                    .members
                    .get(slot.member as usize)
                    .ok_or_else(|| format!("slot points to missing member {}", slot.member))?;
                if m.swarm != s || m.pos as usize != pos {
                    return Err(format!("member {} has wrong swarm/pos", slot.member));
                }
                let peer = self
                    .peers
                    .get(m.peer as usize)
                    .ok_or_else(|| format!("member {} points to missing peer", slot.member))?;
                if peer.conn != slot.conn {
                    return Err(format!("slot conn mismatch for member {}", slot.member));
                }
                completed += m.completed as u32;
            }
            if completed != swarm.completed {
                return Err(format!(
                    "swarm {:?} completed count mismatch",
                    swarm.info_hash
                ));
            }
            slot_total += swarm.slots.len();
        }
        if slot_total != self.members.len() {
            return Err("slot count != member count".into());
        }

        let mut membership_total = 0;
        for (p, peer) in &self.peers {
            let p = p as Idx;
            if peer.members.is_empty() {
                return Err(format!("peer {:?} has no swarms", peer.peer_id));
            }
            if self.find_peer(peer.peer_id.as_bytes()) != Some(p) {
                return Err(format!("peer {:?} not indexed", peer.peer_id));
            }
            for (i, pm) in peer.members.iter().enumerate() {
                let m = self
                    .members
                    .get(pm.member as usize)
                    .ok_or_else(|| format!("peer {:?} points to missing member", peer.peer_id))?;
                if m.peer != p || m.swarm != pm.swarm {
                    return Err(format!("peer {:?} membership mismatch", peer.peer_id));
                }
                if peer.members[..i].iter().any(|o| o.swarm == pm.swarm) {
                    return Err(format!("peer {:?} is twice in a swarm", peer.peer_id));
                }
            }
            membership_total += peer.members.len();
            if !self
                .conn_peers
                .get(&peer.conn)
                .is_some_and(|peers| peers.contains(&p))
            {
                return Err(format!(
                    "peer {:?} missing from its connection",
                    peer.peer_id
                ));
            }
        }
        if membership_total != self.members.len() {
            return Err("peer membership count != member count".into());
        }

        let mut conn_total = 0;
        for (conn, peers) in &self.conn_peers {
            if peers.is_empty() {
                return Err(format!("connection {conn:?} has no peers"));
            }
            for &p in peers {
                if self
                    .peers
                    .get(p as usize)
                    .is_none_or(|peer| peer.conn != *conn)
                {
                    return Err(format!("connection {conn:?} lists a foreign peer"));
                }
            }
            conn_total += peers.len();
        }
        if conn_total != self.peers.len() {
            return Err("connection peer count != peer count".into());
        }

        if self.swarm_by_hash.len() != self.swarms.len()
            || self.peer_by_id.len() != self.peers.len()
        {
            return Err("index table size mismatch".into());
        }
        Ok(())
    }
}
