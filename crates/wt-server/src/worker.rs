//! A worker: one OS thread, one current-thread tokio runtime, one tracker shard, the connections
//! it accepted, and an inbox of events from the other workers (spec §13.3).

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasher;
use std::net::TcpListener as StdListener;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bytes::Bytes;
use slab::Slab;
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::{LocalSet, spawn_local};
use wt_core::{ConnId, Key, Outbox, Shard};
use wt_proto::{Batch, Count, Encoder, Message, OwnedMessage, ProtoError};

use crate::conn;
use crate::placement::{self, Mode};
use crate::reasons::{CloseReason, Counts, HttpRoute, RejectReason};
use crate::ws::codec::close;
use crate::ws::driver::{IoCounters, io_counters};
use crate::{Phase, Shared};

/// Events between workers. Sent in batches (`Vec<Event>`), one channel send per destination per
/// scheduler tick.
pub(crate) enum Event {
    /// A parsed message for this worker's shard, from a connection of another worker. Boxed:
    /// events travel in `Vec` batches and most are small `Deliver`s.
    Request {
        conn: ConnId,
        message: Box<OwnedMessage>,
    },
    /// The connection closed: remove its peers from this shard.
    Disconnect { conn: ConnId },
    /// Send this message to a connection of this worker.
    Deliver { conn: ConnId, message: Bytes },
    /// This shard rejected a request of the connection: close it (like the JS server).
    Close { conn: ConnId },
    /// Scrape this shard (`None`: all swarms).
    Scrape {
        info_hashes: Option<Vec<Vec<u8>>>,
        reply: oneshot::Sender<Vec<ScrapeEntry>>,
    },
    /// Counts and counters of this shard, for `/stats.json` and `/metrics`.
    Stats { reply: oneshot::Sender<ShardStats> },
    /// Peers of one swarm in this shard (`None`: no such swarm), for `/stats.json?infoHash=`.
    Swarm {
        info_hash: Vec<u8>,
        reply: oneshot::Sender<Option<u32>>,
    },
    /// The `top` largest swarms of this shard (0: all) and its swarm count, for `/swarms`.
    Swarms {
        top: usize,
        reply: oneshot::Sender<TopSwarms>,
    },
    /// A connection moved to this worker at its first message (`content` placement).
    Adopt(Box<conn::Adopt>),
    /// A request of the connection was forwarded to `shard`: it may hold its peers now.
    Track { conn: ConnId, shard: usize },
}

/// One worker's part of `/stats.json` and `/metrics`. `Default` (`up: false`): the worker did
/// not answer in time.
#[derive(Default)]
pub(crate) struct ShardStats {
    pub up: bool,
    /// Swarms in the shard, and peers summed over them (a peer in two swarms counts twice).
    pub swarms: usize,
    pub peers: usize,
    /// Requests of this worker's connections applied to its own shard / sent to another.
    pub local_requests: u64,
    pub remote_requests: u64,
    /// Connections that moved to this worker.
    pub moved_in: u64,
    /// Messages this shard produced, per kind (JSON bytes before framing and compression).
    pub sent: wt_proto::Counters,
    /// Messages received from this worker's connections, per kind.
    pub received: Received,
    /// Socket and compression totals of the worker thread.
    pub io: IoCounters,
    /// Connections that ended, per [`CloseReason`] (counted by the worker that held them).
    pub closed: [u64; CloseReason::COUNT],
    /// Messages rejected, per [`RejectReason`] (counted by the worker that rejected them).
    pub rejected: [u64; RejectReason::COUNT],
    /// HTTP requests answered without an upgrade, per [`HttpRoute`].
    pub http: [u64; HttpRoute::COUNT],
    /// Peers removed by expiry.
    pub expired: u64,
}

/// A shard's largest swarms `(info_hash key, peers)`, and its swarm count.
pub(crate) type TopSwarms = (Vec<(Vec<u8>, u32)>, usize);

/// How long a stats gather waits for each other worker.
const STATS_TIMEOUT: Duration = Duration::from_secs(1);

/// Messages received (JSON bytes after inflating), per kind; `invalid`: not parsed.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Received {
    pub announces: Count,
    pub answers: Count,
    pub stops: Count,
    pub scrapes: Count,
    pub invalid: Count,
}

/// What became of a message.
pub(crate) enum Handled {
    Done,
    /// The connection's first message is an announce for a swarm on worker `w`: move the
    /// connection there; the message is handled after the move.
    Move(usize),
}

pub(crate) struct ScrapeEntry {
    pub info_hash: Vec<u8>,
    pub complete: u32,
    pub incomplete: u32,
}

/// Outgoing frames queued for a connection's writer task.
pub(crate) enum Out {
    Text(Bytes),
    /// Only the echo endpoint sends binary frames.
    Binary(Bytes),
    /// A close frame with this code, after the frames queued before it.
    Close(u16),
}

/// A scrape spanning shards, gathered asynchronously.
type ScrapeJob = Option<Vec<Vec<u8>>>;

enum Routed {
    Done,
    Scrape(ScrapeJob),
    Move(usize),
}

pub(crate) enum Pop {
    Frame(Out),
    Empty,
    Gone,
}

struct ConnEntry {
    generation: u32,
    queue: VecDeque<Out>,
    queued_bytes: usize,
    /// Wakes the writer.
    notify: Rc<Notify>,
    /// Wakes the reader to stop.
    closed: Rc<Notify>,
    /// Shards this connection announced to (they may hold its peers).
    shard_mask: u64,
    closing: bool,
    /// Its first message was handled: it stays on this worker.
    placed: bool,
}

/// `ConnId` layout: worker (8 bits) | generation (24) | slot (32).
pub(crate) fn conn_id(worker: usize, generation: u32, slot: usize) -> ConnId {
    ConnId(((worker as u64) << 56) | (((generation & 0xFF_FFFF) as u64) << 32) | slot as u64)
}

pub(crate) fn conn_worker(conn: ConnId) -> usize {
    (conn.0 >> 56) as usize
}

fn conn_generation(conn: ConnId) -> u32 {
    ((conn.0 >> 32) & 0xFF_FFFF) as u32
}

fn conn_slot(conn: ConnId) -> usize {
    (conn.0 & 0xFFFF_FFFF) as usize
}

/// Messages dropped because a connection exceeded `maxBackpressure`, all workers.
pub(crate) static DROPPED_MESSAGES: AtomicU64 = AtomicU64::new(0);

pub(crate) struct Worker {
    pub id: usize,
    pub shared: Arc<Shared>,
    shard: RefCell<Shard>,
    encoder: RefCell<Encoder>,
    conns: RefCell<Slab<ConnEntry>>,
    next_generation: Cell<u32>,
    outbox: RefCell<Vec<Vec<Event>>>,
    flush_scheduled: Cell<bool>,
    start: Instant,
    rng: RefCell<fastrand::Rng>,
    /// Shutting down: no new connections; new WebSockets are closed at once.
    draining: Cell<bool>,
    local_requests: Cell<u64>,
    remote_requests: Cell<u64>,
    moved_in: Cell<u64>,
    received: Cell<Received>,
    closed: Counts<{ CloseReason::COUNT }>,
    rejected: Counts<{ RejectReason::COUNT }>,
    http: Counts<{ HttpRoute::COUNT }>,
    expired: Cell<u64>,
}

impl Worker {
    fn new(id: usize, shared: Arc<Shared>, seed: u64) -> Self {
        let workers = shared.workers;
        Self {
            id,
            shard: RefCell::new(Shard::new(shared.settings, seed)),
            shared,
            encoder: RefCell::new(Encoder::new()),
            conns: RefCell::new(Slab::new()),
            next_generation: Cell::new(0),
            outbox: RefCell::new((0..workers).map(|_| Vec::new()).collect()),
            flush_scheduled: Cell::new(false),
            start: Instant::now(),
            rng: RefCell::new(fastrand::Rng::with_seed(seed)),
            draining: Cell::new(false),
            local_requests: Cell::new(0),
            remote_requests: Cell::new(0),
            moved_in: Cell::new(0),
            received: Cell::new(Received::default()),
            closed: Counts::default(),
            rejected: Counts::default(),
            http: Counts::default(),
            expired: Cell::new(0),
        }
    }

    /// Counts a connection that ended (or was refused before the upgrade).
    pub fn count_close(&self, reason: CloseReason) {
        self.closed.add(reason as usize, 1);
    }

    pub fn count_http(&self, route: HttpRoute) {
        self.http.add(route as usize, 1);
    }

    /// Counts and logs (rate-limited) a rejected message.
    fn reject(&self, e: &ProtoError) {
        let reason = RejectReason::from(e);
        self.rejected.add(reason as usize, 1);
        crate::event_limited!(
            Info,
            "rejected_message",
            reason = reason.as_str(),
            error = e,
            worker = self.id
        );
    }

    /// Tracker clock: seconds since the worker started.
    fn now(&self) -> u32 {
        self.start.elapsed().as_secs() as u32
    }

    pub fn shard_of(&self, info_hash: &[u8]) -> usize {
        (self.shared.router.hash_one(info_hash) % self.shared.workers as u64) as usize
    }

    /// Routing by hash only: `hash` placement, or a single worker (nothing to place).
    fn hashed(&self) -> bool {
        self.shared.placement == Mode::Hash || self.shared.workers == 1
    }

    /// The shard owning `info_hash`; `None`: no swarm for it anywhere (`content` placement).
    fn owner(&self, info_hash: &[u8]) -> Option<usize> {
        if self.hashed() {
            return Some(self.shard_of(info_hash));
        }
        Key::new(info_hash).and_then(|key| self.shared.directory.get(&key))
    }

    /// The shard for an announce: the owner, or a new binding (spec §13.3).
    fn announce_shard(&self, info_hash: &[u8], first: bool) -> usize {
        if self.hashed() {
            return self.shard_of(info_hash);
        }
        // Too long: the local shard rejects it.
        let Some(key) = Key::new(info_hash) else {
            return self.id;
        };
        let directory = &self.shared.directory;
        if let Some(owner) = directory.get(&key) {
            return owner;
        }
        let loads = self.shared.loads.snapshot();
        let pick = placement::pick_two(&mut self.rng.borrow_mut(), self.shared.workers);
        let choice = if first {
            placement::for_new_content(&loads, self.id, pick)
        } else {
            placement::for_new_hash(&loads, self.id, pick)
        };
        directory.claim(key, choice)
    }

    /// Bind before create: an announce from another worker for a swarm this shard does not
    /// have is applied here only if this worker owns (or now claims) the info_hash; otherwise
    /// returns the owner to forward it to.
    fn misrouted(&self, shard: &Shard, message: &Message<'_>) -> Option<usize> {
        if self.hashed() || !matches!(message, Message::Announce { .. }) {
            return None;
        }
        let info_hash = message.route_info_hash()?;
        let info_hash = info_hash.as_bytes();
        if shard.swarm_stats(info_hash).is_some() {
            return None;
        }
        let key = Key::new(info_hash)?;
        let owner = self.shared.directory.claim(key, self.id);
        (owner != self.id).then_some(owner)
    }

    // ---- connections ----

    /// Registers a WebSocket connection: its id, writer wake-up and reader stop signal.
    /// `placed`: the connection already handled its first message (it moved here).
    pub fn register(&self, placed: bool) -> (ConnId, Rc<Notify>, Rc<Notify>) {
        let generation = self.next_generation.get().wrapping_add(1) & 0xFF_FFFF;
        self.next_generation.set(generation);
        let notify = Rc::new(Notify::new());
        let closed = Rc::new(Notify::new());
        let slot = self.conns.borrow_mut().insert(ConnEntry {
            generation,
            queue: VecDeque::new(),
            queued_bytes: 0,
            notify: notify.clone(),
            closed: closed.clone(),
            shard_mask: 0,
            closing: false,
            placed,
        });
        if placed {
            self.moved_in.set(self.moved_in.get() + 1);
        }
        self.shared.loads.add_conn(self.id, 1);
        (conn_id(self.id, generation, slot), notify, closed)
    }

    /// Queues a frame for a connection of this worker. Messages past `maxBackpressure` are
    /// dropped; frames to closed or reused slots are ignored.
    pub fn deliver_local(&self, conn: ConnId, out: Out) {
        let mut conns = self.conns.borrow_mut();
        let Some(entry) = conns
            .get_mut(conn_slot(conn))
            .filter(|e| e.generation == conn_generation(conn) && !e.closing)
        else {
            return;
        };
        if let Out::Text(m) = &out {
            if entry.queued_bytes + m.len() > self.shared.max_backpressure {
                DROPPED_MESSAGES.fetch_add(1, Relaxed);
                return;
            }
            entry.queued_bytes += m.len();
        }
        entry.queue.push_back(out);
        entry.notify.notify_one();
    }

    /// Next frame for the writer of `conn`.
    pub fn pop(&self, conn: ConnId) -> Pop {
        let mut conns = self.conns.borrow_mut();
        let Some(entry) = conns
            .get_mut(conn_slot(conn))
            .filter(|e| e.generation == conn_generation(conn))
        else {
            return Pop::Gone;
        };
        match entry.queue.pop_front() {
            Some(out) => {
                if let Out::Text(m) = &out {
                    entry.queued_bytes -= m.len();
                }
                Pop::Frame(out)
            }
            None => Pop::Empty,
        }
    }

    /// Starts closing `conn` once: its peers are removed from every shard it announced to, the
    /// writer sends a close frame (1000), the reader stops.
    pub fn begin_close(self: &Rc<Self>, conn: ConnId) {
        self.begin_close_with(conn, close::NORMAL);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.get()
    }

    /// Graceful shutdown: every WebSocket of this worker is closed with 1001 after the messages
    /// already queued for it (spec §13.6).
    fn drain(self: &Rc<Self>) {
        self.draining.set(true);
        let open: Vec<ConnId> = self
            .conns
            .borrow()
            .iter()
            .map(|(slot, e)| conn_id(self.id, e.generation, slot))
            .collect();
        for conn in open {
            self.begin_close_with(conn, close::GOING_AWAY);
        }
    }

    /// [`Self::begin_close`] with a close code.
    pub fn begin_close_with(self: &Rc<Self>, conn: ConnId, code: u16) {
        let mask = {
            let mut conns = self.conns.borrow_mut();
            let Some(entry) = conns
                .get_mut(conn_slot(conn))
                .filter(|e| e.generation == conn_generation(conn) && !e.closing)
            else {
                return;
            };
            entry.closing = true;
            entry.queue.push_back(Out::Close(code));
            entry.notify.notify_one();
            entry.closed.notify_one();
            entry.shard_mask
        };
        for shard in 0..self.shared.workers {
            if mask & (1 << shard) == 0 {
                continue;
            }
            if shard == self.id {
                let batch = {
                    let mut encoder = self.encoder.borrow_mut();
                    self.shard.borrow_mut().disconnect(conn, &mut *encoder);
                    encoder.take()
                };
                self.dispatch(batch);
            } else {
                self.push_remote(shard, Event::Disconnect { conn });
            }
        }
    }

    /// Frees the slot after the writer finished.
    pub fn remove(&self, conn: ConnId) {
        let mut conns = self.conns.borrow_mut();
        if conns
            .get(conn_slot(conn))
            .is_some_and(|e| e.generation == conn_generation(conn))
        {
            conns.remove(conn_slot(conn));
            self.shared.loads.add_conn(self.id, -1);
        }
    }

    /// Closes a connection whose message was rejected (1008), local or on another worker.
    fn close_rejected(self: &Rc<Self>, conn: ConnId) {
        match conn_worker(conn) {
            w if w == self.id => self.begin_close_with(conn, close::POLICY),
            w => self.push_remote(w, Event::Close { conn }),
        }
    }

    // ---- message flow ----

    /// One text/binary frame from a connection of this worker. `Err`: close the connection.
    /// One message, parsed from the worker's shared read buffer: local requests are applied
    /// without any copy; a scrape across shards continues in a separate task.
    /// A connection's first message may move it (`Handled::Move`, nothing applied yet).
    pub fn handle_message(
        self: &Rc<Self>,
        conn: ConnId,
        frame: &[u8],
    ) -> Result<Handled, ProtoError> {
        let message = match <wt_proto::DefaultBackend as wt_proto::Backend>::parse(frame) {
            Ok(message) => message,
            Err(e) => {
                self.count_received(|r| r.invalid.add(frame.len()));
                self.reject(&e);
                return Err(e);
            }
        };
        let first = {
            let mut conns = self.conns.borrow_mut();
            conns
                .get_mut(conn_slot(conn))
                .is_some_and(|entry| !std::mem::replace(&mut entry.placed, true))
        };
        let routed = self
            .route_message(conn, frame, &message, first)
            .inspect_err(|e| self.reject(e))?;
        match routed {
            Routed::Done => {}
            Routed::Scrape(job) => {
                let me = self.clone();
                spawn_local(async move { me.scrape(conn, job).await });
            }
            // Counted by the worker it moves to, which handles it.
            Routed::Move(worker) => return Ok(Handled::Move(worker)),
        }
        self.count_received(|r| {
            let kind = match &message {
                Message::Announce { .. } => &mut r.announces,
                Message::Answer { .. } => &mut r.answers,
                Message::Stop { .. } => &mut r.stops,
                Message::Scrape { .. } => &mut r.scrapes,
            };
            kind.add(frame.len());
        });
        Ok(Handled::Done)
    }

    fn count_received(&self, f: impl FnOnce(&mut Received)) {
        let mut received = self.received.get();
        f(&mut received);
        self.received.set(received);
    }

    /// Routes a parsed message. `first`: the connection's first message.
    fn route_message(
        self: &Rc<Self>,
        conn: ConnId,
        frame: &[u8],
        message: &Message<'_>,
        first: bool,
    ) -> Result<Routed, ProtoError> {
        let workers = self.shared.workers;

        if let Message::Scrape { info_hashes } = message {
            return match info_hashes {
                Some(hashes) if hashes.len() == 1 && workers > 1 => {
                    let shard = self.owner(&hashes[0]).unwrap_or(self.id);
                    self.route(shard, conn, frame, message)
                        .map(|_| Routed::Done)
                }
                _ if workers == 1 => self.apply_local(conn, message).map(|_| Routed::Done),
                _ => Ok(Routed::Scrape(
                    info_hashes
                        .as_ref()
                        .map(|hashes| hashes.iter().map(|h| h.to_vec()).collect()),
                )),
            };
        }

        match message.route_info_hash() {
            Some(info_hash) if matches!(message, Message::Announce { .. }) => {
                let shard = self.announce_shard(info_hash.as_bytes(), first);
                if first && shard != self.id && !self.hashed() {
                    return Ok(Routed::Move(shard));
                }
                let mut conns = self.conns.borrow_mut();
                if let Some(entry) = conns.get_mut(conn_slot(conn)) {
                    entry.shard_mask |= 1 << shard;
                }
                drop(conns);
                self.route(shard, conn, frame, message)
                    .map(|_| Routed::Done)
            }
            // No swarm anywhere (`content`): the local shard gives the same outcome.
            Some(info_hash) => {
                let shard = self.owner(info_hash.as_bytes()).unwrap_or(self.id);
                self.route(shard, conn, frame, message)
                    .map(|_| Routed::Done)
            }
            // A stop that cannot match anything.
            None if matches!(message, Message::Stop { .. }) => Ok(Routed::Done),
            // An answer without a usable info_hash: fine with one shard (JS semantics), but it
            // cannot be routed between shards (JS multi-worker rejects it too).
            None if workers == 1 => self.apply_local(conn, message).map(|_| Routed::Done),
            None => Err(ProtoError::BadField("info_hash")),
        }
    }

    fn route(
        self: &Rc<Self>,
        shard: usize,
        conn: ConnId,
        frame: &[u8],
        message: &Message<'_>,
    ) -> Result<(), ProtoError> {
        if shard == self.id {
            self.local_requests.set(self.local_requests.get() + 1);
            self.apply_local(conn, message)
        } else {
            self.remote_requests.set(self.remote_requests.get() + 1);
            // The frame is in the shared read buffer: one copy for the other worker.
            let message = Box::new(OwnedMessage::copy_from(frame, message));
            self.push_remote(shard, Event::Request { conn, message });
            Ok(())
        }
    }

    fn apply_local(self: &Rc<Self>, conn: ConnId, message: &Message<'_>) -> Result<(), ProtoError> {
        let (result, batch) = {
            let mut encoder = self.encoder.borrow_mut();
            let result = wt_proto::apply(
                &mut self.shard.borrow_mut(),
                self.now(),
                conn,
                message,
                &mut encoder,
            );
            (result, encoder.take())
        };
        self.dispatch(batch);
        result
    }

    /// Sends every message of a batch to its connection's worker.
    fn dispatch(self: &Rc<Self>, batch: Batch) {
        for (to, message) in batch.iter() {
            match conn_worker(to) {
                w if w == self.id => self.deliver_local(to, Out::Text(message)),
                w => self.push_remote(w, Event::Deliver { conn: to, message }),
            }
        }
    }

    pub(crate) fn push_remote(self: &Rc<Self>, worker: usize, event: Event) {
        self.outbox.borrow_mut()[worker].push(event);
        if !self.flush_scheduled.replace(true) {
            let me = self.clone();
            spawn_local(async move {
                // Let the current burst of work queue more events first.
                tokio::task::yield_now().await;
                me.flush();
            });
        }
    }

    fn flush(&self) {
        self.flush_scheduled.set(false);
        let mut outbox = self.outbox.borrow_mut();
        for (worker, events) in outbox.iter_mut().enumerate() {
            if !events.is_empty() {
                // Fails only during shutdown.
                let _ = self.shared.senders[worker].send(std::mem::take(events));
            }
        }
    }

    /// Events from other workers, a batch at a time; outgoing messages dispatched once per batch.
    async fn inbox(self: Rc<Self>, mut rx: mpsc::UnboundedReceiver<Vec<Event>>) {
        while let Some(events) = rx.recv().await {
            let mut rejected = Vec::new();
            {
                let mut shard = self.shard.borrow_mut();
                let mut encoder = self.encoder.borrow_mut();
                let now = self.now();
                for event in events {
                    match event {
                        Event::Request { conn, message } => {
                            let misrouted = self.misrouted(&shard, &message.message());
                            if let Some(owner) = misrouted {
                                // Rare: the binding changed while the request was in flight.
                                self.push_remote(owner, Event::Request { conn, message });
                                self.track(conn, owner);
                                continue;
                            }
                            if let Err(e) = wt_proto::apply(
                                &mut shard,
                                now,
                                conn,
                                &message.message(),
                                &mut encoder,
                            ) {
                                self.reject(&e);
                                rejected.push(conn);
                            }
                        }
                        Event::Disconnect { conn } => shard.disconnect(conn, &mut *encoder),
                        Event::Deliver { conn, message } => {
                            self.deliver_local(conn, Out::Text(message))
                        }
                        Event::Close { conn } => rejected.push(conn),
                        Event::Scrape { info_hashes, reply } => {
                            let _ = reply.send(scrape_entries(&shard, info_hashes.as_deref()));
                        }
                        Event::Stats { reply } => {
                            let _ = reply.send(self.shard_stats(&shard, encoder.counters()));
                        }
                        Event::Swarm { info_hash, reply } => {
                            let _ = reply.send(shard.swarm_stats(&info_hash).map(|s| s.peers));
                        }
                        Event::Swarms { top, reply } => {
                            let _ = reply.send(top_swarms(&shard, top));
                        }
                        Event::Adopt(adopt) => {
                            spawn_local(conn::adopted(self.clone(), *adopt));
                        }
                        Event::Track { conn, shard } => self.track(conn, shard),
                    }
                }
            }
            let batch = self.encoder.borrow_mut().take();
            self.dispatch(batch);
            for conn in rejected {
                self.close_rejected(conn);
            }
        }
    }

    /// Scrape across shards, merged in request order (spec §13.4).
    async fn scrape(self: &Rc<Self>, conn: ConnId, info_hashes: Option<Vec<Vec<u8>>>) {
        let mut entries: Vec<ScrapeEntry> = Vec::new();
        let mut pending = Vec::new();
        for shard in 0..self.shared.workers {
            let subset: Option<Vec<Vec<u8>>> = info_hashes.as_ref().map(|hashes| {
                hashes
                    .iter()
                    // Unknown hashes (no swarm anywhere) are in no subset: 0 / 0 entries.
                    .filter(|h| self.owner(h) == Some(shard))
                    .cloned()
                    .collect()
            });
            if subset.as_ref().is_some_and(|s| s.is_empty()) {
                continue;
            }
            if shard == self.id {
                entries.extend(scrape_entries(&self.shard.borrow(), subset.as_deref()));
            } else {
                let (reply, rx) = oneshot::channel();
                self.push_remote(
                    shard,
                    Event::Scrape {
                        info_hashes: subset,
                        reply,
                    },
                );
                pending.push(rx);
            }
        }
        for rx in pending {
            entries.extend(rx.await.unwrap_or_default());
        }

        // The worker's encoder (no await below), so the reply is counted like any other.
        let mut encoder = self.encoder.borrow_mut();
        let out: &mut dyn Outbox<wt_proto::Payload<'_>> = &mut *encoder;
        match &info_hashes {
            None => {
                for e in &entries {
                    out.scrape_entry(conn, &e.info_hash, e.complete, e.incomplete, e.complete);
                }
            }
            Some(hashes) => {
                let found: HashMap<&[u8], &ScrapeEntry> = entries
                    .iter()
                    .map(|e| (e.info_hash.as_slice(), e))
                    .collect();
                for h in hashes {
                    let (complete, incomplete) = found
                        .get(h.as_slice())
                        .map_or((0, 0), |e| (e.complete, e.incomplete));
                    out.scrape_entry(conn, h, complete, incomplete, complete);
                }
            }
        }
        out.scrape_end(conn);
        let batch = encoder.take();
        drop(encoder);
        self.dispatch(batch);
    }

    /// Remembers that `shard` may hold peers of `conn` (on the connection's worker).
    fn track(self: &Rc<Self>, conn: ConnId, shard: usize) {
        if conn_worker(conn) != self.id {
            return self.push_remote(conn_worker(conn), Event::Track { conn, shard });
        }
        let mut conns = self.conns.borrow_mut();
        if let Some(entry) = conns
            .get_mut(conn_slot(conn))
            .filter(|e| e.generation == conn_generation(conn))
        {
            entry.shard_mask |= 1 << shard;
        }
    }

    /// `sent`: the encoder's counters (it may be borrowed by the caller).
    fn shard_stats(&self, shard: &Shard, sent: wt_proto::Counters) -> ShardStats {
        ShardStats {
            up: true,
            swarms: shard.swarm_count(),
            peers: shard.membership_count(),
            local_requests: self.local_requests.get(),
            remote_requests: self.remote_requests.get(),
            moved_in: self.moved_in.get(),
            sent,
            received: self.received.get(),
            io: io_counters(),
            closed: self.closed.get(),
            rejected: self.rejected.get(),
            http: self.http.get(),
            expired: self.expired.get(),
        }
    }

    /// Counts and counters of every worker, in worker order. A worker that does not answer
    /// within [`STATS_TIMEOUT`] is reported with `up: false`.
    pub async fn stats(self: &Rc<Self>) -> Vec<ShardStats> {
        self.gather(
            |reply| Event::Stats { reply },
            |me| me.shard_stats(&me.shard.borrow(), me.encoder.borrow().counters()),
        )
        .await
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect()
    }

    /// Peers of one swarm in every worker (`None`: no swarm there, or no answer).
    pub async fn swarm_peers(self: &Rc<Self>, info_hash: &[u8]) -> Vec<Option<u32>> {
        self.gather(
            |reply| Event::Swarm {
                info_hash: info_hash.to_vec(),
                reply,
            },
            |me| me.shard.borrow().swarm_stats(info_hash).map(|s| s.peers),
        )
        .await
        .into_iter()
        .map(Option::flatten)
        .collect()
    }

    /// The largest swarms of every worker (`top` each, 0: all), in worker order; a worker that
    /// does not answer in time contributes none.
    pub async fn top_swarms(self: &Rc<Self>, top: usize) -> Vec<TopSwarms> {
        self.gather(
            |reply| Event::Swarms { top, reply },
            |me| top_swarms(&me.shard.borrow(), top),
        )
        .await
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect()
    }

    /// Asks every worker (`local` for this one, an event for the others); `None` for a worker
    /// that does not answer within [`STATS_TIMEOUT`].
    async fn gather<T>(
        self: &Rc<Self>,
        event: impl Fn(oneshot::Sender<T>) -> Event,
        local: impl FnOnce(&Self) -> T,
    ) -> Vec<Option<T>> {
        let mut results: Vec<Option<T>> = (0..self.shared.workers).map(|_| None).collect();
        let mut pending = Vec::new();
        for worker in 0..self.shared.workers {
            if worker != self.id {
                let (reply, rx) = oneshot::channel();
                self.push_remote(worker, event(reply));
                pending.push((worker, rx));
            }
        }
        results[self.id] = Some(local(self));
        let deadline = tokio::time::Instant::now() + STATS_TIMEOUT;
        for (worker, rx) in pending {
            match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(value)) => results[worker] = Some(value),
                _ => crate::event_limited!(Warn, "worker_not_responding", worker = worker),
            }
        }
        results
    }

    async fn expiry(self: Rc<Self>) {
        let interval = Duration::from_secs(self.shared.settings.announce_interval.max(1) as u64);
        loop {
            tokio::time::sleep(interval).await;
            let batch = {
                let mut encoder = self.encoder.borrow_mut();
                let expired = self.shard.borrow_mut().expire(self.now(), &mut *encoder);
                self.expired.set(self.expired.get() + expired as u64);
                encoder.take()
            };
            self.dispatch(batch);
            self.release_empty();
        }
    }

    /// Releases directory entries of this worker without a swarm in its shard (spec §13.3).
    fn release_empty(&self) {
        if self.hashed() {
            return;
        }
        let shard = self.shard.borrow();
        let directory = &self.shared.directory;
        for key in directory.owned_by(self.id) {
            if shard.swarm_stats(key.as_bytes()).is_none() {
                directory.release(&key, self.id);
            }
        }
    }

    /// Reloads the certificate of a wss:// listener when its files change (worker 0, every
    /// `tlsReloadInterval` seconds; spec §13.2).
    async fn watch_certificates(self: Rc<Self>) {
        let interval = Duration::from_secs(self.shared.tls_reload_interval);
        loop {
            tokio::time::sleep(interval).await;
            for listener in &self.shared.listeners {
                if let Some(cert) = &listener.cert {
                    crate::tls::log(&listener.name, &cert.reload(false), "file");
                }
            }
        }
    }

    /// Publishes how busy this worker's runtime is, for placement.
    async fn load_ticker(self: Rc<Self>) {
        let metrics = tokio::runtime::Handle::current().metrics();
        let (mut last, mut last_busy) = (Instant::now(), metrics.worker_total_busy_duration(0));
        loop {
            tokio::time::sleep(placement::LOAD_TICK).await;
            let (now, busy) = (Instant::now(), metrics.worker_total_busy_duration(0));
            let wall = now.duration_since(last).as_secs_f64().max(1e-3);
            let busy_secs = busy.saturating_sub(last_busy).as_secs_f64();
            (last, last_busy) = (now, busy);
            self.shared.loads.sample_busy(self.id, busy_secs, wall);
        }
    }
}

/// The `top` swarms with the most peers (0: all), unordered; copies only their keys.
fn top_swarms(shard: &Shard, top: usize) -> TopSwarms {
    let mut swarms: Vec<(u32, &Key)> = shard.swarms().map(|(key, s)| (s.peers, key)).collect();
    if top > 0 && swarms.len() > top {
        swarms.select_nth_unstable_by(top - 1, |a, b| b.0.cmp(&a.0));
        swarms.truncate(top);
    }
    let keys = swarms
        .into_iter()
        .map(|(peers, key)| (key.as_bytes().to_vec(), peers))
        .collect();
    (keys, shard.swarm_count())
}

fn scrape_entries(shard: &Shard, info_hashes: Option<&[Vec<u8>]>) -> Vec<ScrapeEntry> {
    match info_hashes {
        None => shard
            .swarms()
            .map(|(h, s)| ScrapeEntry {
                info_hash: h.as_bytes().to_vec(),
                complete: s.complete,
                incomplete: s.incomplete(),
            })
            .collect(),
        Some(hashes) => hashes
            .iter()
            .filter_map(|h| {
                shard.swarm_stats(h).map(|s| ScrapeEntry {
                    info_hash: h.clone(),
                    complete: s.complete,
                    incomplete: s.incomplete(),
                })
            })
            .collect(),
    }
}

/// A listener as seen by one worker.
pub(crate) struct WorkerListener {
    pub index: usize,
    pub socket: StdListener,
}

/// Runs worker `id` until shutdown.
pub(crate) fn run(
    id: usize,
    shared: Arc<Shared>,
    inbox: mpsc::UnboundedReceiver<Vec<Event>>,
    listeners: Vec<WorkerListener>,
    metrics: Option<StdListener>,
    mut phase: watch::Receiver<Phase>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("worker runtime");
    let local = LocalSet::new();
    local.block_on(&runtime, async move {
        let seed = shared.seed ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let me = Rc::new(Worker::new(id, shared, seed));
        spawn_local(me.clone().inbox(inbox));
        spawn_local(me.clone().expiry());
        if !me.hashed() {
            spawn_local(me.clone().load_ticker());
        }
        let mut accepting: Vec<_> = listeners
            .into_iter()
            .map(|listener| {
                let socket = TcpListener::from_std(listener.socket).expect("listener");
                spawn_local(conn::accept_loop(me.clone(), listener.index, socket))
            })
            .collect();
        if let Some(socket) = metrics {
            let socket = TcpListener::from_std(socket).expect("metrics listener");
            accepting.push(spawn_local(crate::metrics::accept_loop(me.clone(), socket)));
        }
        let has_tls = me.shared.listeners.iter().any(|l| l.cert.is_some());
        if id == 0 && has_tls && me.shared.tls_reload_interval > 0 {
            spawn_local(me.clone().watch_certificates());
        }
        let Ok(Phase::Drain(deadline)) = phase.wait_for(|p| *p != Phase::Running).await.map(|p| *p)
        else {
            return;
        };
        // Graceful shutdown: stop accepting (closes this worker's listening sockets), close
        // every WebSocket, wait until they are gone, the deadline, or an immediate stop.
        for task in &accepting {
            task.abort();
        }
        me.drain();
        let drained = async {
            while !me.conns.borrow().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            _ = drained => {}
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
            _ = phase.wait_for(|p| *p == Phase::Stop) => {}
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_id_packs_worker_generation_and_slot() {
        let id = conn_id(63, 0xFF_FFFF, 0xFFFF_FFFF);
        assert_eq!(
            (conn_worker(id), conn_generation(id), conn_slot(id)),
            (63, 0xFF_FFFF, 0xFFFF_FFFF)
        );
        let id = conn_id(5, 0x1_000_001, 7); // generation wraps to 24 bits
        assert_eq!(
            (conn_worker(id), conn_generation(id), conn_slot(id)),
            (5, 1, 7)
        );
    }
}
