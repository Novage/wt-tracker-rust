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
use wt_core::{ConnId, Outbox, Shard};
use wt_proto::{Batch, Encoder, Message, OwnedMessage, ProtoError};

use crate::Shared;
use crate::conn;

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
    /// `(info_hash, peers)` of every swarm of this shard, for `/stats.json`.
    Stats {
        reply: oneshot::Sender<Vec<(Vec<u8>, u32)>>,
    },
}

pub(crate) struct ScrapeEntry {
    pub info_hash: Vec<u8>,
    pub complete: u32,
    pub incomplete: u32,
}

/// Outgoing frames queued for a connection's writer task.
pub(crate) enum Out {
    Text(Bytes),
    Pong(Bytes),
    Close,
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
        }
    }

    /// Tracker clock: seconds since the worker started.
    fn now(&self) -> u32 {
        self.start.elapsed().as_secs() as u32
    }

    pub fn shard_of(&self, info_hash: &[u8]) -> usize {
        (self.shared.router.hash_one(info_hash) % self.shared.workers as u64) as usize
    }

    // ---- connections ----

    /// Registers a WebSocket connection: its id, writer wake-up and reader stop signal.
    pub fn register(&self) -> (ConnId, Rc<Notify>, Rc<Notify>) {
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
        });
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
    /// writer sends a close frame, the reader stops.
    pub fn begin_close(self: &Rc<Self>, conn: ConnId) {
        let mask = {
            let mut conns = self.conns.borrow_mut();
            let Some(entry) = conns
                .get_mut(conn_slot(conn))
                .filter(|e| e.generation == conn_generation(conn) && !e.closing)
            else {
                return;
            };
            entry.closing = true;
            entry.queue.push_back(Out::Close);
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
        }
    }

    /// Closes any connection, local or on another worker.
    fn close_any(self: &Rc<Self>, conn: ConnId) {
        match conn_worker(conn) {
            w if w == self.id => self.begin_close(conn),
            w => self.push_remote(w, Event::Close { conn }),
        }
    }

    // ---- message flow ----

    /// One text/binary frame from a connection of this worker. `Err`: close the connection.
    pub async fn handle_frame(
        self: &Rc<Self>,
        conn: ConnId,
        frame: Bytes,
    ) -> Result<(), ProtoError> {
        let message = <wt_proto::DefaultBackend as wt_proto::Backend>::parse(&frame)?;
        let workers = self.shared.workers;

        if let Message::Scrape { info_hashes } = &message {
            return match info_hashes {
                Some(hashes) if hashes.len() == 1 && workers > 1 => {
                    let shard = self.shard_of(&hashes[0]);
                    self.route(shard, conn, &frame, &message)
                }
                _ if workers == 1 => self.apply_local(conn, &message),
                _ => {
                    let hashes = info_hashes
                        .as_ref()
                        .map(|hashes| hashes.iter().map(|h| h.to_vec()).collect());
                    self.scrape(conn, hashes).await;
                    Ok(())
                }
            };
        }

        match message.route_info_hash() {
            Some(info_hash) => {
                let shard = self.shard_of(info_hash.as_bytes());
                if matches!(message, Message::Announce { .. }) {
                    let mut conns = self.conns.borrow_mut();
                    if let Some(entry) = conns.get_mut(conn_slot(conn)) {
                        entry.shard_mask |= 1 << shard;
                    }
                }
                self.route(shard, conn, &frame, &message)
            }
            // A stop that cannot match anything.
            None if matches!(message, Message::Stop { .. }) => Ok(()),
            // An answer without a usable info_hash: fine with one shard (JS semantics), but it
            // cannot be routed between shards (JS multi-worker rejects it too).
            None if workers == 1 => self.apply_local(conn, &message),
            None => Err(ProtoError::BadField("info_hash")),
        }
    }

    fn route(
        self: &Rc<Self>,
        shard: usize,
        conn: ConnId,
        frame: &Bytes,
        message: &Message<'_>,
    ) -> Result<(), ProtoError> {
        if shard == self.id {
            self.apply_local(conn, message)
        } else {
            let message = Box::new(OwnedMessage::new(frame.clone(), message));
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

    fn push_remote(self: &Rc<Self>, worker: usize, event: Event) {
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
                            if wt_proto::apply(
                                &mut shard,
                                now,
                                conn,
                                &message.message(),
                                &mut encoder,
                            )
                            .is_err()
                            {
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
                            let _ = reply.send(
                                shard
                                    .swarms()
                                    .map(|(h, s)| (h.as_bytes().to_vec(), s.peers))
                                    .collect(),
                            );
                        }
                    }
                }
            }
            let batch = self.encoder.borrow_mut().take();
            self.dispatch(batch);
            for conn in rejected {
                self.close_any(conn);
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
                    .filter(|h| self.shard_of(h) == shard)
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

        let mut encoder = Encoder::new();
        let out: &mut dyn Outbox<wt_proto::Payload<'_>> = &mut encoder;
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
        self.dispatch(encoder.take());
    }

    /// `(info_hash, peers)` of all swarms of all shards, per shard (for `/stats.json`).
    pub async fn stats(self: &Rc<Self>) -> Vec<Vec<(Vec<u8>, u32)>> {
        let mut per_shard = vec![Vec::new(); self.shared.workers];
        let mut pending = Vec::new();
        for (shard, slot) in per_shard.iter_mut().enumerate() {
            if shard == self.id {
                *slot = self
                    .shard
                    .borrow()
                    .swarms()
                    .map(|(h, s)| (h.as_bytes().to_vec(), s.peers))
                    .collect();
            } else {
                let (reply, rx) = oneshot::channel();
                self.push_remote(shard, Event::Stats { reply });
                pending.push((shard, rx));
            }
        }
        for (shard, rx) in pending {
            per_shard[shard] = rx.await.unwrap_or_default();
        }
        per_shard
    }

    async fn expiry(self: Rc<Self>) {
        let interval = Duration::from_secs(self.shared.settings.announce_interval.max(1) as u64);
        loop {
            tokio::time::sleep(interval).await;
            let batch = {
                let mut encoder = self.encoder.borrow_mut();
                self.shard.borrow_mut().expire(self.now(), &mut *encoder);
                encoder.take()
            };
            self.dispatch(batch);
        }
    }
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
    mut shutdown: watch::Receiver<bool>,
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
        for listener in listeners {
            let socket = TcpListener::from_std(listener.socket).expect("listener");
            spawn_local(conn::accept_loop(me.clone(), listener.index, socket));
        }
        let _ = shutdown.wait_for(|stop| *stop).await;
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
