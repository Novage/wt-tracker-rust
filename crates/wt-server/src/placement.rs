//! Content placement (spec §13.3): which shard owns an info_hash, and where new content goes.
//!
//! In `content` mode a global directory maps every info_hash to the worker whose shard holds its
//! swarm. A new info_hash is bound to the worker of the connection that announces it first, so
//! the swarms of one piece of content (video, audio, every quality) end up on one shard, and a
//! connection moves to that worker at its first announce. New content is spread by load.

use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

use wt_core::Key;

/// How info_hashes are assigned to shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Directory + connection moves (default).
    Content,
    /// `foldhash(info_hash) % workers`; no directory, connections never move.
    Hash,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Content => "content",
            Mode::Hash => "hash",
        }
    }
}

/// New content goes to another worker only if it has fewer than this factor of the own
/// worker's connections.
pub(crate) const MOVE_FACTOR: f64 = 0.8;
/// A new info_hash of a placed connection spills to another worker only when the own worker is
/// at least this busy (permille of wall time)...
pub(crate) const SPILL_BUSY: u32 = 800;
/// ...and the other worker is less busy by at least this much (permille).
pub(crate) const SPILL_MARGIN: u32 = 200;
/// Weight of the newest busy sample in the average (sampled every `LOAD_TICK`).
pub(crate) const BUSY_ALPHA: f64 = 0.3;
pub(crate) const LOAD_TICK: std::time::Duration = std::time::Duration::from_millis(100);

/// info_hash → owning worker. Invariant: a swarm for `h` exists on shard `w` ⇒ `get(h) ==
/// Some(w)`. Kept by binding before creating (`claim`) and releasing only empty entries from
/// the owner's thread (`release`).
pub(crate) struct Directory {
    map: papaya::HashMap<Key, u8, foldhash::fast::RandomState>,
}

impl Directory {
    pub fn new() -> Self {
        Self {
            map: papaya::HashMap::builder()
                .hasher(foldhash::fast::RandomState::default())
                .build(),
        }
    }

    pub fn get(&self, info_hash: &Key) -> Option<usize> {
        self.map.pin().get(info_hash).map(|&w| w as usize)
    }

    /// Binds `info_hash` to `worker` unless it is bound already; returns the owner.
    pub fn claim(&self, info_hash: Key, worker: usize) -> usize {
        *self.map.pin().get_or_insert(info_hash, worker as u8) as usize
    }

    /// Removes the binding if it is still `worker`'s.
    pub fn release(&self, info_hash: &Key, worker: usize) -> bool {
        matches!(
            self.map
                .pin()
                .remove_if(info_hash, |_, &w| w as usize == worker),
            Ok(Some(_))
        )
    }

    /// Hashes bound to `worker`.
    pub fn owned_by(&self, worker: usize) -> Vec<Key> {
        self.map
            .pin()
            .iter()
            .filter(|&(_, &w)| w as usize == worker)
            .map(|(&h, _)| h)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

/// A worker's load as seen by placement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Load {
    /// Open WebSocket connections on the worker (exact, updated on open / close / move).
    pub conns: u32,
    /// Busy time of the worker's runtime, permille of wall time (moving average).
    pub busy: u32,
}

/// Per-worker loads, written only by their own worker.
pub(crate) struct Loads(Vec<(AtomicU32, AtomicU32)>);

impl Loads {
    pub fn new(workers: usize) -> Self {
        Self(
            (0..workers)
                .map(|_| (AtomicU32::new(0), AtomicU32::new(0)))
                .collect(),
        )
    }

    pub fn snapshot(&self) -> Vec<Load> {
        self.0
            .iter()
            .map(|(conns, busy)| Load {
                conns: conns.load(Relaxed),
                busy: busy.load(Relaxed),
            })
            .collect()
    }

    pub fn add_conn(&self, w: usize, delta: i32) {
        self.0[w].0.fetch_add(delta as u32, Relaxed);
    }

    /// Folds a busy sample (`busy` of `wall` time) into worker `w`'s average.
    pub fn sample_busy(&self, w: usize, busy: f64, wall: f64) {
        let sample = (busy / wall).clamp(0.0, 1.0) * 1000.0;
        let old = self.0[w].1.load(Relaxed) as f64;
        let new = old + BUSY_ALPHA * (sample - old);
        self.0[w].1.store(new.round() as u32, Relaxed);
    }
}

/// Worker for new content announced by an unplaced connection of worker `me`: the one with
/// fewer connections of two random workers `pick`, if clearly fewer than `me`'s.
pub(crate) fn for_new_content(loads: &[Load], me: usize, pick: [usize; 2]) -> usize {
    let [a, b] = pick;
    let best = if loads[b].conns < loads[a].conns {
        b
    } else {
        a
    };
    if (loads[best].conns as f64) < MOVE_FACTOR * loads[me].conns as f64 {
        best
    } else {
        me
    }
}

/// Worker for a new info_hash announced by a placed connection of worker `me`: `me`, unless it
/// is saturated and the less busy of `pick` is clearly less busy (spill; splits the content).
pub(crate) fn for_new_hash(loads: &[Load], me: usize, pick: [usize; 2]) -> usize {
    if loads[me].busy < SPILL_BUSY {
        return me;
    }
    let [a, b] = pick;
    let best = if loads[b].busy < loads[a].busy { b } else { a };
    if loads[best].busy + SPILL_MARGIN <= loads[me].busy {
        best
    } else {
        me
    }
}

/// Two distinct random workers (the same one twice if there is only one).
pub(crate) fn pick_two(rng: &mut fastrand::Rng, workers: usize) -> [usize; 2] {
    let a = rng.usize(..workers);
    if workers < 2 {
        return [a, a];
    }
    let b = (a + 1 + rng.usize(..workers - 1)) % workers;
    [a, b]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Key {
        Key::new(s.as_bytes()).unwrap()
    }

    #[test]
    fn claim_keeps_the_first_owner_and_release_only_by_owner() {
        let dir = Directory::new();
        assert_eq!(dir.get(&key("h")), None);
        assert_eq!(dir.claim(key("h"), 2), 2);
        assert_eq!(dir.claim(key("h"), 3), 2);
        assert!(!dir.release(&key("h"), 3));
        assert_eq!(dir.get(&key("h")), Some(2));
        assert_eq!(dir.owned_by(2), vec![key("h")]);
        assert!(dir.release(&key("h"), 2));
        assert_eq!(dir.get(&key("h")), None);
        assert_eq!(dir.claim(key("h"), 3), 3);
    }

    #[test]
    fn concurrent_claims_have_one_winner() {
        let dir = std::sync::Arc::new(Directory::new());
        let owners: Vec<usize> = (0..8)
            .map(|w| {
                let dir = dir.clone();
                std::thread::spawn(move || {
                    (0..1000)
                        .map(|i| dir.claim(key(&format!("h{i}")), w))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        for i in 0..1000 {
            let first = owners[i];
            assert!((0..8).all(|t| owners[t * 1000 + i] == first));
            assert_eq!(dir.get(&key(&format!("h{i}"))), Some(first));
        }
        assert_eq!(dir.len(), 1000);
    }

    fn conns(c: &[u32]) -> Vec<Load> {
        c.iter().map(|&conns| Load { conns, busy: 0 }).collect()
    }

    fn busy(b: &[u32]) -> Vec<Load> {
        b.iter().map(|&busy| Load { conns: 0, busy }).collect()
    }

    #[test]
    fn new_content_stays_unless_another_worker_has_clearly_fewer_connections() {
        assert_eq!(for_new_content(&conns(&[1, 1, 1, 1]), 1, [2, 3]), 1);
        assert_eq!(for_new_content(&conns(&[100, 90, 85, 100]), 0, [1, 2]), 0);
        assert_eq!(for_new_content(&conns(&[100, 90, 50, 100]), 0, [1, 2]), 2);
        assert_eq!(for_new_content(&conns(&[100, 10, 50, 100]), 0, [2, 1]), 1);
    }

    #[test]
    fn new_hash_spills_only_from_a_saturated_worker() {
        assert_eq!(for_new_hash(&busy(&[700, 0, 0, 0]), 0, [1, 2]), 0);
        // Saturated, but the others are as busy.
        assert_eq!(for_new_hash(&busy(&[900, 850, 800, 900]), 0, [1, 2]), 0);
        // Saturated: the less busy of the two picks.
        assert_eq!(for_new_hash(&busy(&[950, 600, 300, 100]), 0, [1, 2]), 2);
    }

    #[test]
    fn pick_two_gives_distinct_workers() {
        let mut rng = fastrand::Rng::with_seed(1);
        for _ in 0..1000 {
            let [a, b] = pick_two(&mut rng, 3);
            assert!(a != b && a < 3 && b < 3);
        }
        assert_eq!(pick_two(&mut rng, 1), [0, 0]);
    }

    #[test]
    fn loads_track_connections_and_busy_time() {
        let loads = Loads::new(2);
        loads.add_conn(1, 1);
        loads.add_conn(1, 1);
        loads.add_conn(1, -1);
        loads.sample_busy(0, 0.05, 0.1); // 50% busy
        assert_eq!(
            loads.snapshot(),
            vec![
                Load {
                    conns: 0,
                    busy: 150
                },
                Load { conns: 1, busy: 0 }
            ]
        );
        loads.sample_busy(0, 0.05, 0.1);
        assert_eq!(loads.snapshot()[0].busy, 255);
    }
}
