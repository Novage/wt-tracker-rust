# wt-tracker-rust — specification

Rust port of [wt-tracker](../../wt-tracker) (Node.js + uWebSockets.js WebTorrent tracker).
This file describes the **implemented** behaviour. Planned work is listed only in §12.

| Area | Status |
|---|---|
| Tracker math (`crates/wt-core`) | implemented |
| Benchmarks vs JS (`crates/wt-bench`, `bench/`) | implemented |
| Protocol layer (JSON ↔ `Request`) | planned |
| Server (WebSocket, TLS, sharding across cores) | planned |

---

## 1. Goals

- Multi-core: one single-threaded shard per core, no locks on the hot path.
- Fast: zero-copy message handling, a WebSocket stack on par with uWebSockets.
- Behaviour compatible with the JS `FastTracker` (`../wt-tracker/src/fast-tracker.ts`), with the
  deliberate differences listed in §8.

## 2. Architecture

- **`wt-core`** is sans-IO: no async, sockets, threads or clocks. A `Shard` owns a set of swarms.
  Requests come in through `Shard::handle(now, conn, Request, &mut Outbox)`; everything to send
  goes out synchronously through the `Outbox` trait. The caller supplies time (`now`, seconds,
  monotonic) and connection ids.
- **Payload-agnostic:** `Request<'a, O>` and `Outbox<O>` are generic over the offer/answer payload
  `O`. The core never inspects it, only passes references through. The protocol layer will use
  `Bytes` slices of the received frame, so SDPs are never copied or re-escaped.
- **Sharding (planned):** a server runs N shards, routing each request by
  `hash(info_hash) % N`. Every request carries `info_hash`, so answers route without a global
  peer table.

| Crate / dir | Purpose |
|---|---|
| `crates/wt-core` | `Shard`, `Key`, `Request`, `Outbox`, `Settings` |
| `crates/wt-core/tests` | ported JS tests, behaviour tests, model-based proptest |
| `crates/wt-bench` | `wt-bench` (timing + scaling), `wt-bench-mem` (memory) |
| `crates/wt-difftest`, `difftest/run.ts` | differential test: same random traces through JS `FastTracker` and Rust `Shard` |
| `bench/js` | JS twins of every scenario, run against `../wt-tracker/src/fast-tracker.ts` |
| `bench/run.sh`, `bench/compare.ts` | run both sides and regenerate §11 |
| `docs/SPEC.md` | this specification |
| `AGENTS.md` | agent-neutral working rules (spec upkeep, checklist); `CLAUDE.md` imports it |
| `.agents/skills/` | shared agent skills; `.claude/skills` symlinks here |
| `scripts/check-spec.sh` | fails when code changed without a `docs/SPEC.md` change |

## 3. Concepts and data model

| Concept | Identity | Notes |
|---|---|---|
| Connection | `ConnId(u64)`, opaque, from the I/O layer | carries **any number of peers** |
| Peer | `peer_id: Key` | belongs to exactly one connection |
| Swarm | `info_hash: Key` | exists only while it has members |
| Membership | (peer, swarm) | a peer can be in **any number of swarms**; holds `last_seen`, `completed` |

So one web client (one connection) may announce several peer_ids, and each peer_id may be in
several swarms at once.

**Key:** the unescaped bytes of `info_hash` / `peer_id`, stored inline (no heap):
`MAX_KEY_LEN = 40` bytes, which covers 20-character binary strings even when every character
is ≥ 0x80 (2 bytes in UTF-8). Longer keys are rejected (§5.8).

**Storage** (indices are `u32`):

```text
swarms:  Slab<Swarm  { slots: Vec<Slot{conn, member}>, completed, cursor, info_hash }>
peers:   Slab<Peer   { conn, members: SmallVec<[{swarm, member}; 2]>, peer_id }>
members: Slab<Member { peer, swarm, pos, last_seen, completed }>
swarm_by_hash, peer_by_id: hashbrown::HashTable<u32>     // keys read from the slabs
conn_peers: HashMap<ConnId, SmallVec<[u32; 2]>>
hasher: foldhash, randomly seeded (HashDoS-resistant); rng: fastrand, seeded per shard
```

- `Swarm.slots` caches each member's connection, so offer fan-out reads one contiguous array.
- `Member.pos` is the member's index in `Swarm.slots`. Removal is a swap-remove that fixes the
  moved member's `pos` in O(1).

## 4. Invariants

These must hold after every public call. `Shard::check_invariants()` verifies them, and the
tests call it after every step.

1. No swarm is empty, and every swarm is indexed by its `info_hash`.
2. For every slot at position `i` of swarm `s`, its member has `swarm == s`, `pos == i`, and its
   peer's `conn` equals the slot's `conn`.
3. `swarm.completed` equals the number of members with `completed == true`.
4. No peer is without memberships. Every peer is indexed by its `peer_id`, and a peer is in a
   given swarm at most once.
5. Every peer's membership list matches the members table (`member.peer`, `member.swarm`).
6. `conn_peers[c]` lists exactly the peers whose `conn == c`, and is never empty.
7. Totals agree: Σ slots = Σ peer memberships = members; the index table sizes equal the slab
   sizes.

## 5. Behaviour

### 5.1 Announce (`Request::Announce`)

Input: `info_hash`, `peer_id`, `event` (`None` | `Started` | `Completed`), `left_zero`,
`numwant: Option<u32>`, `offers: Option<&[O]>`.

1. Both keys must be ≤ `MAX_KEY_LEN`, otherwise `Err(KeyTooLong)` with no state change.
2. If `peer_id` is known on a **different** connection, that peer is removed entirely (all its
   swarms, `peer_removed` emitted) and recreated on the new connection.
3. Get or create the swarm. Then one of:
   - new peer: create it plus its membership;
   - known peer, new swarm: add a membership;
   - known membership: refresh `last_seen = now`.
4. `completed = (event == Completed) || left_zero`. When true, the membership becomes completed
   and is counted once. Completed **never reverts**.
5. Emit `announce_reply(conn, info_hash, interval = announce_interval, complete, incomplete)`,
   where `incomplete = members − complete`.
6. Fan out offers (§5.2).

### 5.2 Offer fan-out

- Skip if the swarm has ≤ 1 member, `offers` is `None`, or `numwant` is `None`.
- `n = min(members − 1, offers.len(), max_offers, numwant)`. The announcer never receives its own
  offer. Offers `0..n` go to `n` **distinct** receivers in order. Each receiver gets
  `offer(to_conn, from_peer_id, info_hash, &offers[i])`.
- If `n == members − 1`, every other member gets one offer, in swarm order.
- Otherwise the receivers are chosen by `Settings::offer_selection`:

| Strategy | Receivers | RNG per announce | Neighbour co-selection¹ |
|---|---|---|---|
| `RandomSample` **(default)** | uniformly random set of `n` distinct others (Floyd's algorithm) | `n` | ≈ 0.20 (uniform) |
| `RandomWindow` (JS behaviour) | contiguous window from a random start, wrapping, skipping self | 1 | ≈ 0.83 |
| `RoundRobin` | contiguous window from a per-swarm cursor that continues where the last announce stopped | 0 | ≈ 0.82 |

¹ P(two adjacent swarm entries both receive offers | either does), with 10 receivers out of 29
others; measured by `random_sample_does_not_cluster_neighbours`. Adjacent entries are roughly
peers that joined at the same time. All three strategies give every peer the same chance of
receiving offers. Only `RandomSample` makes the receivers of one announce independent of each
other.

### 5.3 Answer (`Request::Answer { to_peer_id, answer }`)

Looks up `to_peer_id` in the shard's peer table (not checked against a swarm). If found, emits
`answer(peer.conn, answer)`; otherwise `Err(UnknownPeer)`.

### 5.4 Stop (`Request::Stop { info_hash, peer_id }`)

If the swarm, the peer and their membership all exist, the membership is removed. A peer left
without memberships is removed (`peer_removed`). Anything unknown is a silent no-op. The
requesting connection is not checked (§8).

### 5.5 Scrape (`Request::Scrape { target }`)

`All` covers every swarm. `One(h)` and `Many([h…])` produce one entry per requested hash, in
order, with zeros for unknown hashes. Each entry is `scrape_entry(conn, info_hash, complete,
incomplete, downloaded = complete)`, and the reply always ends with `scrape_end(conn)`, even when
it is empty.

### 5.6 Disconnect (`Shard::disconnect(conn)`)

Removes every peer of the connection, with one `peer_removed` per peer. An unknown connection is
a no-op.

### 5.7 Expiry (`Shard::expire(now) -> usize`)

Removes every membership with `now − last_seen > 2 × announce_interval` (strictly greater,
saturating), then every peer left without memberships (`peer_removed`). Returns the number of
memberships removed. The caller decides when to run it; the JS tracker sweeps every
`announce_interval`.

### 5.8 Errors (`TrackerError`)

| Error | When | State change |
|---|---|---|
| `KeyTooLong` | announce with `info_hash` or `peer_id` > `MAX_KEY_LEN` | none |
| `UnknownPeer` | answer to an unknown `to_peer_id` | none |

Stop and scrape never fail. Malformed JSON and missing fields will be rejected by the protocol
layer before reaching the core.

### 5.9 Outbox events

`announce_reply`, `offer`, `answer`, `scrape_entry`, `scrape_end`, `peer_removed`. All are
called synchronously during the call that causes them, with borrowed arguments: copy or
serialize before returning. `peer_removed` is emitted for disconnect, a stop of the last swarm,
expiry, and a connection change. It corresponds to JS `onRemovePeer`.

## 6. Settings

| Field | Default | Meaning |
|---|---|---|
| `max_offers` | 20 | max offers forwarded per announce |
| `announce_interval` | 20 | seconds; sent in replies; expiry timeout is 2× this |
| `offer_selection` | `RandomSample` | §5.2 |

`Shard::new(settings, seed)`: `seed` seeds the offer RNG (deterministic tests).

## 7. Wire mapping (for the planned protocol layer)

| JSON | `Request` |
|---|---|
| `action: "announce"`, no `event`, `answer` present | `Answer { to_peer_id, answer }` |
| `action: "announce"`, `event` absent / `"started"` / `"completed"` | `Announce` (`event` → `AnnounceEvent`, `left == 0` → `left_zero`, integer `numwant` → `Some`) |
| `action: "announce"`, `event: "stopped"` | `Stop` |
| `action: "scrape"` | `Scrape` (`info_hash` absent → `All`, string → `One`, array → `Many`) |
| anything else | error, handled by the protocol layer |

## 8. Differences from the JS `FastTracker`

| Topic | JS | Rust | Why |
|---|---|---|---|
| Partial offer fan-out | random window | `RandomSample` by default; window and round-robin available | no clustering of receivers; costs about 30 ns per announce |
| Key length | any string | ≤ 40 bytes, else `KeyTooLong` | inline keys, no allocation |
| Time | `performance.now()` ms, internal `setInterval` sweep | caller passes `now` seconds and calls `expire` | sans-IO, deterministic |
| Offer validation | throws mid-loop, after some offers were already sent | payload is opaque; validated by the protocol layer before the core | all-or-nothing |
| Scrape `Many` with duplicates | deduplicated (object keys) | one entry per requested hash | the protocol layer may deduplicate |
| Peer identity | global per tracker (per worker in multi-worker) | per shard | sharding |
| Stop from another connection | allowed | allowed (parity) | hardening is planned (§12) |
| Answer target not in the same swarm | allowed | allowed (parity) | hardening is planned (§12) |

## 9. Testing requirements

- `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `scripts/check-spec.sh` must pass.
- `tests/announce.rs` and `tests/simulation.rs` port the JS tests and must keep passing.
- `tests/behaviour.rs` must have at least one test per rule in §5 and per strategy in §5.2.
- `tests/model.rs` compares random operation sequences against a naive model, for **every**
  `OfferSelection` variant. New operations or semantics must be added to the model.
- Every public behaviour change must go through `check_invariants()` in tests.
- **Differential test vs JS** (`node difftest/run.ts [traces] [ops] [first-seed]`, needs
  `../wt-tracker`). It generates seeded random traces (announce with all event, `left`,
  `numwant` and offer variants, answers including unknown targets, stop, scrape All/One/Many,
  disconnect, clock advance, expiry) over 1–6 connections, 1–10 peer_ids (including non-ASCII,
  several per connection, ~8% connection moves) and 1–4 swarms. Each trace runs through the JS
  `FastTracker` (fake clock, captured sweep, seeded `Math.random`) and through `wt-difftest`
  for **every** `OfferSelection`. After every op it checks:
  - **equality JS ↔ Rust**: error flag, announce replies, answers, removed peers (as a set),
    scrape files, full state (swarm → sorted peer_ids + completed count; peer_id →
    connection), and offers as a multiset of (from, info_hash, offer id);
  - **the spec, on each side independently**: §5.1 reply counts; §5.2 offer count
    `n = min(others, offers, max_offers, numwant)`, offer ids `0..n`, receivers a sub-multiset
    of the other members' connections (all of them when `n == others`); §5.3 answer routing or
    error; §5.5 scrape entries; removed peers = peers that left the state or changed
    connection; Rust `check_invariants()`.

  Offer receivers are not compared exactly: both sides choose them randomly, and the swarm order
  may legitimately differ after multi-peer removals (disconnect, expiry). Failing traces are
  saved to `difftest/failures/` and the run exits non-zero. Behaviour changes that intentionally
  diverge from JS must update both this harness and §8.

## 10. Benchmarks

Run `bench/run.sh`. It builds and runs `wt-bench`, `wt-bench-mem` and `bench/js/bench.ts` (JS
tracker from `../wt-tracker`, override with `WT_TRACKER_DIR`), then regenerates §11. Every
scenario exists in both Rust and JS with identical parameters, ids (`p`/`h` + 19-digit index)
and message counting. The tables check that both sides emit the same message counts.

| Scenario | Shape | Op |
|---|---|---|
| `join_one_swarm` | 100k peers (1 conn each) join 1 swarm | announce |
| `join_many_swarms` | 1M peers join 100k swarms (`i % 100k`) | announce |
| `multi_peer_join` | 100k conns × 3 peer_ids × 2 swarms (of 10k) = 600k memberships | announce |
| `reannounce` | every membership of `multi_peer_join` re-announces with 10 offers | announce |
| `reannounce_one_swarm` | every peer of `join_one_swarm` re-announces with 10 offers | announce |
| `*_window`, `*_round_robin` | the same, with the other `OfferSelection` (Rust only) | announce |
| `answer` | 1M answers to pseudo-random known peers | answer |
| `stop_all` | stop every membership of `multi_peer_join` | stop |
| `disconnect_all` | disconnect all 100k conns of `multi_peer_join` | disconnect |
| `expire_sweep` | `multi_peer_join` at t=0, even peers refreshed at t=30, sweep at t=45 | per membership scanned |
| memory | live heap after building `multi_peer_join` / `join_many_swarms` | bytes per membership |
| scaling | re-announce on 1/2/4/8 threads. Strong: one state split by `swarm % N`. Weak: a full copy per thread. | aggregate announces/s |

Caveats: JSON parsing and serialization are excluded on both sides. JS benefits from cached
string hashes on pre-made ids; in production every message brings fresh strings. Rust heap
figures include `Vec` growth slack; on macOS, RSS overstates because freed pages are retained.

## 11. Performance results

<!-- perf-tables:begin -->
- Generated by `bench/run.sh` on 2026-10-01. Do not edit by hand.
- Rust: {"arch":"aarch64","os":"macos","parallelism":8}
- JS: {"node":"v26.3.0","cpu":"Apple M1","parallelism":8}
- Median of 5 runs after 2 warmups. 10 offers per announce, numwant 10. Messages are counted, not serialized.
- Rust scenarios without a suffix use the default `RandomSample`; `_window` / `_round_robin` use the other strategies. JS always uses a random window.

### Time per operation

| Scenario | Ops/run | JS ns/op | Rust ns/op | Speedup | Same messages |
|---|---:|---:|---:|---:|:-:|
| join_one_swarm | 100,000 | 447.7 | 139.3 | 3.2× | yes |
| join_many_swarms | 1,000,000 | 868.0 | 225.8 | 3.8× | yes |
| multi_peer_join | 600,000 | 826.1 | 170.5 | 4.8× | yes |
| reannounce | 600,000 | 583.8 | 139.4 | 4.2× | yes |
| answer | 1,000,000 | 291.4 | 71.1 | 4.1× | yes |
| reannounce_one_swarm | 100,000 | 240.1 | 92.5 | 2.6× | yes |
| reannounce_window (vs JS reannounce) | 600,000 | 583.8 | 108.1 | 5.4× | n/a |
| reannounce_one_swarm_window (vs JS reannounce_one_swarm) | 100,000 | 240.1 | 58.1 | 4.1× | n/a |
| reannounce_round_robin (vs JS reannounce) | 600,000 | 583.8 | 88.8 | 6.6× | n/a |
| reannounce_one_swarm_round_robin (vs JS reannounce_one_swarm) | 100,000 | 240.1 | 54.4 | 4.4× | n/a |
| stop_all | 600,000 | 198.6 | 132.2 | 1.5× | yes |
| disconnect_all | 100,000 | 736.8 | 233.2 | 3.2× | yes |
| expire_sweep | 600,000 | 100.1 | 24.7 | 4.1× | yes |

### Memory per membership (peer-in-swarm)

| State | Memberships | JS heapUsed B | Rust heap B (incl. Vec slack) | Rust RSS B |
|---|---:|---:|---:|---:|
| multi_peer_join | 600,000 | 156.7 | 152.5 | 92.5 |
| join_many_swarms | 1,000,000 | 233.6 | 233.9 | 282.1 |

### Multi-core scaling (re-announce, M announces/s)

Strong: one 600k-membership state split across N shards by `swarm % N`. Weak: every shard holds a full copy.

| Threads | JS strong | Rust strong | Rust/JS | JS weak | Rust weak |
|---:|---:|---:|---:|---:|---:|
| 1 | 1.89 (1.0×) | 6.89 (1.0×) | 3.6× | 1.80 (1.0×) | 6.94 (1.0×) |
| 2 | 2.12 (1.1×) | 9.58 (1.4×) | 4.5× | 2.52 (1.4×) | 9.12 (1.3×) |
| 4 | 3.86 (2.0×) | 18.05 (2.6×) | 4.7× | 3.68 (2.1×) | 13.55 (2.0×) |
| 8 | 4.18 (2.2×) | 31.81 (4.6×) | 7.6× | 3.10 (1.7×) | 18.66 (2.7×) |
<!-- perf-tables:end -->

## 12. Open items

- **Memory (next):** about 150 B per membership, the same as JS. Shrinking the key to 24 bytes
  saved only 12%. Remaining costs are the per-connection map and slab/`Vec` growth slack.
- Protocol crate: borrowed JSON parsing with `RawValue` SDPs, and direct response encoding.
- Server: per-core runtimes, `SO_REUSEPORT`, cross-shard queues; choose the WebSocket stack
  (fastwebsockets vs sockudo-ws) with a tracker-shaped load test.
- Hardening: check the requesting connection on stop; optionally require the answer target to
  share the swarm.
