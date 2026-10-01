# wt-tracker-rust — specification

Rust port of [wt-tracker](../../wt-tracker) (Node.js + uWebSockets.js WebTorrent tracker).
This file describes the **implemented** behaviour. Planned work is listed only in §12.

| Area | Status |
|---|---|
| Tracker math (`crates/wt-core`) | implemented |
| Benchmarks vs JS (`crates/wt-bench`, `bench/`) | implemented |
| Protocol layer (`crates/wt-proto`, JSON ↔ `Request`) | implemented |
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
| `crates/wt-proto` | wire protocol: `handle`, parser backends, `Encoder` (§7) |
| `crates/wt-core/tests` | ported JS tests, behaviour tests, model-based proptest |
| `crates/wt-bench` | `wt-bench` (timing + scaling), `wt-bench-mem` (memory) |
| `crates/wt-difftest`, `difftest/run.ts` | wire-level differential test: same random frame traces through the JS tracker and Rust `wt-proto` + `Shard` |
| `bench/fixtures/offer.sdp` | realistic 1.3 KB WebRTC data-channel offer used by the protocol benchmarks |
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

## 7. Wire protocol (`crates/wt-proto`)

`wt_proto::handle(shard, now, conn, frame, &mut Encoder) -> Result<(), ProtoError>` parses one
WebSocket text frame, applies it to the shard and appends the outgoing messages to the encoder
(`handle_with::<B>` picks the parser backend, §7.4). **Every `Err` means: close the connection
and call `Shard::disconnect(conn)`**, as the JS server does (`ws.close()` → `onClose` →
`disconnect`). A rejected frame changes no state.

### 7.1 Incoming frames

The frame must be valid UTF-8 and a JSON object (whitespace around it is allowed, anything else
after it is not). Member names may use escapes. Duplicate members: the last one wins, like
`JSON.parse` (exception: two `to_peer_id` members in an answer → `BadField`). Unknown members are
ignored but still validated (answers forward them).

| Field / case | Rule (same as the JS tracker) | Result |
|---|---|---|
| `action` | `"scrape"` → scrape; `"announce"` → by `event`; missing, not a string, or another value | `UnknownAction` |
| `event` | absent → none; `"started"`, `"completed"`; `"stopped"` → stop; any other value, including `null` | `UnknownEvent` |
| answer | `event` absent and `answer` present with **any** value (even `null`) | answer |
| announce `info_hash`, `peer_id` | must be strings (`BadField`); decoded > 40 bytes → `KeyTooLong`; lone surrogate → `BadField` | `Announce` |
| `numwant` | a number with an integral value (`Number.isInteger`: `10`, `10.0`, `1e1`, `1e300`) → `Some(clamp(n, 0, u32::MAX))` (negative → 0 offers); anything else (`2.5`, `"10"`, `null`, absent) → `None` | |
| `left` | a number equal to 0 (`0`, `-0`, `0.0`) → `left_zero`; `"0"` is not | |
| `offers` | absent → `None`; an array whose items are objects with `offer` an object or array (JS `typeof "object"`, not `null`) → one `Payload::Offer { offer_id, sdp }` per item (raw values, `None` if absent); anything else | `BadField("offers")` |
| answer `to_peer_id` | a string ≤ 40 bytes (`BadField` / `KeyTooLong`); the key must be spelled literally (escaped spelling → `BadField`) | |
| answer `peer_id` | must be a string (not decoded) | `BadField` |
| answer `info_hash` | not checked; kept raw for shard routing | |
| answer body | the frame with the `to_peer_id` member (and one adjacent comma) cut out: `Payload::Answer { head, tail }`, two slices, no copy | |
| stop `peer_id` | must be a string | `BadField` |
| stop ids that cannot match (non-string `info_hash`, > 40 bytes, lone surrogate) | no-op | `Ok` |
| scrape `info_hash` | absent → all swarms; string → that one; array → its string elements in order (others skipped); any other value → none (`"files":{}`) | `Scrape` |

### 7.2 Outgoing messages

Byte-identical to `JSON.stringify` of the JS tracker's message objects:

| Message | Layout |
|---|---|
| announce reply | `{"action":"announce","interval":N,"info_hash":S,"complete":N,"incomplete":N}` |
| offer | `{"action":"announce","info_hash":S,"offer_id":RAW,"peer_id":S,"offer":{"type":"offer","sdp":RAW}}`; `offer_id` / `sdp` omitted when absent; other offer fields dropped |
| answer | `head` + `tail`: the received answer without `to_peer_id`, all other members in order |
| scrape | `{"action":"scrape","files":{S:{"complete":N,"incomplete":N,"downloaded":N},…}}`; a repeated hash keeps its first entry; order for "all swarms" is unspecified |

- `S` (ids, hashes) is written like `JSON.stringify`: `"` and `\` escaped, `\b \t \n \f \r`, other
  characters below 0x20 as `\u00xx` (lowercase), everything else raw UTF-8.
- `RAW` values (`sdp`, `offer_id`, the answer body) are copied verbatim from the received frame:
  never decoded or re-encoded. They equal JS output byte for byte when the input is canonical
  (`JSON.stringify` output, which is what browsers send); otherwise they are semantically equal.

### 7.3 Errors (`ProtoError`)

| Variant | When |
|---|---|
| `InvalidJson` | not UTF-8, not JSON, or data after the object |
| `NotAnObject` | valid JSON whose top level is not an object (including `null`) |
| `UnknownAction` / `UnknownEvent` | §7.1 |
| `BadField(name)` | a field with the wrong type or an unsupported form (§7.1) |
| `KeyTooLong` | announce `info_hash` / `peer_id` or answer `to_peer_id` > 40 bytes |
| `Tracker(TrackerError)` | rejected by the core (§5.8), e.g. answer to an unknown peer |

### 7.4 Parser backends, encoder and allocation

| Backend | How | Allocation per message (steady state) | Default |
|---|---|---|---|
| `SerdeJson` | `serde_json` borrowed visitors + `&RawValue`; UTF-8 of the whole frame validated first (skipped values are forwarded) | **zero** (tested) | yes |
| `Sonic` (cargo feature `sonic`) | `sonic-rs` validating lazy object/array iterators (its serde path copies every escaped string, i.e. every SDP) | zero-copy, but ~26 small allocations per 10-offer announce (member keys), none ≥ 256 bytes (tested) | no |

- Ids are unescaped directly into the inline `Key`; offers are kept in `SmallVec<[Payload; 20]>`;
  only scrape hashes with escapes allocate (`Cow`).
- `Encoder`: one byte buffer plus a message index. `messages()` yields `(ConnId, &[u8])` in emission
  order, `removed()` the `peer_removed` events, `bytes()` the total size; `clear()` keeps capacity.
  Integers via `itoa`.

## 8. Differences from the JS `FastTracker`

| Topic | JS | Rust | Why |
|---|---|---|---|
| Partial offer fan-out | random window | `RandomSample` by default; window and round-robin available | no clustering of receivers; costs about 30 ns per announce |
| Key length | any string | ≤ 40 bytes, else `KeyTooLong` | inline keys, no allocation |
| Time | `performance.now()` ms, internal `setInterval` sweep | caller passes `now` seconds and calls `expire` | sans-IO, deterministic |
| Malformed `offers` | error only when an offer is actually sent (swarm > 1, integer `numwant`), after the reply and possibly some offers went out | `BadField("offers")` before anything happens, whenever offers are malformed | all-or-nothing; affects only malformed clients |
| Scrape with repeated hashes | one entry (object keys) | core reports each; the encoder keeps the first | same on the wire |
| Raw `sdp` / `offer_id` / answer body | re-serialized canonically | forwarded verbatim | zero-copy; byte-identical for canonical input, semantically equal otherwise |
| Invalid UTF-8 | replaced with U+FFFD | `InvalidJson` | text frames must be UTF-8 |
| Lone surrogate (`\ud800`) | accepted | `SerdeJson`: `BadField` in ids, forwarded verbatim elsewhere; `Sonic`: `InvalidJson` anywhere | no UTF-8 encoding exists |
| Two `to_peer_id` members in an answer | both removed | `BadField("to_peer_id")` | a cut would forward the other copy |
| `to_peer_id` key spelled with escapes | removed | `BadField("to_peer_id")` | never produced by `JSON.stringify` |
| Integer-like member names (`"1"`) | reordered first when re-serialized | order preserved | JS object key order quirk |
| Frame `null` | **uncaught `TypeError`** in the message handler (`null.action`) — a crash path | `NotAnObject` → close | JS bug |
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
- `crates/wt-proto/tests` (each runs against **every** compiled-in backend; `cargo test
  --workspace` enables `sonic` through `wt-bench`):
  - `parse.rs`: one test per rule of §7.1, plus errors, duplicates, whitespace and the answer cut
    at first / middle / last position;
  - `encode.rs`: golden bytes for every message of §7.2, captured from node `JSON.stringify`;
  - `alloc.rs`: the allocation guarantees of §7.4 (counting allocator);
  - `roundtrip.rs` (proptest): arbitrary Unicode ids, sent canonically or fully `\u`-escaped,
    decode to the same `Key` and re-encode exactly like `serde_json::to_string` (=
    `JSON.stringify`).
- **Differential test vs JS** (`node difftest/run.ts [traces] [ops] [first-seed]`, needs
  `../wt-tracker`), at the wire level. It generates seeded random traces of **raw JSON frames**
  over 1–6 connections, 1–10 peer_ids (non-ASCII and control characters, several per
  connection, ~8% connection moves) and 1–4 swarms: announces with every `event` / `left` /
  `numwant` form of §7.1, 0–12 offers (`offer_id` string / number / `null` / absent, `sdp` with
  CRLF, quotes, backslashes, U+2028, emoji, control characters, absent; array `offer`; extra
  members), answers (`answer` object / `null` / number, extra members, any member order,
  unknown targets), stops, scrapes (all / one / array with non-strings / number), error frames
  that both sides reject without a state change, disconnects, clock advances and expiry, with
  shuffled member order and ~6% non-canonical frames (whitespace, escaped spellings, duplicate
  members). JS runs each frame like `uws-tracker` (`JSON.parse` → `FastTracker` with fake clock,
  captured sweep and seeded `Math.random` → `JSON.stringify`; a `SyntaxError` / `TrackerError`
  closes the connection → `disconnect`); Rust runs `wt-difftest` (`wt_proto::handle_with` +
  `Shard`, an error → `disconnect`) for **every** parser backend × `OfferSelection`. After every
  op it checks:
  - **equality JS ↔ Rust**: error flag, removed peers (as a set), full state (swarm → sorted
    peer_ids + completed count; peer_id → connection), and **sent bytes**: every non-offer
    message per connection and every offer message as a multiset of texts, byte for byte
    (compared after `JSON.parse` for non-canonical frames and scrape-all);
  - **the spec, on each side independently**: §5.1 reply; §5.2 offer count
    `n = min(others, offers, max_offers, numwant)` with exactly the first `n` payloads, receivers
    a sub-multiset of the other members' connections (all of them when `n == others`); §5.3 /
    §7.2 answer routing and body (the frame minus `to_peer_id`) or error; §5.5 scrape files;
    removed peers = peers that left the state or changed connection; expected errors; Rust
    `check_invariants()`.

  Offer receivers are not compared exactly: both sides choose them randomly, and the swarm order
  may legitimately differ after multi-peer removals (disconnect, expiry). The deliberate
  differences of §8 are not generated (covered by unit tests). Failing traces are saved to
  `difftest/failures/` and the run exits non-zero. Behaviour changes that intentionally diverge
  from JS must update both this harness and §8.

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
| `proto_parse_announce` | parse one of 1000 distinct ~14 KB announce frames (10 offers × 1.3 KB SDP from `bench/fixtures/offer.sdp`); JS: `StringDecoder` + `JSON.parse` like uws-tracker | parse |
| `proto_encode_announce` | encode reply + 10 offers from a parsed announce; JS: FastTracker's reused objects + `JSON.stringify` | encode |
| `pipeline_reannounce` | `multi_peer_join` state; frame → parse → shard → serialized messages (1000 distinct frames) | message |
| `pipeline_answer` | answer with a 1.3 KB SDP to a pseudo-random known peer, full pipeline | message |
| `*_serde_json`, `*_sonic` | Rust protocol scenarios per parser backend; compared with the JS twin | |

Protocol frames are built identically on both sides (ASCII), so the parsed / sent byte counts
must match ("Same messages" includes bytes). Caveats: core scenarios exclude JSON on both
sides. JS benefits from cached string hashes on pre-made ids in core scenarios; in production
every message brings fresh strings. Rust heap figures include `Vec` growth slack; on macOS, RSS
overstates because freed pages are retained.

## 11. Performance results

<!-- perf-tables:begin -->
- Generated by `bench/run.sh` on 2026-10-01. Do not edit by hand.
- Rust: {"arch":"aarch64","os":"macos","parallelism":8}
- JS: {"node":"v26.3.0","cpu":"Apple M1","parallelism":8}
- Median of 5 runs after 2 warmups. 10 offers per announce, numwant 10. Core scenarios count messages without serializing; `proto_*` / `pipeline_*` include JSON parsing and serialization (1.3 KB SDP per offer, ~14 KB per announce frame) and compare output bytes.
- Rust scenarios without a suffix use the default `RandomSample`; `_window` / `_round_robin` use the other strategies (JS always uses a random window). `_serde_json` / `_sonic` are the two `wt-proto` parser backends; JS uses `StringDecoder` + `JSON.parse` / `JSON.stringify` like uws-tracker.

### Time per operation

| Scenario | Ops/run | JS ns/op | Rust ns/op | Speedup | Same messages |
|---|---:|---:|---:|---:|:-:|
| join_one_swarm | 100,000 | 460.9 | 137.1 | 3.4× | yes |
| join_many_swarms | 1,000,000 | 773.2 | 220.7 | 3.5× | yes |
| multi_peer_join | 600,000 | 852.0 | 164.6 | 5.2× | yes |
| reannounce | 600,000 | 886.6 | 142.4 | 6.2× | yes |
| answer | 1,000,000 | 318.3 | 76.9 | 4.1× | yes |
| reannounce_one_swarm | 100,000 | 334.9 | 89.6 | 3.7× | yes |
| reannounce_window (vs JS reannounce) | 600,000 | 886.6 | 104.5 | 8.5× | n/a |
| reannounce_one_swarm_window (vs JS reannounce_one_swarm) | 100,000 | 334.9 | 51.0 | 6.6× | n/a |
| reannounce_round_robin (vs JS reannounce) | 600,000 | 886.6 | 90.9 | 9.8× | n/a |
| reannounce_one_swarm_round_robin (vs JS reannounce_one_swarm) | 100,000 | 334.9 | 48.2 | 7.0× | n/a |
| stop_all | 600,000 | 244.6 | 93.9 | 2.6× | yes |
| disconnect_all | 100,000 | 844.4 | 247.9 | 3.4× | yes |
| expire_sweep | 600,000 | 104.6 | 26.6 | 3.9× | yes |
| proto_parse_announce_serde_json (vs JS proto_parse_announce) | 20,000 | 16628.6 | 6913.0 | 2.4× | yes |
| proto_parse_announce_sonic (vs JS proto_parse_announce) | 20,000 | 16628.6 | 36589.8 | 0.5× | yes |
| proto_encode_announce | 20,000 | 7595.2 | 1188.8 | 6.4× | yes |
| pipeline_reannounce_serde_json (vs JS pipeline_reannounce) | 20,000 | 26886.7 | 8354.3 | 3.2× | yes |
| pipeline_reannounce_sonic (vs JS pipeline_reannounce) | 20,000 | 26886.7 | 37850.6 | 0.7× | yes |
| pipeline_answer_serde_json (vs JS pipeline_answer) | 20,000 | 3110.5 | 884.1 | 3.5× | yes |
| pipeline_answer_sonic (vs JS pipeline_answer) | 20,000 | 3110.5 | 1214.5 | 2.6× | yes |

### Memory per membership (peer-in-swarm)

| State | Memberships | JS heapUsed B | Rust heap B (incl. Vec slack) | Rust RSS B |
|---|---:|---:|---:|---:|
| multi_peer_join | 600,000 | 156.7 | 152.5 | 93.5 |
| join_many_swarms | 1,000,000 | 233.6 | 233.9 | 282.2 |

### Multi-core scaling (re-announce, M announces/s)

Strong: one 600k-membership state split across N shards by `swarm % N`. Weak: every shard holds a full copy.

| Threads | JS strong | Rust strong | Rust/JS | JS weak | Rust weak |
|---:|---:|---:|---:|---:|---:|
| 1 | 1.77 (1.0×) | 6.56 (1.0×) | 3.7× | 1.69 (1.0×) | 6.34 (1.0×) |
| 2 | 1.94 (1.1×) | 9.28 (1.4×) | 4.8× | 2.36 (1.4×) | 9.00 (1.4×) |
| 4 | 3.50 (2.0×) | 17.23 (2.6×) | 4.9× | 3.50 (2.1×) | 13.86 (2.2×) |
| 8 | 4.41 (2.5×) | 34.57 (5.3×) | 7.8× | 4.61 (2.7×) | 19.05 (3.0×) |
<!-- perf-tables:end -->

## 12. Open items

- **Memory:** about 150 B per membership, the same as JS. Shrinking the key to 24 bytes
  saved only 12%. Remaining costs are the per-connection map and slab/`Vec` growth slack.
- **Parsing speed (protocol, next):** parsing is ~80% of the Rust pipeline (§11). Next step: a
  custom structural scanner (SIMD search for `"` / `\` plus control-character check, `memchr`
  or `std::simd`) behind the `Backend` trait.
- `sonic-rs` backend: ~5× slower than `serde_json` on aarch64 (M1; Ampere A1 is also NEON) for
  these string-heavy frames. Re-measure on x86 (AVX2); remove the backend if it stays slower.
- Optional: skip the UTF-8 check when the WebSocket layer already validated the text frame
  (costs ~0.5 µs per 14 KB).
- Server: per-core runtimes, `SO_REUSEPORT`, cross-shard queues; choose the WebSocket stack
  (fastwebsockets vs sockudo-ws) with a tracker-shaped load test.
- Hardening: check the requesting connection on stop; optionally require the answer target to
  share the swarm.
