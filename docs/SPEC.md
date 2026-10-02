# wt-tracker-rust — specification

Rust port of [wt-tracker](../../wt-tracker) (Node.js + uWebSockets.js WebTorrent tracker).
This file describes the **implemented** behaviour. Planned work is listed only in §12.

| Area | Status |
|---|---|
| Tracker math (`crates/wt-core`) | implemented |
| Benchmarks vs JS (`crates/wt-bench`, `bench/`) | implemented |
| Protocol layer (`crates/wt-proto`, JSON ↔ `Request`) | implemented |
| Server (`crates/wt-server`: WebSocket, TLS, sharding across cores) | implemented (prototype) |
| Load generator (`crates/wt-loadgen`, `loadtest/run.sh`) | implemented |

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
- **Sharding:** the server runs N shards (one per worker), routing each request by
  `hash(info_hash) % N` (§13.3). Every request carries `info_hash`, so answers route without a
  global peer table.

| Crate / dir | Purpose |
|---|---|
| `crates/wt-core` | `Shard`, `Key`, `Request`, `Outbox`, `Settings` |
| `crates/wt-proto` | wire protocol: `handle`, parser backends, `Encoder` (§7) |
| `crates/wt-server` | the server, binary `wt-tracker` (§13); `src/ws/` own WebSocket framing (`codec`) and connection driver (`driver`); `echo` + `examples/ws-echo.rs` echo server for conformance testing |
| `crates/wt-loadgen`, `loadtest/run.sh` | load generator, wire smoke check, JS vs Rust load test (§14) |
| `loadtest/autobahn.sh` | Autobahn testsuite (docker) against `ws-echo`, ws and wss (§9) |
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
- `handle_with::<B>` = `B::parse` + `apply(shard, now, conn, &Message, out)`. `Message::route_info_hash()`
  gives the `info_hash` that picks the shard (`None` for scrapes, unmatchable stops, answers
  without a usable string `info_hash`).
- `OwnedMessage::new(frame: Bytes, &message)` makes a parsed message `Send` without copying (the
  frame plus offsets); `.message()` gives it back.
- `Encoder`: one byte buffer plus a message index. `messages()` yields `(ConnId, &[u8])` in emission
  order, `removed()` the `peer_removed` events, `bytes()` the total size; `clear()` keeps capacity;
  `take()` moves all messages out as a `Batch` (one `Bytes` buffer, each message a slice of it).
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
| permessage-deflate (`compression` > 0) | negotiated by uWebSockets | not negotiated (warning at startup) | not implemented in the own framing |
| Answer without a string `info_hash`, several workers | single tracker: delivered; multi-worker: error | 1 worker: delivered; > 1: `BadField("info_hash")` → close | cannot be routed to a shard |
| Answer target only in a swarm of another shard | delivered (one global peer table) | `UnknownPeer` → close | per-shard peer tables; real answers target a member of the same swarm |
| `/stats.json` `memory` | `process.memoryUsage()` | `{ "rss": bytes }`; extra `workers`, `droppedMessages` | |
| Slow receivers | uWS buffers up to its backpressure limit | messages beyond `maxBackpressure` (1 MiB) per connection are dropped | bounded memory |
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

- `crates/wt-server/tests/server.rs` (in-process server, real sockets): offers and answers across
  workers and shards (byte-exact), scrape merged across shards in request order, disconnect
  cleanup across shards, bad / oversized / invalid-UTF-8 frames close and remove peers, binary,
  fragmented and pipelined (same packet as the handshake) frames, the multi-shard answer rule,
  idle timeout with pings, HTTP routes and ws path, origin rules, `maxConnections`, wss with a
  generated certificate, backpressure drops; `ConnId` packing unit test.
- WebSocket framing: `ws/codec.rs` unit tests (RFC 6455 example frame, every length boundary at
  every split point, protocol errors and close codes, fragment reassembly and misuse, unmasking)
  and a property test with tungstenite as the oracle: random messages (incl. the 125/126 and
  65535/65536 boundaries) as masked, randomly fragmented frames with pings in between, fed in
  random chunks, must decode to the same messages. `tests/native.rs`: a frame written one byte
  at a time, 200 frames in one write, a 60 KB message over TLS records, the client's Finished and
  HTTP request in one TLS flight.
- **Autobahn testsuite** (`loadtest/autobahn.sh`, docker image `crossbario/autobahn-testsuite`,
  fuzzing client against the `ws-echo` example over ws and wss; compression cases 12.* / 13.*
  excluded): no case may be FAILED; reports in `target/autobahn/reports`. Last result (M1,
  2026-10-02): 301 cases each over ws and wss — 287 OK, 11 NON-STRICT (3.2, 3.3, 4.1.3, 4.1.4,
  4.2.3, 4.2.4, 5.15, 6.4.1–6.4.4: invalid UTF-8 detected when a fragmented message completes,
  not at the first bad fragment; a ping queued before an invalid frame is still answered),
  3 INFORMATIONAL, 0 FAILED. `wt-proto/tests/owned.rs`:
  `OwnedMessage` round trip (also across threads) and `Encoder::take`.

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
- Generated by `bench/run.sh` on 2026-10-02. Do not edit by hand.
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

### Load test (end to end, `loadtest/run.sh`)

- {"cpu":"Apple M1","cores":8,"os":"darwin 27.0.0","node":"v26.3.0"}; client and server on the same machine.
- 100 swarms, 10 offers per announce (1.3 KB SDP), every offer answered, 15 s steady phase. JS with `compression: 0` (Rust does not negotiate permessage-deflate). `js-workers` = JS multi-worker tracker, `rust-1` / `rust-n` = 1 / all-core workers.
- Wire smoke check (same messages from JS and Rust): **yes**.

| Profile / target | Conns (connected) | Announce every | Errors | Msgs/s (in + out) | Server CPU (cores) | CPU µs / msg | RSS MiB | RSS KiB / conn | RTT p50 / p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| light js ws | 3000 (3000) | 5 s | 0 | 19,211 | 0.37 | 19.0 | 84.0 | 28.7 | 0.48 / 1.97 |
| light js wss | 3000 (3000) | 5 s | 0 | 19,207 | 0.38 | 19.7 | 34.7 | 11.8 | 0.54 / 1.98 |
| light js-workers ws | 3000 (2983) | 5 s | 17 | 19,098 | 0.76 | 39.8 | 181.9 | 62.4 | 0.49 / 1.74 |
| light js-workers wss | 3000 (2954) | 5 s | 46 | 18,912 | 0.78 | 41.5 | 199.3 | 69.1 | 0.51 / 1.83 |
| light rust-1 ws | 3000 (3000) | 5 s | 0 | 19,214 | 0.24 | 12.6 | 14.3 | 4.9 | 0.26 / 1.59 |
| light rust-1 wss | 3000 (3000) | 5 s | 0 | 19,217 | 0.29 | 15.2 | 23.9 | 8.2 | 0.29 / 1.67 |
| light rust-n ws | 3000 (3000) | 5 s | 0 | 19,213 | 0.40 | 21.0 | 19.1 | 6.5 | 0.39 / 1.97 |
| light rust-n wss | 3000 (3000) | 5 s | 0 | 19,206 | 0.46 | 23.9 | 27.0 | 9.2 | 0.42 / 1.74 |
| heavy js ws | 4000 (4000) | 1 s | 0 | 128,063 | 0.68 | 5.3 | 71.3 | 18.3 | 0.49 / 1.46 |
| heavy js-workers ws | 4000 (3951) | 1 s | 49 | 126,376 | 1.22 | 9.7 | 305.6 | 79.2 | 264.70 / 545.79 |
| heavy rust-1 ws | 4000 (4000) | 1 s | 0 | 128,054 | 0.51 | 3.9 | 25.1 | 6.4 | 0.31 / 1.64 |
| heavy rust-n ws | 4000 (4000) | 1 s | 0 | 128,051 | 1.33 | 10.4 | 26.7 | 6.8 | 0.46 / 1.95 |
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
- **Multi-core efficiency (next):** with peers of a swarm on different workers, most offers and
  answers cross workers; at 128k msgs/s `rust-n` uses ~2.7× the CPU of `rust-1` (§11). Plan:
  move a connection to the worker that owns its swarm after its first announce (clients
  usually use one swarm per connection), so traffic stays on one core; batch cross-worker
  flushes.
- **WebSocket library comparison (M1, done):** fastwebsockets and sockudo-ws both kept
  per-connection buffers sized to the largest frame: 63–92 KiB (fastwebsockets) and 113–193 KiB
  (sockudo-ws: 64 KiB read + 16 KiB write buffer per connection) per connection vs JS 18–36 KiB,
  at the same CPU per message (3.9–4.4 µs at 128k msgs/s on one worker); sockudo-ws also reset
  ~1.8% of connections during the ramp. The own framing with shared per-worker buffers (§13.2)
  brought memory to 15–22 KiB per connection and CPU per message to ≤ fastwebsockets, passes
  the same suites and Autobahn, and replaced both.
- **Moving connections between workers (research):** to match the info_hashes a connection is
  active in (quality switches change them); connection state is already owned and `Send`.
  Needs a design for connection ids, in-flight messages and placement (group-id URL parameter
  vs dynamic placement).
- A single busy worker accepts new connections more slowly during a 1000/s ramp; on macOS
  (listen backlog 128) a few connects were refused in one manual run. Consider prioritising
  the accept loop or `reusePort` on Linux.
- TLS / large tests on Linux and Ampere A1 with a separate client machine; `reusePort`.
- Hardening: check the requesting connection on stop; optionally require the answer target to
  share the swarm.

## 13. Server (`crates/wt-server`, binary `wt-tracker`)

`wt-tracker [config.json]` reads the given file, else `./config.json` if it exists, else uses
defaults (like the JS tracker). `wt_server::start(Config) -> Server` runs it in-process
(tests); dropping the `Server` stops it.

### 13.1 Configuration (JS format)

| Field | Default | Notes |
|---|---|---|
| `servers[].server.host` / `.port` | `0.0.0.0` / `8000` | one listener per item; port 0 = any (`Server::local_addrs`) |
| `servers[].server.key_file_name` + `cert_file_name` | — | PEM; both set → wss:// (rustls, ring, TLS 1.2 + 1.3, ALPN `http/1.1`) |
| `servers[].server.passphrase`, `dh_params_file_name`, `ca_file_name`, `ssl_ciphers`, `ssl_prefer_low_memory_usage` | — | accepted, **ignored with a startup warning** |
| `servers[].websockets.path` | `/*` | `/*` any path, `/a/*` prefix, else exact (query ignored) |
| `servers[].websockets.maxPayloadLength` | 65536 | larger message → close |
| `servers[].websockets.idleTimeout` | 240 s | no frame received for this long → close; pings every `idleTimeout / 2`; 0 = off |
| `servers[].websockets.compression` | 0 | accepted; > 0 → warning, permessage-deflate is never negotiated |
| `servers[].websockets.maxConnections` | 0 (off) | upgrade denied (TCP close) when open WebSockets `> maxConnections` (same off-by-one as JS) |
| `tracker.maxOffers` / `announceInterval` | 20 / 20 | §6; expiry runs every `announceInterval` |
| `tracker.offerSelection` | `sample` | `sample` / `window` / `round_robin` (§5.2) |
| `websocketsAccess.allowOrigins` / `denyOrigins` / `denyEmptyOrigin` | — | both lists set → config error; denied → TCP close |
| `workers` (new) | available parallelism | 1–64; one shard each |
| `reusePort` (new) | false | Linux only: one `SO_REUSEPORT` socket per worker; otherwise one shared socket |
| `maxBackpressure` (new) | 1 MiB | per-connection queued bytes; further messages to it are dropped (`droppedMessages`) |
| `indexHtml` (new) | `./index.html` if present | served at `GET /` |

Unknown fields are ignored. Invalid config (wrong types, both origin lists, half a key pair,
`workers` out of range, unknown `offerSelection`) → error at startup.

### 13.2 Connections and HTTP

- TCP (`TCP_NODELAY`) → optional TLS handshake → one HTTP/1.1 request head (≤ 8 KiB; TLS +
  head within 10 s).
- `GET` with `Upgrade: websocket` on a matching path → `maxConnections` and origin checks (fail
  → TCP close) → `101` with `Sec-WebSocket-Accept` (requested `Sec-WebSocket-Protocol` echoed,
  like uws-tracker; no extensions). Bytes sent right after the head are kept.
- Otherwise: `GET /` → `index.html` (200) or `404 Not Found`; `GET /stats.json` (§13.5); anything
  else → `404 Not Found`. HTTP responses close the connection.
- WebSocket (`src/ws`): own RFC 6455 server framing, one task per connection, driven by socket
  readiness (`ready` + `try_read` / `try_write_vectored`):
  - **Shared buffers per worker thread:** every connection reads into one buffer of its worker
    (≥ 256 KiB, grown to `maxPayloadLength` + 14), where frames are parsed and unmasked in place;
    a connection keeps bytes only for an incomplete frame (freed when it completes) and a
    reassembly buffer only while a fragmented message is in progress. An idle connection holds
    no buffers.
  - **TLS** (wss) uses rustls' unbuffered API on the same shared buffers (TLS ciphertext in, a
    shared plaintext / ciphertext scratch out); a connection keeps an incomplete TLS record and
    unsent ciphertext only while they exist. The HTTP request head is read through it.
  - **Writes:** queued messages are sent with one vectored write per wake-up (frame headers +
    the shared encoder slices, no copy); TLS encrypts up to 64 KiB of frames per batch.
  - Text and binary messages are both parsed (§7); fragmented messages are reassembled.
  - **Rules / close codes:** RSV bits, unmasked client frames, unknown opcodes, fragmented or
    > 125-byte control frames, stray continuations → 1002; message > `maxPayloadLength` → 1009;
    invalid UTF-8 in a text message or close reason → 1007; a message rejected by the tracker
    (§7.3) → 1008; a received close frame is answered with its code (1000 without one); idle
    timeout (no frame for `idleTimeout`, pings every `idleTimeout / 2`) or a close from the server
    side → 1000. Pings are answered with pongs, in order, before queued data. The close frame is
    written within 1 s, then TLS `close_notify` and TCP shutdown; the connection's peers are
    removed (§13.3).
  - The local shard path parses straight from the shared buffer and applies the message without
    any copy; a message for another worker is copied once (`OwnedMessage::copy_from`).
  - All per-connection state is owned and `Send` (needed to move connections between workers
    later, §12).
- Earlier transports (fastwebsockets, sockudo-ws) were removed after the comparison in §12.

### 13.3 Workers and sharding

- N workers: a thread each, with a current-thread tokio runtime, its own `Shard` (seeded per
  worker), its accepted connections and an inbox channel. Every worker accepts on every
  listener.
- `ConnId` = worker (8 bits) | generation (24 bits) | slot (32 bits); messages for a closed
  connection or a reused slot are dropped.
- A frame is parsed once by the worker that owns the connection, then routed:
  - announce / stop / answer / scrape of one hash → shard `foldhash(info_hash) % N` (seed shared
    by all workers); local shard → applied directly; otherwise sent as an `OwnedMessage` (frame
    `Bytes` + offsets, no copy, no re-parse);
  - a stop whose ids cannot match → nothing; an answer without a usable `info_hash` → local shard
    if N = 1, else `BadField("info_hash")`;
  - scrape of all / several hashes → gathered (§13.4).
- The owning shard's output is encoded once per batch (`Encoder::take`): each message is a slice
  of one buffer, delivered to its connection's queue (local) or batched per destination worker
  (one channel send per destination per scheduler tick).
- A rejected message on a remote shard closes the connection on its own worker (`Close` event).
- Each connection remembers which shards it announced to; on close every one of them gets a
  disconnect, through the same FIFO as its requests.

### 13.4 Scrape and stats across shards

Scrape of all swarms or of several hashes is scattered to the shards owning them and gathered;
entries are encoded in request order with the first occurrence kept (§7.2), all swarms in shard
order. `/stats.json` gathers `(info_hash, peers)` of every shard.

### 13.5 `/stats.json`

`{"torrentsCount", "peersCount", "servers":[{"server":"host:port","webSocketsCount"}],
"memory":{"rss"}, "workers", "droppedMessages", "peersCountPerInfoHashPerTracker":[{"totalPeers",
"<hex info_hash>": peers, …} per shard]}`. The hex is computed like JS `Buffer.from(infoHash,
"binary").toString("hex")` (one byte per character).

## 14. Load test (`crates/wt-loadgen`, `loadtest/run.sh`)

- `wt-loadgen load`: N clients (tokio multi-thread, tokio-tungstenite, rustls with `--ca`), one
  connection and one peer each, in one of `--swarms` swarms. Each announces `started` with
  `--offers` offers (SDP from `bench/fixtures/offer.sdp`), re-announces every `--interval` s
  (spread over the interval), and answers every offer it receives. Connects are paced evenly at
  `--ramp` per second (bursts overflow small listen backlogs, e.g. macOS `somaxconn` = 128).
  After the ramp, a `--duration` s steady phase measures messages/s, announce → reply RTT
  (HdrHistogram), and the server's CPU seconds and RSS (`/proc` or `ps`, `--server-pid`).
  Connection failures and early closes are reported by reason (error text, or the server's close
  code).
- `wt-loadgen smoke`: a deterministic script over 3 clients (announces with full fan-out, an
  answer, a second swarm, scrapes, a stop, an invalid frame); the received messages per client,
  sorted.
- `wt-loadgen gen-cert DIR`: self-signed `cert.pem` / `key.pem` for `localhost` (rcgen).
- `loadtest/run.sh`: for the profiles light (`LIGHT_CONNS`=3000, re-announce every 5 s, ws and
  wss) and heavy (`HEAVY_CONNS`=4000, every 1 s, ws), runs the JS tracker (`run-tracker.ts`), the
  JS multi-worker tracker (`run-worker-tracker.ts`), Rust with 1 worker and with all cores, each in a fresh process with the same config (`compression: 0`,
  `announceInterval: 120`); then the smoke script against JS and Rust, compared exactly. Writes
  `bench/results/load.json` and regenerates the load table of §11.
