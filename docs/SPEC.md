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
- **Sharding:** the server runs N shards (one per worker). Each info_hash belongs to one shard,
  found in a global directory (`content` placement: the swarms of one piece of content share a
  shard and connections move to it) or by `hash(info_hash) % N` (`hash` placement) (§13.3).
  Every request carries `info_hash`, so answers route without a global peer table.

| Crate / dir | Purpose |
|---|---|
| `crates/wt-core` | `Shard`, `Key`, `Request`, `Outbox`, `Settings` |
| `crates/wt-proto` | wire protocol: `handle`, parser backends, `Encoder` (§7) |
| `crates/wt-server` | the server, binary `wt-tracker` (§13); `src/ws/` own WebSocket framing (`codec`), permessage-deflate (`deflate`) and connection driver (`driver`); `placement` info_hash directory, worker loads and placement choices (§13.3); `echo` + `examples/ws-echo.rs` echo server for conformance testing |
| `crates/wt-loadgen`, `loadtest/run.sh` | load generator (tokio-tungstenite, or its own client `src/client.rs` for permessage-deflate and wire bytes), wire smoke check, JS vs Rust load test (§14) |
| `loadtest/autobahn.sh` | Autobahn testsuite (docker) against `ws-echo`, ws and wss, incl. compression (§9) |
| `loadtest/aquatic.sh`, `loadtest/aquatic/` | load test against aquatic_ws in a Linux container (§14) |
| `.github/workflows/ci.yml` | CI: the finish checklist and short fuzzing on pull requests and `main`, Autobahn on `main`, nightly long fuzzing (§9) |
| `fuzz/` | cargo-fuzz targets (separate workspace, nightly); target logic in `wt-server::fuzz` / `wt-proto::fuzz` (feature `fuzzing`); seed corpus from `fuzz/make-seeds.py` (§9) |
| `README.md`, `CHANGELOG.md`, `SECURITY.md`, `LICENSE`, `NOTICE` | overview, changes, vulnerability reporting, Apache-2.0 |
| `rust-toolchain.toml` | Rust 1.98.1 (also `rust-version = "1.98"` in `Cargo.toml`) |
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
  Integers via `itoa`. `counters()`: messages and JSON bytes produced since the encoder was
  created, per kind (`announce_replies`, `offers`, `answers`, `scrapes`; `Count { messages,
  bytes }`), not reset by `clear()` / `take()`.

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
| permessage-deflate, `compression: 1` (both defaults) | `permessage-deflate; client_no_context_takeover; server_no_context_takeover`, client messages inflated, replies never compressed (`send(msg, false, false)`) | the same (header compared by the smoke check) | parity |
| `compression` > 1 (dedicated compressor) | per-connection compressor | treated as 1 (warning at startup) | no per-connection zlib state |
| Offer with `server_max_window_bits=N` | accepted without echoing it (RFC 7692 §7.1.2.1 requires the echo) | `; server_max_window_bits=N` echoed | RFC |
| `x-webkit-deflate-frame` (old Safari) | negotiated | not negotiated | obsolete |
| Compressed outgoing messages | never | messages ≥ 1 KiB (offers) when negotiated; `compressOutgoingMinSize: 0` = never | ~25% less egress at no measurable CPU on production traffic (§12) |
| Inflated message > `maxPayloadLength` / corrupt | connection closed | close 1009 / 1007 | |
| Answer without a string `info_hash`, several workers | single tracker: delivered; multi-worker: error | 1 worker: delivered; > 1: `BadField("info_hash")` → close | cannot be routed to a shard |
| Answer target only in a swarm of another shard | delivered (one global peer table) | `UnknownPeer` → close | per-shard peer tables; real answers target a member of the same swarm |
| `/stats.json` `memory` | `process.memoryUsage()` | `{ "rss": bytes }`; extra `workers`, `droppedMessages`, `placement` | |
| Slow receivers | uWS buffers up to its backpressure limit | messages beyond `maxBackpressure` (1 MiB) per connection are dropped | bounded memory |
| Peer identity | global per tracker (per worker in multi-worker) | per shard | sharding |
| Stop from another connection | allowed | allowed (parity) | hardening is planned (§12) |
| SIGTERM / SIGINT | process exits at once, connections dropped | graceful shutdown: close 1001, up to `shutdownTimeout` (§13.6) | deployments (docker stop, systemd) |
| Answer target not in the same swarm | allowed | allowed (parity) | hardening is planned (§12) |

## 9. Testing requirements

- `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `scripts/check-spec.sh` must pass. CI
  (`.github/workflows/ci.yml`, Ubuntu, toolchain from `rust-toolchain.toml`) runs them on every
  pull request and push to `main`: job `check` (fmt, clippy, tests), job `spec`
  (`check-spec.sh` against the pull request's base or the previous `main`; label
  `spec-unchanged-ok` sets `SPEC_UNCHANGED_OK=1`), job `difftest` (`node difftest/run.ts`, Node
  26, against `Novage/wt-tracker` checked out at `WT_TRACKER_REF` next to this repository, after
  `npm ci`), job `fuzz` (below), and on pushes to `main`, nightly and manual runs job `autobahn`
  (`loadtest/autobahn.sh`, reports uploaded as an artifact). Nightly (03:00 UTC, schedule) runs
  every job. Benchmarks and load tests are not run in CI (they need an idle machine).
- **Fuzzing** (`fuzz/`, cargo-fuzz / libFuzzer, nightly Rust): every target must not panic for
  any input, and checks invariants that hold for every input:
  - `ws_frames`: a client byte stream (deflate on / off, payload limit and read chunk size from
    the first byte) through `parse_frame`, `Fragments` and `inflate`, fed in chunks like the
    driver: consumed lengths and payload ranges within the buffer, frames and messages ≤ the
    limit, control frames final and ≤ 125 bytes, `Incomplete` totals beyond the buffer, RSV1
    only with deflate, inflated ≤ the limit;
  - `deflate_roundtrip`: `deflate` then `inflate` returns the message, every window size;
  - `http_upgrade`: `parse_head`, `upgrade_response` with and without compression,
    `path_matches`, `negotiate`: a well-formed `101` (every header line `Name: value`, no CR /
    LF in values, so echoed request values cannot inject headers), the extension header only
    when negotiated, outgoing windows 9..15 or none;
  - `protocol`: frames (`0xFF`-separated, connection and kind from the first byte) parsed and
    applied to a shard with disconnects and expiry, both directly and through
    `OwnedMessage::copy_from` on a second shard with the same seed: `check_invariants` after
    every step, identical output on both paths, every outgoing message valid JSON for a known
    connection.

  CI job `fuzz`: each target 60 s on pull requests and pushes, 20 min nightly (`-timeout=10`,
  `-rss_limit_mb=4096`), the corpus found so far restored from the Actions cache, crash inputs
  uploaded as artifacts. Seeds (`fuzz/corpus/<target>/seed-*`, committed, written by
  `fuzz/make-seeds.py`) cover every frame kind, compressed and fragmented messages, an upgrade
  with all handled headers, and a swarm with offers, answer, scrape, stop, disconnect and expiry.
  `cargo test` also runs every target on stable over the seeds plus 2000 mutations of them
  (`wt-server` and `wt-proto` unit tests). Last local run (M1, 2026-10-02, nightly 1.101,
  cargo-fuzz 0.13.2): 60 s per target, 1.19 M inputs in all, no crash, timeout or failed check;
  coverage (edges) `ws_frames` 774, `deflate_roundtrip` 684, `http_upgrade` 932, `protocol`
  2527, still growing when the time ran out.
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
  generated certificate, backpressure drops; `ConnId` packing unit test. Tests that need swarms
  spread over shards use `placement: "hash"`.
- `tests/placement.rs` (4 workers, `content` placement, shards read from `/stats.json`): one
  swarm on one shard with every request local; video + audio of a connection on one shard with
  an offer / answer; a quality switch (new hash, stop of the old) on the same shard; a connection
  whose first message is a scrape is not moved and forwards; a ping before and messages after
  the first announce in the same packet move with the connection (pong and replies in order);
  moving wss connections keep their TLS session (several messages per TLS write, 57 KB offers
  afterwards); 50 concurrent first announces of new content bind one shard; an emptied swarm's
  binding is released at the expiry tick and can bind again. Moves are forced by a crowd of
  pinned idle connections (new content avoids the crowded worker), so they happen however the
  OS spreads accepts. `placement.rs` unit tests: claim / release semantics, 8 threads claiming
  1000 hashes concurrently (one owner each), the placement choices, busy averaging.
- WebSocket framing: `ws/codec.rs` unit tests (RFC 6455 example frame, every length boundary at
  every split point, protocol errors and close codes, fragment reassembly and misuse, unmasking)
  and a property test with tungstenite as the oracle: random messages (incl. the 125/126 and
  65535/65536 boundaries) as masked, randomly fragmented frames with pings in between, fed in
  random chunks, must decode to the same messages. `tests/native.rs`: a frame written one byte
  at a time, 200 frames in one write, a 60 KB message over TLS records, the client's Finished and
  HTTP request in one TLS flight, and session resumption: after one full handshake a client
  resumes on every reconnect, even after 300 other clients' full handshakes (more than rustls'
  default 256-entry session cache holds, which alone would force a full handshake again).
- **Autobahn testsuite** (`loadtest/autobahn.sh`, docker image `crossbario/autobahn-testsuite`,
  fuzzing client against the `ws-echo` example over ws and wss, including the permessage-deflate
  cases 12.* / 13.*; the echo server negotiates compression and compresses every reply): no case
  may be FAILED. The cases run per server in groups (1–7, 9–10, 12, 13); a group whose run lost a
  connection through Docker Desktop's `host.docker.internal` (wstest then skips the rest of the
  run) is run again, up to 3 attempts. Reports in `target/autobahn/reports-<server>-<group>`,
  merged into `target/autobahn/merged.json`. Last result (M1, 2026-10-02):
  517 cases each over ws and wss — 503 OK, 11 NON-STRICT (3.2, 3.3, 4.1.3, 4.1.4, 4.2.3, 4.2.4,
  5.15, 6.4.1–6.4.4: invalid UTF-8 detected when a fragmented message completes, not at the
  first bad fragment; a ping queued before an invalid frame is still answered), 3 INFORMATIONAL,
  0 FAILED.
- permessage-deflate: `ws/deflate.rs` unit tests (negotiation table: Chrome / Firefox offers,
  `server_max_window_bits` echo, 8 → no outgoing compression, unknown / repeated / bad
  parameters declined, the next offer tried, x-webkit declined; RFC 7692 §7.2.3 example frames;
  inflate limit → 1009, corrupt → 1007, the inflater reset after errors; deflate round trip for
  every window size, checked with miniz_oxide) and a property test: random messages compressed
  by miniz_oxide (final-block streams, every level) inflate to the original and fail above the
  limit. `codec.rs`: RSV1 only when negotiated and only on a first data frame (not on
  continuation or control frames; RSV2 / RSV3 never). `tests/compression.rs` (blocking raw
  client, plain and rustls): the negotiated header, compressed single and fragmented announces,
  uncompressed replies by default, no extension without an offer or for an unacceptable one;
  `compression: 0` → RSV1 frame → 1002; inflated > `maxPayloadLength` → 1009, corrupt → 1007,
  invalid UTF-8 after inflating → 1007; `compressOutgoingMinSize` compresses only messages that
  long and only for clients that negotiated; a 40 KB compressed message in 4 fragments over TLS;
  connections that move at their first compressed announce keep compression; with the default
  (1024) an offer ≥ 1 KiB arrives compressed while short replies do not, and `/stats.json`
  `traffic` counts the received and sent messages and bytes, the deflated and inflated bytes and
  socket bytes (a connection moved at its first message is counted once).
  `wt-proto/tests/encode.rs`: `Encoder::counters` per kind, kept across `clear` / `take`.
- `tests/shutdown.rs`: a graceful shutdown closes every connection with 1001 and returns once
  they are closed, then connects are refused; a zero timeout returns at once with a client that
  never answers; the `wt-tracker` binary (Unix) closes with 1001 and exits 0 on SIGTERM. `wt-proto/tests/owned.rs`:
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
| join_one_swarm | 100,000 | 545.8 | 144.3 | 3.8× | yes |
| join_many_swarms | 1,000,000 | 718.2 | 224.6 | 3.2× | yes |
| multi_peer_join | 600,000 | 779.8 | 166.6 | 4.7× | yes |
| reannounce | 600,000 | 796.8 | 137.3 | 5.8× | yes |
| answer | 1,000,000 | 299.7 | 72.6 | 4.1× | yes |
| reannounce_one_swarm | 100,000 | 273.0 | 103.3 | 2.6× | yes |
| reannounce_window (vs JS reannounce) | 600,000 | 796.8 | 97.1 | 8.2× | n/a |
| reannounce_one_swarm_window (vs JS reannounce_one_swarm) | 100,000 | 273.0 | 49.7 | 5.5× | n/a |
| reannounce_round_robin (vs JS reannounce) | 600,000 | 796.8 | 87.0 | 9.2× | n/a |
| reannounce_one_swarm_round_robin (vs JS reannounce_one_swarm) | 100,000 | 273.0 | 48.1 | 5.7× | n/a |
| stop_all | 600,000 | 224.0 | 87.2 | 2.6× | yes |
| disconnect_all | 100,000 | 801.5 | 233.3 | 3.4× | yes |
| expire_sweep | 600,000 | 99.2 | 25.2 | 3.9× | yes |
| proto_parse_announce_serde_json (vs JS proto_parse_announce) | 20,000 | 16411.7 | 6978.9 | 2.4× | yes |
| proto_parse_announce_sonic (vs JS proto_parse_announce) | 20,000 | 16411.7 | 32381.5 | 0.5× | yes |
| proto_encode_announce | 20,000 | 7411.4 | 1015.1 | 7.3× | yes |
| pipeline_reannounce_serde_json (vs JS pipeline_reannounce) | 20,000 | 26540.0 | 8287.2 | 3.2× | yes |
| pipeline_reannounce_sonic (vs JS pipeline_reannounce) | 20,000 | 26540.0 | 33791.4 | 0.8× | yes |
| pipeline_answer_serde_json (vs JS pipeline_answer) | 20,000 | 3033.1 | 879.1 | 3.5× | yes |
| pipeline_answer_sonic (vs JS pipeline_answer) | 20,000 | 3033.1 | 1100.0 | 2.8× | yes |

### Memory per membership (peer-in-swarm)

| State | Memberships | JS heapUsed B | Rust heap B (incl. Vec slack) | Rust RSS B |
|---|---:|---:|---:|---:|
| multi_peer_join | 600,000 | 156.7 | 152.5 | 92.4 |
| join_many_swarms | 1,000,000 | 233.6 | 233.9 | 282.2 |

### Multi-core scaling (re-announce, M announces/s)

Strong: one 600k-membership state split across N shards by `swarm % N`. Weak: every shard holds a full copy.

| Threads | JS strong | Rust strong | Rust/JS | JS weak | Rust weak |
|---:|---:|---:|---:|---:|---:|
| 1 | 1.88 (1.0×) | 7.15 (1.0×) | 3.8× | 1.73 (1.0×) | 6.89 (1.0×) |
| 2 | 2.15 (1.1×) | 9.52 (1.3×) | 4.4× | 2.76 (1.6×) | 9.54 (1.4×) |
| 4 | 3.34 (1.8×) | 17.34 (2.4×) | 5.2× | 3.71 (2.1×) | 14.58 (2.1×) |
| 8 | 4.23 (2.3×) | 29.20 (4.1×) | 6.9× | 4.81 (2.8×) | 19.69 (2.9×) |

### Load test (end to end, `loadtest/run.sh`)

- {"cpu":"Apple M1","cores":8,"os":"darwin 27.0.0","node":"v26.3.0"}; client and server on the same machine.
- 100 swarms, 10 offers per announce (1.3 KB SDP), every offer answered, 15 s steady phase. Servers with `compression: 0` except in the deflate profile (the tungstenite client does not offer permessage-deflate anyway). `js-workers` = JS multi-worker tracker, `rust-1` / `rust-n` = 1 / all-core workers, `rust-n-hash` = all-core workers with `placement: "hash"`. Profile media: 2 swarms per connection (video + audio), video quality switch every 10 s among 4. Profile deflate: like light, but clients offer permessage-deflate and compress everything they send; servers with `compression: 1` (Rust compressing outgoing messages ≥ 1 KiB, the default; `rust-n-in`: `compressOutgoingMinSize: 0`, inflating only, like JS; `rust-n-off`: `compression: 0`, the uncompressed baseline). The deflate rows in the table below predate the 1 KiB default: there `rust-1` / `rust-n` inflated only and `rust-n-out` compressed ≥ 1 KiB. Wire = bytes on the client sockets (own client, deflate profile only). Local % = requests applied on the connection's own worker (Rust).
- Wire smoke check (same messages from JS and Rust): **yes**.

| Profile / target | Conns (connected) | Announce every | Errors | Msgs/s (in + out) | Server CPU (cores) | CPU µs / msg | RSS MiB (idle) | RSS KiB / conn (above idle) | RTT p50 / p99 ms | Local % | Wire KiB/s server out / in |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| light js ws | 3000 (3000) | 5 s | 0 | 19,206 | 0.33 | 17.0 | 79.5 (94.2) | 27.1 (0.0) | 0.47 / 2.38 | – | – |
| light js wss | 3000 (3000) | 5 s | 0 | 19,209 | 0.35 | 18.1 | 74.9 (93.2) | 25.6 (0.0) | 0.50 / 1.81 | – | – |
| light js-workers ws | 3000 (2958) | 5 s | 42 | 18,937 | 0.56 | 29.4 | 187.2 (207.4) | 64.8 (0.0) | 0.36 / 1.25 | – | – |
| light js-workers wss | 3000 (2897) | 5 s | 103 | 18,546 | 0.55 | 29.9 | 176.9 (211.0) | 62.5 (0.0) | 0.39 / 2.62 | – | – |
| light rust-1 ws | 3000 (3000) | 5 s | 0 | 19,206 | 0.21 | 10.8 | 16.3 (3.1) | 5.6 (4.5) | 0.23 / 1.27 | 100.0 | – |
| light rust-1 wss | 3000 (3000) | 5 s | 0 | 19,217 | 0.32 | 16.7 | 27.2 (3.1) | 9.3 (8.2) | 0.33 / 1.73 | 100.0 | – |
| light rust-n ws | 3000 (3000) | 5 s | 0 | 19,216 | 0.23 | 12.2 | 17.9 (3.6) | 6.1 (4.9) | 0.25 / 16.80 | 100.0 | – |
| light rust-n wss | 3000 (3000) | 5 s | 0 | 19,213 | 0.27 | 13.9 | 27.7 (3.6) | 9.4 (8.2) | 0.27 / 1.40 | 100.0 | – |
| light rust-n-hash ws | 3000 (3000) | 5 s | 0 | 19,210 | 0.45 | 23.4 | 19.3 (3.6) | 6.6 (5.4) | 0.39 / 1.60 | 11.5 | – |
| light rust-n-hash wss | 3000 (3000) | 5 s | 0 | 19,211 | 0.45 | 23.2 | 37.9 (3.6) | 12.9 (11.7) | 0.40 / 1.55 | 11.1 | – |
| heavy js ws | 4000 (4000) | 1 s | 0 | 128,040 | 0.68 | 5.3 | 74.5 (94.5) | 19.1 (0.0) | 0.51 / 1.48 | – | – |
| heavy js-workers ws | 4000 (3956) | 1 s | 44 | 126,185 | 1.23 | 9.7 | 342.3 (207.6) | 88.6 (34.9) | 280.06 / 737.28 | – | – |
| heavy rust-1 ws | 4000 (4000) | 1 s | 0 | 128,050 | 0.55 | 4.3 | 26.3 (3.1) | 6.7 (5.9) | 0.32 / 1.55 | 100.0 | – |
| heavy rust-n ws | 4000 (4000) | 1 s | 0 | 128,085 | 0.70 | 5.4 | 28.4 (3.6) | 7.3 (6.4) | 0.30 / 1.69 | 100.0 | – |
| heavy rust-n-hash ws | 4000 (4000) | 1 s | 0 | 128,100 | 1.54 | 12.0 | 29.6 (3.6) | 7.6 (6.7) | 0.50 / 2.01 | 12.1 | – |
| media js ws | 3000 (3000) | 5 s | 0 | 46,631 | 0.54 | 11.6 | 100.0 (94.0) | 34.1 (2.0) | 0.67 / 2.15 | – | – |
| media js-workers ws | 3000 (2984) | 5 s | 16 | 46,362 | 0.91 | 19.6 | 289.8 (189.0) | 99.4 (34.6) | 0.63 / 2.04 | – | – |
| media rust-1 ws | 3000 (3000) | 5 s | 0 | 46,630 | 0.42 | 9.1 | 22.7 (3.1) | 7.7 (6.7) | 0.36 / 2.06 | 100.0 | – |
| media rust-n ws | 3000 (3000) | 5 s | 0 | 46,619 | 0.38 | 8.1 | 20.9 (3.6) | 7.1 (5.9) | 0.29 / 2.75 | 100.0 | – |
| media rust-n-hash ws | 3000 (3000) | 5 s | 41 | 43,895 | 0.39 | 8.9 | 70.1 (3.6) | 23.9 (22.7) | 12.25 / 972.80 | 13.7 | – |
| deflate js ws | 3000 (3000) | 5 s | 0 | 19,272 | 0.29 | 14.9 | 22.4 (93.9) | 7.7 (0.0) | 0.62 / 32.38 | – | 18,168 / 4,683 |
| deflate rust-1 ws | 3000 (3000) | 5 s | 0 | 19,208 | 0.25 | 13.2 | 14.8 (3.1) | 5.0 (4.0) | 0.28 / 1.48 | 100.0 | 18,091 / 4,674 |
| deflate rust-n ws | 3000 (3000) | 5 s | 0 | 19,207 | 0.24 | 12.4 | 15.2 (3.6) | 5.2 (4.0) | 0.24 / 1.25 | 100.0 | 18,090 / 4,674 |
| deflate rust-n-off ws | 3000 (3000) | 5 s | 0 | 19,215 | 0.22 | 11.4 | 15.0 (3.6) | 5.1 (3.9) | 0.25 / 1.49 | 100.0 | 18,097 / 17,809 |
| deflate rust-n-out ws | 3000 (3000) | 5 s | 0 | 19,205 | 0.38 | 19.8 | 18.3 (3.6) | 6.3 (5.0) | 0.23 / 1.78 | 100.0 | 10,416 / 4,674 |

### Load test vs aquatic_ws (Linux container, `loadtest/aquatic.sh`)

- {"where":"Docker Desktop Linux VM","cores":8,"kernel":"Linux 7.0.14-linuxkit aarch64","aquatic_rev":"a2ddc4b323c5aaf844ce32b655b0ffc8c4836cde"}; server and load generator in one container, sharing its CPUs. Same profiles and load generator as above.
- `aquatic-1` = 1 socket + 1 swarm worker (2 threads), `aquatic-n` = all cores split ¾ socket / ¼ swarm workers (io_uring, glommio, async-tungstenite, mimalloc); `rust-1` / `rust-n` = 1 / all-core workers (`rust-n` with `reusePort`, `content` placement).

| Profile / target | Conns (connected) | Announce every | Errors | Msgs/s (in + out) | Server CPU (cores) | CPU µs / msg | RSS MiB (idle) | RSS KiB / conn (above idle) | RTT p50 / p99 ms | Local % | Wire KiB/s server out / in |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| light aquatic-1 ws | 3000 (3000) | 5 s | 0 | 25,198 | 0.66 | 26.2 | 266.9 (45.5) | 91.1 (75.6) | 0.69 / 1.68 | – | – |
| light aquatic-1 wss | 3000 (3000) | 5 s | 0 | 25,203 | 0.66 | 26.3 | 293.8 (46.5) | 100.3 (84.4) | 0.70 / 1.73 | – | – |
| light aquatic-n ws | 3000 (3000) | 5 s | 0 | 25,203 | 1.96 | 77.7 | 394.0 (162.1) | 134.5 (79.2) | 0.68 / 3.83 | – | – |
| light aquatic-n wss | 3000 (3000) | 5 s | 0 | 25,200 | 1.96 | 77.9 | 415.2 (161.1) | 141.7 (86.8) | 0.64 / 7.73 | – | – |
| light rust-1 ws | 3000 (3000) | 5 s | 0 | 19,200 | 0.30 | 15.9 | 14.5 (2.8) | 4.9 (4.0) | 0.30 / 1.11 | 100.0 | – |
| light rust-1 wss | 3000 (3000) | 5 s | 0 | 19,201 | 0.36 | 18.9 | 45.3 (3.4) | 15.5 (14.3) | 0.37 / 1.22 | 100.0 | – |
| light rust-n ws | 3000 (3000) | 5 s | 0 | 19,201 | 0.36 | 18.5 | 17.4 (3.1) | 5.9 (4.9) | 0.33 / 1.17 | 100.0 | – |
| light rust-n wss | 3000 (3000) | 5 s | 0 | 19,203 | 0.41 | 21.2 | 34.5 (3.7) | 11.8 (10.5) | 0.35 / 1.26 | 100.0 | – |
| heavy aquatic-1 ws | 4000 (4000) | 1 s | 0 | 110,184 | 1.34 | 12.1 | 443.9 (45.5) | 113.6 (102.0) | 155.78 / 7634.94 | – | – |
| heavy aquatic-n ws | 4000 (4000) | 1 s | 0 | 167,445 | 5.08 | 30.4 | 463.7 (160.1) | 118.7 (77.7) | 0.82 / 24.64 | – | – |
| heavy rust-1 ws | 4000 (4000) | 1 s | 0 | 127,999 | 0.59 | 4.6 | 20.5 (2.9) | 5.2 (4.5) | 0.26 / 1.65 | 100.0 | – |
| heavy rust-n ws | 4000 (4000) | 1 s | 0 | 128,009 | 1.02 | 8.0 | 24.5 (3.1) | 6.3 (5.5) | 0.28 / 0.94 | 100.0 | – |
| media aquatic-1 ws | 3000 (3000) | 5 s | 0 | 60,048 | 0.84 | 14.0 | 281.2 (45.5) | 96.0 (80.5) | 0.62 / 1.65 | – | – |
| media aquatic-n ws | 3000 (3000) | 5 s | 0 | 61,146 | 2.76 | 45.2 | 414.5 (158.1) | 141.5 (87.5) | 0.48 / 7.60 | – | – |
| media rust-1 ws | 3000 (3000) | 5 s | 0 | 46,608 | 0.46 | 10.0 | 18.9 (2.8) | 6.5 (5.5) | 0.32 / 0.90 | 100.0 | – |
| media rust-n ws | 3000 (3000) | 5 s | 0 | 46,615 | 0.64 | 13.8 | 23.4 (3.1) | 8.0 (6.9) | 0.36 / 18.24 | 100.0 | – |
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
- **Multi-core efficiency (done: `content` placement, §13.3):** with hash routing most offers
  and answers crossed workers (`rust-n` ~2.7× the CPU of `rust-1` at 128k msgs/s). Clients
  (p2p-media-loader: one WebSocket per tracker per player, shared by the video and audio streams
  and every quality; the old quality kept ~30 s after a switch) mostly use the swarms of one
  piece of content per connection. Swarms now follow the content and connections move once, at
  their first announce. Load test (M1, 8 workers, machine not idle, §11): requests 100% local in
  every profile (vs 11–17% with `hash`); CPU per message heavy 5.1 µs (`hash` 10.6, one worker
  4.0), media 10.1 µs (`hash` 17.1, one worker 7.8), light ws 12.8 µs (`hash` 23.0); RTT p50
  0.29 vs 0.49 ms (heavy); 0 errors. The remaining gap to one worker is not analysed yet (likely per-worker
  wake-ups with the load spread over 8 threads). An
  early variant also spilled new hashes from a worker above 1.25 × the mean request rate: it
  split contents (~90% local) for no gain while no worker was saturated; spilling now needs a
  saturated worker (§13.3), and new content is balanced by connection count (request rates lag
  behind a ramp).
- **aquatic_ws comparison (done, `loadtest/aquatic.sh`, §11):** in a Linux container (Docker
  Desktop VM on M1, 8 CPUs shared with the load generator), for the same useful traffic (the same
  announces, offers and answers delivered): `rust-n` uses 4.3–5.4× less CPU than `aquatic-n`
  (heavy: 1.0 vs 5.1 cores) and `rust-1` 1.8–2.2× less than `aquatic-1`; `aquatic-1` saturates in
  the heavy profile (74% of offers delivered, RTT p50 156 ms) while `rust-1` carries it on 0.6
  cores. Memory per connection above idle 4–7 KiB (ws) / 10–14 KiB (wss) vs 75–102 KiB
  (async-tungstenite buffers per connection), plus aquatic's fixed 45 / 160 MiB at idle; RTT p50
  0.26–0.37 vs 0.48–0.82 ms. aquatic also replies to every announce that carries an answer (the
  JS tracker and this server send nothing), so it sends ~10× more announce replies; its CPU µs /
  msg includes them. Where aquatic's CPU goes with many workers (every request crosses from a
  socket to a swarm worker and back) is not analysed.
- **permessage-deflate (done, §13.2):** `compression: 1` now negotiates like the JS tracker
  (header compared by the smoke check). Deflate profile (M1, §11; clients compress everything
  they send, like browsers): inflating client messages costs about 1 µs / msg (`rust-n` 12.4 vs
  11.4 µs with `compression: 0`; JS 14.9) and cuts client → server bytes 3.8× (4.7 vs 17.8
  MiB/s); outgoing compression of messages ≥ 1 KiB (`rust-n-out`) cuts server → client bytes by
  42% (10.4 vs 18.1 MiB/s) for +60% CPU (19.8 µs / msg, 0.38 vs 0.24 cores). Only announces are
  large; answers and replies stay under 1 KiB. Autobahn now includes 12.* / 13.*: 517 cases per
  server, 0 FAILED. Open: dedicated (context takeover) compression is not offered; a smaller
  outgoing threshold or a faster level was not measured.
- **Production canary (done, tracker.novage.com.ua, 2026-10-02):** Oracle Cloud Ampere A1,
  2 cores (Neoverse-N1), 11 GiB, Ubuntu 24.04; real p2p-media-loader peers on wss:// port 443
  (Let's Encrypt certificate via systemd `LoadCredential`), `workers: 2`, `reusePort`,
  `maxOffers: 10`, `announceInterval: 180`, `idleTimeout: 190`, `compression: 1`. Sampled every
  minute (`/proc`, `ss`, `/proc/net/dev`, `/stats.json`):
  - Rust vs aquatic_ws (its config corrected for 2 cores: 2 socket + 1 swarm worker), 30 min
    each, at ~42k connections: CPU 0.35 vs 1.46 cores (load 0.3–0.5 vs 1.5–2.2), RSS 590 MiB
    (14 KiB / connection, all tracker state) vs 1,896 MiB (46 KiB); Rust tracked 55.7k peers /
    31.5k torrents, aquatic 51.1k / 28.9k; inbound 2.3–2.6 vs 5.3 MB/s (aquatic does not
    negotiate permessage-deflate, so browsers send uncompressed); aquatic sent ~190 error
    responses / s (cause not logged). Outbound 3.1–3.9 vs 2.8 MB/s: not attributable from
    windows 30 min apart (more peers served, aquatic's rejected announces); `traffic` counters
    added to `/stats.json` for that.
  - Outgoing compression ≥ 1 KiB, back to back at ~45k connections: outbound 3.58 → 2.61 MB/s
    (−27%; −20 to −25% per peer while evening traffic declined), CPU 0.339 → 0.327 cores (no
    measurable cost, unlike the +60% of the local load test; fewer bytes to encrypt is a likely
    reason, not verified), inbound −13% (fewer ACKs). At that rate egress is ~6.8 instead of
    ~9.3 TB / month (Oracle free tier: 10 TB). Made the default (`compressOutgoingMinSize`
    1024).
  - RSS per connection rose from 11.4 to 14.1 KiB during the ramp, then stayed flat (568–570 MiB
    at ~44k connections for 10 min): tracker state and caches, no sign of a leak.
  - Restarting the service under load takes ~4 s (graceful 1001 to all connections). Switching
    between Rust and aquatic took 4–5.5 min each way: orphaned connections of the stopped
    listener (FIN-WAIT, LAST-ACK) keep its socket options, and a listener without
    `SO_REUSEADDR` (aquatic), or one of another user, cannot bind the port until they drain.
- **TLS handshakes in production egress (2026-10-04, noon, ~42k peers):** ~120 new
  connections / s (a connection lasts ~6 min on average), each full handshake sending ~3.7 KB
  (certificate chain): ~0.45 of 1.79 MB/s interface egress (~25%). rustls' default 256-entry
  session cache covered ~2 s of handshakes, so reconnects rarely resumed; stateless session
  tickets (§13.2) now let them resume. Expected: up to ~20% less egress if browsers resume;
  to be measured in production.
- **Placement, open (research):** creation-time balance cannot foresee popularity — a piece of
  content that outgrows one core stays on it (only its new hashes spill); fixing that needs
  moving live swarms or connections. Swarm-creation races in a mass reconnect (after a restart)
  can split a content until its swarms empty. Clients that put unrelated torrents on one socket
  (webtorrent / bittorrent-tracker) still cross workers. Accepts: on macOS all workers share
  one listening socket and one worker may accept most connections (they move afterwards, but
  that worker does every TLS handshake).
- **WebSocket library comparison (M1, done):** fastwebsockets and sockudo-ws both kept
  per-connection buffers sized to the largest frame: 63–92 KiB (fastwebsockets) and 113–193 KiB
  (sockudo-ws: 64 KiB read + 16 KiB write buffer per connection) per connection vs JS 18–36 KiB,
  at the same CPU per message (3.9–4.4 µs at 128k msgs/s on one worker); sockudo-ws also reset
  ~1.8% of connections during the ramp. The own framing with shared per-worker buffers (§13.2)
  brought memory to 15–22 KiB per connection and CPU per message to ≤ fastwebsockets, passes
  the same suites and Autobahn, and replaced both.
- A single busy worker accepts new connections more slowly during a 1000/s ramp; on macOS
  (listen backlog 128) a few connects were refused in one manual run. Consider prioritising
  the accept loop or `reusePort` on Linux.
- TLS / large tests on Linux and Ampere A1 with a separate client machine; `reusePort`.
- **Observability (planned):** structured logging (today only startup lines; runtime errors,
  rejected messages and closes pass silently) and possibly a Prometheus endpoint next to
  `/stats.json`.
- **Fuzzing, longer (planned):** the targets are libFuzzer / OSS-Fuzz compatible; longer runs
  than the nightly 20 min per target, e.g. through OSS-Fuzz.
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
| `servers[].server.key_file_name` + `cert_file_name` | — | PEM; both set → wss:// (rustls, ring, TLS 1.2 + 1.3, ALPN `http/1.1`, session tickets) |
| `servers[].server.passphrase`, `dh_params_file_name`, `ca_file_name`, `ssl_ciphers`, `ssl_prefer_low_memory_usage` | — | accepted, **ignored with a startup warning** |
| `servers[].websockets.path` | `/*` | `/*` any path, `/a/*` prefix, else exact (query ignored) |
| `servers[].websockets.maxPayloadLength` | 65536 | larger message → close |
| `servers[].websockets.idleTimeout` | 240 s | no frame received for this long → close; pings every `idleTimeout / 2`; 0 = off |
| `servers[].websockets.compression` | 1 | permessage-deflate (§13.2): 0 = off; 1 = negotiated like uWebSockets' shared compressor; other values → 1 with a startup warning |
| `servers[].websockets.compressOutgoingMinSize` (new) | 1024 | with permessage-deflate negotiated, outgoing messages at least this long are compressed; 0 = never (like JS) |
| `servers[].websockets.maxConnections` | 0 (off) | upgrade denied (TCP close) when open WebSockets `> maxConnections` (same off-by-one as JS) |
| `tracker.maxOffers` / `announceInterval` | 20 / 20 | §6; expiry runs every `announceInterval` |
| `tracker.offerSelection` | `sample` | `sample` / `window` / `round_robin` (§5.2) |
| `websocketsAccess.allowOrigins` / `denyOrigins` / `denyEmptyOrigin` | — | both lists set → config error; denied → TCP close |
| `workers` (new) | available parallelism | 1–64; one shard each |
| `placement` (new) | `content` | `content`: info_hash directory, swarms follow the content, connections move at their first announce; `hash`: `foldhash(info_hash) % workers`, no moves (§13.3). Unknown value → config error |
| `reusePort` (new) | false | Linux only: one `SO_REUSEPORT` socket per worker; otherwise one shared socket |
| `maxBackpressure` (new) | 1 MiB | per-connection queued bytes; further messages to it are dropped (`droppedMessages`) |
| `indexHtml` (new) | `./index.html` if present | served at `GET /` |
| `shutdownTimeout` (new) | 5 | seconds a graceful shutdown waits for connections to close (§13.6) |

Unknown fields are ignored. Invalid config (wrong types, both origin lists, half a key pair,
`workers` out of range, unknown `offerSelection` or `placement`) → error at startup.

### 13.2 Connections and HTTP

- TCP (`TCP_NODELAY`) → optional TLS handshake → one HTTP/1.1 request head (≤ 8 KiB; TLS +
  head within 10 s).
- `GET` with `Upgrade: websocket` on a matching path → `maxConnections` and origin checks (fail
  → TCP close) → `101` with `Sec-WebSocket-Accept` (requested `Sec-WebSocket-Protocol` echoed,
  like uws-tracker) and, if negotiated, `Sec-WebSocket-Extensions` (below). Bytes sent right
  after the head are kept.
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
    **Session resumption:** stateless session tickets (rustls `Ticketer`: AEAD ticket keys
    rotated every 6 h, shared by all workers, no server-side session state; one ticket per
    handshake, `send_tls13_tickets = 1`). A reconnecting client resumes without the certificate
    exchange (~3.5 KB less sent per reconnect with a Let's Encrypt ECDSA chain, no signature).
    Tickets do not survive a restart (new keys).
  - **Writes:** queued messages are sent with one vectored write per wake-up (frame headers +
    the shared encoder slices, no copy); TLS encrypts up to 64 KiB of frames per batch.
  - Text and binary messages are both parsed (§7); fragmented messages are reassembled.
  - **permessage-deflate** (`compression` ≥ 1, `ws/deflate.rs`): the first acceptable
    `permessage-deflate` offer of all `Sec-WebSocket-Extensions` lines is accepted
    (parameters `server_no_context_takeover`, `client_no_context_takeover`,
    `server_max_window_bits=8..15`, `client_max_window_bits[=8..15]`; an unknown, repeated or
    invalid parameter declines that offer). Response: `permessage-deflate;
    client_no_context_takeover; server_no_context_takeover`, plus
    `; server_max_window_bits=N` if offered. No context takeover either way, so every message is
    compressed on its own and a connection keeps no zlib state (2 bytes: negotiated, outgoing
    window; kept when it moves). RSV1 marks a compressed message on its first frame; the
    reassembled payload (≤ `maxPayloadLength` compressed) is inflated (`00 00 ff ff`
    appended, fed separately) by one raw inflater per worker thread (flate2 with zlib-rs, reset
    per message) into a buffer of the worker (freed above 256 KiB), at most `maxPayloadLength`
    bytes; then UTF-8 checked and handled like any message. Outgoing messages are compressed
    when at least `compressOutgoingMinSize` bytes long (default 1024: offers; announce replies
    and answers are shorter; 0 = never, like JS), for connections
    whose window is ≥ 9 (`server_max_window_bits=8` cannot be produced by zlib): one raw
    deflater per worker thread and window size (level 1, reset per message, sync flush, the
    trailing `00 00 ff ff` removed) into a new buffer per message, RSV1 set. Control frames are
    never compressed.
  - **Rules / close codes:** RSV2 / RSV3, RSV1 without permessage-deflate or on a control or
    continuation frame, unmasked client frames, unknown opcodes, fragmented or > 125-byte control
    frames, stray continuations → 1002; message (compressed or inflated) > `maxPayloadLength` →
    1009; corrupt compressed data → 1007;
    invalid UTF-8 in a text message or close reason → 1007; a message rejected by the tracker
    (§7.3) → 1008; a received close frame is answered with its code (1000 without one); idle
    timeout (no frame for `idleTimeout`, pings every `idleTimeout / 2`) or a close from the server
    side → 1000. Pings are answered with pongs, in order, before queued data. The close frame is
    written within 1 s, then TLS `close_notify` and TCP shutdown; the connection's peers are
    removed (§13.3).
  - The local shard path parses straight from the shared buffer and applies the message without
    any copy; a message for another worker is copied once (`OwnedMessage::copy_from`).
  - All per-connection state is owned and `Send`: a connection can be **detached** after a
    message (`Endpoint::message` → `Flow::Detach`) and resumed on another worker
    (`driver::resume`): the socket is deregistered (`into_std`) and re-registered there, with
    its rustls session, pending TLS bytes, the unparsed rest of its input and queued control
    frames (pongs); idle timing continues.
- Earlier transports (fastwebsockets, sockudo-ws) were removed after the comparison in §12.

### 13.3 Workers, sharding and placement

- N workers: a thread each, with a current-thread tokio runtime, its own `Shard` (seeded per
  worker), its connections and an inbox channel. Every worker accepts on every listener.
- `ConnId` = worker (8 bits) | generation (24 bits) | slot (32 bits); messages for a closed
  connection or a reused slot are dropped.
- **Owner of an info_hash** (`placement`, 1 worker → always the local shard):
  - `hash`: shard `foldhash(info_hash) % N` (seed shared by all workers);
  - `content` (default): a global directory `info_hash → worker` (`papaya` lock-free map,
    foldhash). **Invariant:** a swarm for `h` exists on shard `w` ⇒ `directory[h] = w`, so an
    info_hash is never split over shards. Kept by: *bind before create* — an announce for an
    unknown `h` claims it (insert-if-absent; a concurrent claimant gets the winner), and a
    shard that receives a forwarded announce for a swarm it does not have claims `h` itself
    first or, if another worker owns it, forwards it there (and tells the connection's worker
    that this shard may hold its peers, `Track`); *release only when empty* — at every expiry
    tick (`announceInterval`) each worker releases its entries without a swarm in its shard
    (remove-if-still-own). Both run on the owner's thread.
- **Placement of new info_hashes** (`content`): loads are per worker, published by the worker
  itself: open WebSocket connections (exact) and busy time of its runtime (tokio
  `worker_total_busy_duration`, sampled every 100 ms, moving average with weight 0.3, permille).
  Two distinct random workers are drawn (power of two choices):
  - **new content** (the first message of a connection is an announce of an unknown `h`): the
    one with fewer connections, if it has fewer than 0.8 × the connections of the accepting
    worker; otherwise the accepting worker;
  - **new hash of a placed connection** (audio, a new quality): the connection's worker, unless
    it is ≥ 80% busy and the less busy pick is at least 20 points less busy (spill; splits the
    content, so only when the worker is saturated).
- **Moving at the first message** (`content`): a connection's first message decides. An
  announce whose `h` is owned by (or newly placed on) another worker → nothing is applied, the
  connection is detached (§13.2) with that message and sent to the owner (`Adopt` event). The
  owner registers a new `ConnId`, applies the message, then resumes the connection (input that
  arrived with it is processed after it, in order). The old slot is freed without a disconnect:
  no shard holds the old id. Any other first message (scrape, stop, answer, a local announce)
  keeps the connection on its accepting worker. After its first message a connection never
  moves.
- A frame is parsed once by the worker that owns the connection, then routed:
  - announce / stop / answer / scrape of one hash → the owner's shard (`content`: an unknown
    `h` on stop / answer / scrape → the local shard; no swarm exists anywhere, same outcome);
    local shard → applied directly; otherwise sent as an `OwnedMessage` (one copy of the frame
    + offsets, no re-parse);
  - a stop whose ids cannot match → nothing; an answer without a usable `info_hash` → local shard
    if N = 1, else `BadField("info_hash")`;
  - scrape of all / several hashes → gathered (§13.4).
- The owning shard's output is encoded once per batch (`Encoder::take`): each message is a slice
  of one buffer, delivered to its connection's queue (local) or batched per destination worker
  (one channel send per destination per scheduler tick).
- A rejected message on a remote shard closes the connection on its own worker (`Close` event).
- Each connection remembers which shards it announced to; on close every one of them gets a
  disconnect, through the same FIFO as its requests. (A request forwarded again because its
  binding changed in flight can race a close; the peer then expires after
  `2 × announceInterval`.)

### 13.4 Scrape and stats across shards

Scrape of all swarms or of several hashes is scattered to the shards owning them and gathered
(`content`: hashes not in the directory are asked nowhere and reported 0 / 0); entries are
encoded in request order with the first occurrence kept (§7.2), all swarms in shard order.
`/stats.json` gathers `(info_hash, peers)` and the request counters of every shard.

### 13.5 `/stats.json`

`{"torrentsCount", "peersCount", "servers":[{"server":"host:port","webSocketsCount"}],
"memory":{"rss"}, "workers", "droppedMessages", "placement":{"mode", "movedConnections",
"localRequests", "remoteRequests", "workers":[{"connections", "busy"} per worker],
"directorySize"}, "traffic":{…}, "peersCountPerInfoHashPerTracker":[{"totalPeers",
"<hex info_hash>": peers, …} per shard]}`. `localRequests` / `remoteRequests`: requests of each
worker's connections applied to its own shard / sent to another one (scrape gathers not counted);
`busy`: 0–1, `content` only. `traffic` (totals of all workers since start, each `{"messages",
"bytes"}`): `sent` (`announceReplies`, `offers`, `answers`, `scrapes`: JSON bytes as encoded, before
framing and compression, counted by the shard that produced them), `received` (`announces`,
`answers`, `stops`, `scrapes`, `invalid`: JSON bytes after inflating, counted once by the worker
that handles them), `socketBytes` `{"in", "out"}` (bytes read from / written to sockets, TLS and
the HTTP upgrade included), `compression` `{"deflated", "inflated"}` each `{"messages",
"bytesBefore", "bytesAfter"}`. The hex is computed like JS `Buffer.from(infoHash,
"binary").toString("hex")` (one byte per character).

### 13.6 Shutdown

- The binary handles SIGINT and, on Unix, SIGTERM: the first signal starts a graceful shutdown
  (`Server::shutdown_gracefully(shutdownTimeout)`), a second one exits at once (exit code 1).
  After a graceful shutdown the exit code is 0.
- Graceful shutdown: every worker stops accepting (its listening sockets are closed, so new
  connects are refused), and every WebSocket gets a close frame with **1001** (Going Away) after
  the messages already queued for it, then TLS `close_notify` and TCP shutdown (each within the
  1 s close timeout of §13.2); its peers are removed. A connection that upgrades or moves to a
  worker during the shutdown is closed the same way at once. A worker stops when it has no
  WebSocket left or at the deadline (`shutdownTimeout` seconds; 0 = do not wait); connections
  still open then (and HTTP requests in progress) are dropped.
- `Server::shutdown` / dropping the `Server`: stop at once (in-process use, tests).

## 14. Load test (`crates/wt-loadgen`, `loadtest/run.sh`)

- `wt-loadgen load`: N clients (tokio multi-thread, tokio-tungstenite or with `--deflate` the own
  client, rustls with `--ca`), one
  connection and one peer_id each, watching one of `--swarms` contents. Each content has
  `--streams` streams (default 1; e.g. 2 = video + audio) and stream 0 has `--qualities`
  qualities (default 1); every (content, stream, quality) is its own swarm. A client announces
  `started` in its streams with `--offers` offers (SDP from `bench/fixtures/offer.sdp`),
  re-announces them every `--interval` s (spread over the interval), and answers every offer it
  receives (in the offer's swarm). With `--switch S` > 0 and several qualities it switches stream
  0 to the next quality every S s (spread): `started` in the new swarm, `stopped` in the old one
  `--overlap` s later (default 5; skipped if it switched back meanwhile). Connects are paced evenly at
  `--ramp` per second (bursts overflow small listen backlogs, e.g. macOS `somaxconn` = 128).
  After the ramp, a `--duration` s steady phase measures messages/s, announce → reply RTT
  (HdrHistogram), and the server's CPU seconds and RSS (`/proc` or `ps`, `--server-pid`).
  Connection failures and early closes are reported by reason (error text, or the server's close
  code). `--deflate`: the own client (`src/client.rs`: handshake, masked frames out, frames in,
  RSV1 inflate / deflate without context takeover, plain or rustls) offers Chrome's
  `permessage-deflate; client_max_window_bits` and, when negotiated, compresses every message it
  sends (zlib default level, like browsers); it counts bytes on the wire (TLS included):
  `wire_in_bytes` / `wire_out_bytes` per second and `deflate_negotiated` connections in the
  report (tungstenite rejects RSV1 frames, so it cannot be used for this).
- `wt-loadgen smoke`: a deterministic script over 3 clients (announces with full fan-out, an
  answer, a second swarm, scrapes, a stop, an invalid frame); the received messages per client,
  sorted; then a permessage-deflate step (own client with Chrome's offer): the server's
  `Sec-WebSocket-Extensions` and its replies to a compressed announce and scrape.
- `wt-loadgen gen-cert DIR`: self-signed `cert.pem` / `key.pem` for `localhost` (rcgen).
- `loadtest/run.sh`: for the profiles light (`LIGHT_CONNS`=3000, re-announce every 5 s, ws and
  wss), heavy (`HEAVY_CONNS`=4000, every 1 s, ws) and media (`MEDIA_CONNS`=3000, video + audio,
  4 qualities, switch every 10 s, overlap 5 s, every 5 s, ws), runs the JS tracker
  (`run-tracker.ts`), the JS multi-worker tracker (`run-worker-tracker.ts`), Rust with 1 worker,
  with all cores (`content` placement) and with all cores and `placement: "hash"`
  (`rust-n-hash`), each in a fresh process with the same config (`compression: 0`,
  `announceInterval: 120`). After each run it keeps the server's `/stats.json` `placement`
  (Rust): the Local % column. Profile deflate (`DEFLATE_CONNS`=3000, every 5 s, ws, `--deflate`)
  runs its own targets: `js`, `rust-1`, `rust-n` with `compression: 1` (Rust compressing outgoing
  messages ≥ 1 KiB, the default), `rust-n-in` (`compressOutgoingMinSize: 0`: inflating only, like
  JS) and `rust-n-off` (`compression: 0`: the uncompressed baseline with the same client); the
  Wire column. Then the smoke script against JS and Rust
  (both `compression: 1`), compared exactly, the deflate step included.
  Writes `bench/results/load.json` and regenerates the load table of §11.
- `loadtest/aquatic.sh`: the same profiles against [aquatic_ws](https://github.com/greatest-ape/aquatic)
  (Linux only: glommio / io_uring), pinned by `AQUATIC_REV`. `loadtest/aquatic/Dockerfile` builds
  aquatic_ws (default features: mimalloc, prometheus) and this workspace's `wt-tracker` and
  `wt-loadgen` from source (Debian trixie); `loadtest/aquatic/run.sh` runs inside one container
  (`seccomp=unconfined`, unlimited memlock for io_uring, nofile 65536), server and load generator
  sharing its CPUs: targets `aquatic-1` (1 socket + 1 swarm worker), `aquatic-n` (N threads:
  N − ⌊N/4⌋ socket, ⌊N/4⌋ swarm workers), `rust-1`, `rust-n` (N workers, `reusePort`). aquatic config:
  its defaults (`aquatic_ws -p`) with address, workers, TLS and `peer_announce_interval = 120`
  changed. Server CPU and RSS from `/proc` (RSS from `VmRSS`). Writes `bench/results/aquatic.json`
  and a second load table in §11. No wire smoke check (aquatic's replies differ from JS).
