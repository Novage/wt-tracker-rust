# Changelog

All notable changes to this project are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions will follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) once releases start.

## [Unreleased]

### Added

- Tracker logic (`wt-core`): peers, swarms, offer routing; one connection may carry several
  peer_ids in several swarms; offer selection strategies `sample` (default), `window` and
  `round_robin`. Benchmarks against identical JS benchmarks.
- Wire protocol (`wt-proto`): zero-copy JSON parsing and encoding, byte-compatible with the JS
  tracker (`JSON.stringify` output), checked by a differential test against `../wt-tracker`.
- Server (`wt-tracker` binary): the JS tracker's `config.json`, ws:// and wss:// (rustls),
  `/stats.json`, `/` and 404 routes, WebSocket path, origin rules, `maxConnections`,
  `maxPayloadLength`, idle timeout with pings, per-connection backpressure limit.
- Multi-core: one worker thread with its own tracker shard per core.
- Own WebSocket implementation (RFC 6455) reading every connection into one buffer per worker
  thread, TLS on rustls' unbuffered API over the same buffers.
- Content placement (default): an info_hash directory keeps the swarms of one piece of content
  on one worker; connections move to it at their first announce; new content goes to the worker
  with fewer connections. `placement: "hash"` keeps hash routing.
- permessage-deflate negotiated like the JS tracker's default (`compression: 1`): client messages
  inflated by one shared inflater per worker; opt-in compression of large outgoing messages
  (`compressOutgoingMinSize`).
- Load generator and load tests: JS vs Rust (light, heavy, media and deflate profiles), against
  aquatic_ws in a Linux container, wire smoke check against the JS tracker.
- Autobahn testsuite runs over ws and wss, compression included.
- README, LICENSE (Apache-2.0), NOTICE, SECURITY.md, this changelog, CI for pull requests and
  `main`, toolchain pinned to Rust 1.98.1.

- Graceful shutdown on SIGTERM / SIGINT: connections are closed with 1001 (Going Away) and the
  server waits up to `shutdownTimeout` seconds (default 5); a second signal exits at once.

- Fuzzing: cargo-fuzz targets for WebSocket frames, permessage-deflate, the HTTP upgrade and the
  protocol applied to a shard; run in CI on every pull request and nightly, and on stable over
  the seed corpus by `cargo test`.

- `/stats.json` `traffic`: messages and bytes sent and received per kind, socket bytes, and
  bytes before / after compression and inflating.
- First production run at tracker.novage.com.ua (Oracle Ampere A1, 2 cores, real peers): 4× less
  CPU and 3× less memory than aquatic_ws on the same host and load.

- TLS session resumption with stateless session tickets: reconnecting clients skip the
  certificate exchange (~3.5 KB less egress per reconnect; ~25% of production egress was
  handshakes).

### Changed

- `compressOutgoingMinSize` now defaults to 1024: outgoing messages of at least 1 KiB (offers)
  are compressed when the client negotiated permessage-deflate (~25% less egress in production at
  no measurable CPU); 0 restores the JS behaviour (never).

- WebSocket libraries fastwebsockets and sockudo-ws, used by the first server prototype, were
  replaced by the own implementation (5–9 instead of 63–193 KiB per connection).
