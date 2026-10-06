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

- Traffic counters (now in `/metrics`): messages and bytes sent and received per kind, socket
  bytes, and bytes before / after compression and inflating.
- First production run at tracker.novage.com.ua (Oracle Ampere A1, 2 cores, real peers): 4× less
  CPU and 3× less memory than aquatic_ws on the same host and load.

- TLS session resumption with stateless session tickets: reconnecting clients skip the
  certificate exchange (~3.5 KB less egress per reconnect; ~25% of production egress was
  handshakes).

- Observability: Prometheus `/metrics` on an optional private listener (`metrics` setting):
  traffic, compression, placement counters, connections closed by reason and messages rejected
  by reason, per worker; a worker that does not answer within 1 s shows as `wt_worker_up 0`.
  Structured logs: one logfmt line per event on stderr (`logLevel`; rate-limited rejected
  messages and accept errors; every close with its reason at `debug`).
- `/stats.json?infoHash=<hex>`: peers of one swarm and the workers holding it.
- `/swarms?top=N` on the private listener: the largest swarms (hex info_hash, peers, worker).
- Install guide for Oracle Cloud Ampere A1 (Always Free) with certbot:
  `docs/install-oracle-ampere-a1.md`.
- TLS certificate reload without a restart on SIGHUP (e.g. `systemctl reload` from a certbot
  deploy hook; the files are not watched). Open connections and session tickets stay; a key
  that does not match is rejected and the old certificate kept. Metrics `wt_tls_reloads_total`
  and `wt_tls_certificate_expiry_seconds`.

- Metrics listener: optional HTTPS (`cert_file_name` / `key_file_name`, reloaded on SIGHUP with
  the other certificates) and HTTP basic auth (`username` / `password`), so a hosted scraper
  such as Grafana Cloud can read it from the internet; it also serves `/stats.json`.
- `/metrics`: process CPU, open and maximum file descriptors, CPU per worker thread (a stuck
  worker shows at 100%) and the kernel's listen-queue overflows (Linux).
- `monitoring/`: a Grafana dashboard and alert rules built on `/metrics` alone, with a setup
  guide for Grafana Cloud's free tier or Prometheus.

### Changed

- **Hardening (differs from the JS tracker):** a `stop` is applied only from the connection
  that owns the peer (peer_ids are public: any client could remove other viewers). An answer is
  delivered only if its sender is a peer of the sending connection and both sender and target
  are in the answer's swarm; otherwise it is dropped (`wt_dropped_answers_total`) and the
  connection stays open, also for an unknown target (which closed it before). An answer needs a
  string `info_hash` (else `bad_field`, close 1008) with any number of workers.
- **Breaking:** `/stats.json` is a small summary (`torrentsCount`, `peersCount`, `servers`,
  `memory`, `workers`, `uptimeSeconds`); `peersCountPerInfoHashPerTracker`, `placement`,
  `traffic` and `droppedMessages` moved to `/metrics`, the per-info-hash list to `/swarms`. A
  request no longer copies every swarm.
- Startup and shutdown lines are logfmt events on stderr instead of plain text on stdout.

- `compressOutgoingMinSize` now defaults to 1024: outgoing messages of at least 1 KiB (offers)
  are compressed when the client negotiated permessage-deflate (~25% less egress in production at
  no measurable CPU); 0 restores the JS behaviour (never).

- WebSocket libraries fastwebsockets and sockudo-ws, used by the first server prototype, were
  replaced by the own implementation (5–9 instead of 63–193 KiB per connection).

### Fixed

- A worker could hang at 100% CPU, stalling its connections until an out-of-memory kill: a
  client whose first TLS record was larger than 8 KiB and arrived in parts left the HTTP head
  reader looping on a readable socket it did not read, without ever yielding. Production
  incidents on 2026-10-04, -05 and -06; caught by a watchdog's `perf` capture.
- A message rejected by another worker's shard closed its connection with 1000 instead of 1008
  (Policy Violation), unlike a rejection by the local shard.
