# wt-tracker-rust

A multi-core [WebTorrent](https://webtorrent.io/) tracker in Rust: a port of
[wt-tracker](https://github.com/Novage/wt-tracker) (Node.js + uWebSockets.js), built for
[p2p-media-loader](https://github.com/Novage/p2p-media-loader) and other WebTorrent clients.

- **Drop-in for the JS tracker:** same `config.json` format, same wire protocol (replies are
  byte-identical to the JS tracker's, checked by a differential test).
- **Observable:** Prometheus `/metrics` on a separate listener, optionally HTTPS with a password
  (traffic, compression, closes and rejected messages by reason, CPU per worker, per worker),
  logfmt logs on stderr, and a ready Grafana dashboard with alert rules in
  [`monitoring/`](monitoring/README.md) (free with Grafana Cloud).
- **Multi-core without cross-thread traffic:** one tracker shard per core. The swarms of one piece
  of content (video, audio, every quality) share a shard, and a connection moves to that shard at
  its first announce, so its requests are handled on one core.
- **Own WebSocket implementation:** RFC 6455 framing that reads every connection into one buffer
  per worker thread, as uWebSockets does. TLS (rustls) works on the same buffers, and
  permessage-deflate is negotiated like the JS tracker's default. It passes all 517 cases of the
  Autobahn testsuite over ws and wss.
- **Low memory:** 5–8 KiB per connection under load (about 9 KiB with TLS), against 19–34 KiB
  for the JS tracker.

**Status:** in production since October 2026 at `wss://tracker.novage.com.ua` (Oracle Cloud
Ampere A1, 2 cores, Always Free), with up to 51k concurrent peers so far. Development continues;
open items are listed in [§12 of the specification](docs/SPEC.md#12-open-items).

## Performance

Load test on an Apple M1: client and server on the same machine, 100 swarms, 10 offers per
announce, every offer answered (`loadtest/run.sh`; full table in
[spec §11](docs/SPEC.md#11-performance-results)).

| Profile | Server | CPU µs / message | KiB / connection | RTT p50 |
|---|---|---:|---:|---:|
| 3000 connections, announce every 5 s | JS tracker | 17.0 | 27.1 | 0.47 ms |
| | Rust, 1 worker | **10.8** | **5.6** | **0.23 ms** |
| 4000 connections, 128k messages/s | JS tracker | 5.3 | 19.1 | 0.51 ms |
| | Rust, 1 worker | **4.3** | **6.7** | **0.32 ms** |
| | Rust, all cores | 5.4 | 7.3 | 0.30 ms |

**In production** at `wss://tracker.novage.com.ua` (Oracle Ampere A1, 2 cores, real
p2p-media-loader peers, ~42k connections): 0.35 cores and 590 MiB, against 1.46 cores and 1.9 GiB
for aquatic_ws on the same host and load; compressing outgoing offers cut egress by ~25% at no
measurable CPU cost ([spec §12](docs/SPEC.md#12-open-items)). The free server handles about 100k
peers within its free 10 TB of monthly egress and about 200k with its 2 cores (estimated).

Against [aquatic_ws](https://github.com/greatest-ape/aquatic) (Rust, io_uring) in a Linux
container, for the same traffic, this server uses 1.8–5.4× less CPU and 4–14 KiB per connection
instead of 75–102 KiB (`loadtest/aquatic.sh`).

## Quick start

Requires Rust 1.98 or newer.

```bash
cargo build --release -p wt-server
./target/release/wt-tracker config.json
```

**Production on Oracle Cloud's Always Free Ampere A1 with Let's Encrypt** (ports, stateless
network rules, certbot renewals without a restart, systemd):
[docs/install-oracle-ampere-a1.md](docs/install-oracle-ampere-a1.md).

Without an argument it reads `./config.json` if present, else listens on `ws://0.0.0.0:8000` with
the defaults. Clients connect to `ws://host:8000/` (any path by default). SIGTERM or Ctrl-C
closes every connection with 1001 (Going Away) and exits within `shutdownTimeout` seconds; a
second signal exits at once. SIGHUP reloads the TLS certificates without dropping connections
(have your renewal tool send it, e.g. `systemctl reload`).

## Configuration

The JS tracker's `config.json` format; unknown fields are ignored. For example:

```json
{
  "servers": [
    {
      "server": { "host": "0.0.0.0", "port": 443, "cert_file_name": "cert.pem", "key_file_name": "key.pem" },
      "websockets": { "path": "/*", "maxPayloadLength": 65536, "idleTimeout": 240, "compression": 1, "maxConnections": 0 }
    }
  ],
  "tracker": { "maxOffers": 20, "announceInterval": 20 },
  "websocketsAccess": { "allowOrigins": ["https://example.com"] },
  "workers": 8
}
```

Settings this server adds (all optional):

| Setting | Default | Meaning |
|---|---|---|
| `workers` | number of cores | worker threads (1–64), one tracker shard each |
| `placement` | `content` | `content`: swarms follow the content and connections move to them; `hash`: shard by `hash(info_hash)` |
| `reusePort` | `false` | Linux: one `SO_REUSEPORT` listening socket per worker |
| `maxBackpressure` | 1 MiB | queued outgoing bytes per connection before messages to it are dropped |
| `indexHtml` | `./index.html` if present | page served at `GET /` |
| `shutdownTimeout` | `5` | seconds to wait for connections to close on SIGTERM / SIGINT |
| `logLevel` | `info` | `error`, `warn`, `info` or `debug` (every connection close with its reason) |
| `metrics` | off | `{"host": "127.0.0.1", "port": 9100}`: listener for Prometheus `GET /metrics`, `GET /swarms` and `GET /stats.json`; add `cert_file_name` + `key_file_name` for HTTPS and `username` + `password` for basic auth |
| `websockets.compressOutgoingMinSize` | `1024` | with permessage-deflate, compress outgoing messages at least this long (0 = never, like the JS tracker) |
| `tracker.offerSelection` | `sample` | how offers pick peers: `sample`, `window` or `round_robin` |

Every setting and its exact behaviour: [spec §13.1](docs/SPEC.md#131-configuration-js-format).
Deliberate differences from the JS tracker: [spec §8](docs/SPEC.md#8-differences-from-the-js-fasttracker).

HTTP routes: the WebSocket upgrade on `websockets.path`, `GET /stats.json` (torrents, peers,
connections per listener, memory, workers, uptime; `?infoHash=<hex>` for one swarm) and `GET /`.
With `metrics` set, `GET /metrics` on that listener: Prometheus metrics per worker (messages and
bytes sent / received per kind, rejected messages and closed connections by reason, socket and
compression bytes, placement counters, process and per-worker CPU, file descriptors), `GET
/swarms?top=100`: the largest swarms with their hex info_hash, peers and worker, and `GET
/stats.json` ([spec §13.7](docs/SPEC.md#137-metrics-and-swarms)). Dashboard, alerts and setup:
[`monitoring/`](monitoring/README.md).

Logs: one logfmt line per event on stderr, e.g. `level=info event=listening addr=0.0.0.0:443`
(no timestamp under systemd / journald; [spec §13.8](docs/SPEC.md#138-logging)).

## Repository

| Path | What |
|---|---|
| `crates/wt-core` | tracker logic (peers, swarms, offer routing), sans-IO |
| `crates/wt-proto` | zero-copy JSON wire protocol, byte-compatible with the JS tracker |
| `crates/wt-server` | the server (binary `wt-tracker`): workers, placement, WebSocket, TLS, compression |
| `crates/wt-bench`, `bench/` | benchmarks, with identical JS benchmarks against `../wt-tracker` |
| `crates/wt-difftest`, `difftest/` | differential test against the JS tracker |
| `crates/wt-loadgen`, `loadtest/` | load generator, load tests (vs JS and aquatic), Autobahn |
| `docs/SPEC.md` | the specification: behaviour, protocol, server, tests, performance |
| `docs/install-oracle-ampere-a1.md` | install guide: Oracle Cloud Ampere A1, certbot, systemd |

## Development

[`docs/SPEC.md`](docs/SPEC.md) is the source of truth for behaviour, and every code change updates
it. [`AGENTS.md`](AGENTS.md) has the rules for people and coding agents. Before a pull request:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
node difftest/run.ts          # needs ../wt-tracker with node_modules
./scripts/check-spec.sh
```

CI runs these on every pull request and push to `main`, with a short fuzzing run of every fuzz
target (`fuzz/`, cargo-fuzz on nightly Rust), plus the Autobahn testsuite on `main`; every
night it runs all of them with 20 minutes of fuzzing per target.
Benchmarks (`bench/run.sh`) and load tests (`loadtest/run.sh`, `loadtest/aquatic.sh`) are run by
hand on an idle machine; they regenerate the tables in spec §11.

## Security

See [SECURITY.md](SECURITY.md).

## License

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
