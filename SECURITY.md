# Security policy

## Supported versions

The project has no releases yet. Security fixes go to the `main` branch only.

## Reporting a vulnerability

Please do not report security problems in public issues or pull requests.

Report them privately through GitHub: on the repository's **Security** tab, choose **Report a
vulnerability**. This opens a private advisory that only the maintainers can see. Include:

- what an attacker can do, and what they need (a WebSocket connection, a TLS client, a crafted
  frame or message);
- steps or a small program to reproduce it, and the configuration used;
- the commit you tested.

We will acknowledge the report in the advisory, discuss the fix there, and credit you when the
advisory is published, unless you prefer not to be named.

## Scope

In scope: anything a remote client can trigger, for example

- a crash, panic or hang of the server or of a worker thread;
- memory or CPU use beyond the configured limits (`maxPayloadLength`, `maxBackpressure`,
  `maxConnections`, `idleTimeout`), including decompression bombs over permessage-deflate;
- bugs in the WebSocket framing, TLS, HTTP upgrade or JSON parsing;
- bypassing `websocketsAccess` origin rules;
- messages delivered to the wrong peer or swarm, or data of one client exposed to another beyond
  what the WebTorrent protocol shares by design.

Out of scope: denial of service by sheer traffic volume, issues that need access to the server's
host or configuration, and the known limitations below.

## Hardening a deployment

- Keep the limits above at their defaults or lower; set `maxConnections` for the capacity of the
  host, and `websocketsAccess.allowOrigins` to your sites.
- Run the server as an unprivileged user, with the TLS key readable only by it.
- `/stats.json` lists every info_hash with its peer count, like the JS tracker. If that is
  sensitive, do not expose it publicly (block it in a reverse proxy).

## Known limitations

These match the JS tracker and are tracked in [spec §12](docs/SPEC.md#12-open-items):

- There are no per-IP connection or request rate limits; put them in front of the server
  (firewall or proxy) if you need them.
- A `stopped` announce removes the given peer_id even if it was sent by another connection.
- Peers exchange WebRTC offers and answers through the tracker, so they learn each other's ICE
  candidates (IP addresses). This is inherent to WebTorrent.
