//! The private listener (spec §13.7): Prometheus `/metrics` (text format 0.0.4) and `/swarms`.

use std::fmt::{Display, Write as _};
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;
use tokio::time::timeout;

use crate::http;
use crate::reasons::{CloseReason, HttpRoute, RejectReason};
use crate::stats;
use crate::worker::{DROPPED_MESSAGES, ShardStats, Worker};
use crate::ws::driver::Io;

/// The request head must arrive within this time.
const HEAD_TIMEOUT: Duration = Duration::from_secs(10);
/// `/swarms` without `top`.
const DEFAULT_TOP: usize = 100;

/// Accepts scrapers on worker 0 until shutdown.
pub(crate) async fn accept_loop(me: Rc<Worker>, socket: TcpListener) {
    loop {
        match socket.accept().await {
            Ok((stream, _)) => {
                spawn_local(serve(me.clone(), stream));
            }
            Err(e) => {
                crate::event_limited!(Error, "accept_failed", listener = "metrics", error = e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

async fn serve(me: Rc<Worker>, stream: TcpStream) {
    let mut io = Io::Plain(stream);
    let Ok(Ok((head, _))) = timeout(HEAD_TIMEOUT, io.read_head()).await else {
        return;
    };
    let (path, query) = head.path.split_once('?').unwrap_or((&head.path, ""));
    let response = match (head.method.as_str(), path) {
        ("GET", "/metrics") => {
            let body = render(&me).await;
            http::response(
                "200 OK",
                Some("text/plain; version=0.0.4; charset=utf-8"),
                body.as_bytes(),
            )
        }
        ("GET", "/swarms") => match stats::query_param(query, "top").map(str::parse::<usize>) {
            None => swarms_response(&me, DEFAULT_TOP).await,
            Some(Ok(top)) => swarms_response(&me, top).await,
            Some(Err(_)) => http::response("400 Bad Request", None, b"top must be a number"),
        },
        _ => http::response("404 Not Found", None, b"404 Not Found"),
    };
    let _ = io.write_all(&response).await;
    io.shutdown().await;
}

async fn swarms_response(me: &Rc<Worker>, top: usize) -> Vec<u8> {
    let body = stats::swarms_json(me, top).await;
    http::response("200 OK", Some("application/json"), body.as_bytes())
}

/// The text exposition: one `# HELP` / `# TYPE` header per family, then its samples.
struct Text(String);

impl Text {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &dyn Display)], value: impl Display) {
        self.0.push_str(name);
        if !labels.is_empty() {
            self.0.push('{');
            for (i, (key, label)) in labels.iter().enumerate() {
                if i > 0 {
                    self.0.push(',');
                }
                let label = label.to_string();
                let _ = write!(self.0, "{key}=\"{}\"", escape(&label));
            }
            self.0.push('}');
        }
        let _ = writeln!(self.0, " {value}");
    }

    /// One family with a sample per worker.
    fn per_worker(
        &mut self,
        name: &str,
        kind: &str,
        help: &str,
        stats: &[ShardStats],
        value: impl Fn(&ShardStats) -> u64,
    ) {
        self.family(name, kind, help);
        for (worker, s) in stats.iter().enumerate().filter(|(_, s)| s.up) {
            self.sample(name, &[("worker", &worker)], value(s));
        }
    }

    /// One family with a sample per worker and label value; `value(stats, i)` is the value
    /// for `labels[i]`.
    #[allow(clippy::too_many_arguments)]
    fn per_worker_label(
        &mut self,
        name: &str,
        kind: &str,
        help: &str,
        stats: &[ShardStats],
        label: &str,
        labels: &[&str],
        value: impl Fn(&ShardStats, usize) -> u64,
    ) {
        self.family(name, kind, help);
        for (worker, s) in stats.iter().enumerate().filter(|(_, s)| s.up) {
            for (i, value_label) in labels.iter().enumerate() {
                self.sample(
                    name,
                    &[("worker", &worker), (label, value_label)],
                    value(s, i),
                );
            }
        }
    }
}

/// Label values escape `\`, `"` and newlines.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(crate) async fn render(me: &Rc<Worker>) -> String {
    let stats = me.stats().await;
    let shared = &me.shared;
    let mut t = Text(String::with_capacity(16 * 1024));

    t.family(
        "wt_build_info",
        "gauge",
        "Version of the running wt-tracker.",
    );
    t.sample(
        "wt_build_info",
        &[("version", &env!("CARGO_PKG_VERSION"))],
        1,
    );
    t.family(
        "process_start_time_seconds",
        "gauge",
        "Start time of the process since the Unix epoch, in seconds.",
    );
    t.sample("process_start_time_seconds", &[], shared.started_unix);
    if let Some(rss) = crate::stats::rss_bytes() {
        t.family(
            "process_resident_memory_bytes",
            "gauge",
            "Resident memory size, in bytes.",
        );
        t.sample("process_resident_memory_bytes", &[], rss);
    }

    t.family(
        "wt_worker_up",
        "gauge",
        "1 if the worker answered the metrics request in time.",
    );
    for (worker, s) in stats.iter().enumerate() {
        t.sample("wt_worker_up", &[("worker", &worker)], u8::from(s.up));
    }
    t.per_worker(
        "wt_torrents",
        "gauge",
        "Swarms in the worker's shard.",
        &stats,
        |s| s.swarms as u64,
    );
    t.per_worker(
        "wt_peers",
        "gauge",
        "Peers in the worker's swarms (a peer in two swarms counts twice).",
        &stats,
        |s| s.peers as u64,
    );
    let loads = shared.loads.snapshot();
    t.family(
        "wt_connections",
        "gauge",
        "Open WebSocket connections of the worker.",
    );
    for (worker, load) in loads.iter().enumerate() {
        t.sample("wt_connections", &[("worker", &worker)], load.conns);
    }
    if !(shared.placement == crate::placement::Mode::Hash || shared.workers == 1) {
        t.family(
            "wt_worker_busy_ratio",
            "gauge",
            "Share of time the worker's runtime was busy (moving average, content placement).",
        );
        for (worker, load) in loads.iter().enumerate() {
            t.sample(
                "wt_worker_busy_ratio",
                &[("worker", &worker)],
                load.busy as f64 / 1000.0,
            );
        }
    }
    t.family(
        "wt_listener_connections",
        "gauge",
        "Open WebSocket connections of the listener, all workers.",
    );
    for l in &shared.listeners {
        t.sample(
            "wt_listener_connections",
            &[("listener", &l.name)],
            l.web_sockets.load(Relaxed),
        );
    }

    const RECEIVED: [&str; 5] = ["announce", "answer", "stop", "scrape", "invalid"];
    let received = |s: &ShardStats, i: usize| {
        let r = &s.received;
        [r.announces, r.answers, r.stops, r.scrapes, r.invalid][i]
    };
    t.per_worker_label(
        "wt_received_messages_total",
        "counter",
        "Messages received from the worker's connections, per kind (invalid: not parsed).",
        &stats,
        "kind",
        &RECEIVED,
        |s, i| received(s, i).messages,
    );
    t.per_worker_label(
        "wt_received_bytes_total",
        "counter",
        "JSON bytes received (after inflating), per kind.",
        &stats,
        "kind",
        &RECEIVED,
        |s, i| received(s, i).bytes,
    );

    const SENT: [&str; 4] = ["announce_reply", "offer", "answer", "scrape"];
    let sent = |s: &ShardStats, i: usize| {
        let c = &s.sent;
        [c.announce_replies, c.offers, c.answers, c.scrapes][i]
    };
    t.per_worker_label(
        "wt_sent_messages_total",
        "counter",
        "Messages produced by the worker's shard, per kind.",
        &stats,
        "kind",
        &SENT,
        |s, i| sent(s, i).messages,
    );
    t.per_worker_label(
        "wt_sent_bytes_total",
        "counter",
        "JSON bytes produced (before framing and compression), per kind.",
        &stats,
        "kind",
        &SENT,
        |s, i| sent(s, i).bytes,
    );

    t.per_worker_label(
        "wt_rejected_messages_total",
        "counter",
        "Messages rejected (the connection is closed with 1008), per reason.",
        &stats,
        "reason",
        &RejectReason::ALL
            .iter()
            .map(|r| r.as_str())
            .collect::<Vec<_>>(),
        |s, i| s.rejected[i],
    );
    t.per_worker_label(
        "wt_closed_connections_total",
        "counter",
        "Connections that ended, per reason (before the upgrade included).",
        &stats,
        "reason",
        &CloseReason::ALL
            .iter()
            .map(|r| r.as_str())
            .collect::<Vec<_>>(),
        |s, i| s.closed[i],
    );
    t.per_worker_label(
        "wt_http_requests_total",
        "counter",
        "HTTP requests answered without an upgrade, per route.",
        &stats,
        "route",
        &HttpRoute::ALL
            .iter()
            .map(|r| r.as_str())
            .collect::<Vec<_>>(),
        |s, i| s.http[i],
    );

    t.per_worker_label(
        "wt_socket_bytes_total",
        "counter",
        "Bytes read from / written to sockets (TLS and the HTTP upgrade included).",
        &stats,
        "direction",
        &["in", "out"],
        |s, i| [s.io.socket_in, s.io.socket_out][i],
    );
    t.per_worker(
        "wt_deflate_messages_total",
        "counter",
        "Outgoing messages compressed.",
        &stats,
        |s| s.io.deflated,
    );
    t.per_worker_label(
        "wt_deflate_bytes_total",
        "counter",
        "Bytes of compressed outgoing messages, before and after compression.",
        &stats,
        "stage",
        &["before", "after"],
        |s, i| [s.io.deflate_in, s.io.deflate_out][i],
    );
    t.per_worker(
        "wt_inflate_messages_total",
        "counter",
        "Incoming compressed messages.",
        &stats,
        |s| s.io.inflated,
    );
    t.per_worker_label(
        "wt_inflate_bytes_total",
        "counter",
        "Bytes of incoming compressed messages, before and after inflating.",
        &stats,
        "stage",
        &["before", "after"],
        |s, i| [s.io.inflate_in, s.io.inflate_out][i],
    );
    t.per_worker_label(
        "wt_routed_requests_total",
        "counter",
        "Requests of the worker's connections applied to its own shard or sent to another.",
        &stats,
        "target",
        &["local", "remote"],
        |s, i| [s.local_requests, s.remote_requests][i],
    );
    t.per_worker(
        "wt_moved_connections_total",
        "counter",
        "Connections that moved to the worker at their first announce.",
        &stats,
        |s| s.moved_in,
    );
    t.per_worker(
        "wt_expired_peers_total",
        "counter",
        "Peers removed by expiry.",
        &stats,
        |s| s.expired,
    );
    if shared.certs().next().is_some() {
        t.family(
            "wt_tls_reloads_total",
            "counter",
            "Certificate reloads of wss:// listeners (SIGHUP or a file change), by result.",
        );
        for (name, cert) in shared.certs() {
            for (result, n) in [("ok", &cert.reloads_ok), ("error", &cert.reloads_failed)] {
                t.sample(
                    "wt_tls_reloads_total",
                    &[("listener", &name), ("result", &result)],
                    n.load(Relaxed),
                );
            }
        }
        t.family(
            "wt_tls_certificate_expiry_seconds",
            "gauge",
            "notAfter of the certificate a wss:// listener serves, in Unix seconds.",
        );
        // No sample for a certificate whose notAfter could not be parsed: a 0 would look expired.
        for (name, cert) in shared.certs() {
            if let Some(not_after) = cert.not_after() {
                t.sample(
                    "wt_tls_certificate_expiry_seconds",
                    &[("listener", &name)],
                    not_after,
                );
            }
        }
    }
    t.family(
        "wt_dropped_messages_total",
        "counter",
        "Messages dropped because a connection exceeded maxBackpressure.",
    );
    t.sample(
        "wt_dropped_messages_total",
        &[],
        DROPPED_MESSAGES.load(Relaxed),
    );
    t.family(
        "wt_directory_entries",
        "gauge",
        "info_hashes bound to a worker (content placement).",
    );
    t.sample("wt_directory_entries", &[], shared.directory.len());
    t.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_and_label_escaping() {
        let mut t = Text(String::new());
        t.family("wt_x_total", "counter", "Help text.");
        t.sample("wt_x_total", &[("worker", &1), ("listener", &"a\"b\\c")], 7);
        t.sample("wt_y", &[], 0.5);
        assert_eq!(
            t.0,
            "# HELP wt_x_total Help text.\n# TYPE wt_x_total counter\n\
             wt_x_total{worker=\"1\",listener=\"a\\\"b\\\\c\"} 7\nwt_y 0.5\n"
        );
    }
}
