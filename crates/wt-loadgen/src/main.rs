//! Tracker-shaped load generator and wire smoke check (spec §14). Works against any
//! wt-tracker server (JS or Rust); talks only the wire protocol.
//!
//! ```text
//! wt-loadgen load  --url ws://127.0.0.1:8000/ [--conns 5000] [--swarms 100] [--offers 10]
//!                  [--interval 5] [--ramp 1000] [--duration 30] [--threads N] [--ca cert.pem]
//!                  [--streams 1] [--qualities 1] [--switch 0] [--overlap 5] [--deflate]
//!                  [--server-pid PID] [--label NAME]           → JSON report on stdout
//! wt-loadgen smoke --url ws://127.0.0.1:8000/ [--ca cert.pem]  → JSON transcript on stdout
//! wt-loadgen gen-cert DIR                                       → DIR/cert.pem, DIR/key.pem
//! ```

mod client;

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use tokio_rustls::rustls;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const SDP_FIXTURE: &str = include_str!("../../../bench/fixtures/offer.sdp");

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn num<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    arg(args, name).map_or(default, |v| {
        v.parse().unwrap_or_else(|_| panic!("bad {name}"))
    })
}

fn tls_config(args: &[String]) -> Option<Arc<rustls::ClientConfig>> {
    let ca = arg(args, "--ca")?;
    let pem = std::fs::read(&ca).unwrap_or_else(|e| panic!("{ca}: {e}"));
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::pem::PemObject::pem_slice_iter(&pem) {
        let cert: rustls::pki_types::CertificateDer = cert.expect("PEM certificate");
        roots.add(cert).expect("CA certificate");
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    Some(Arc::new(config))
}

fn connector(args: &[String]) -> Option<Connector> {
    tls_config(args).map(Connector::Rustls)
}

/// A client connection: tokio-tungstenite, or the own client (permessage-deflate, wire bytes).
enum Conn {
    Tungstenite(Box<Ws>),
    Own(Box<client::Client>),
}

enum In {
    Text(String),
    Close(Option<u16>),
    Error(String),
    End,
    Other,
}

impl Conn {
    async fn send(&mut self, text: String) -> bool {
        match self {
            Conn::Tungstenite(ws) => ws.send(Message::text(text)).await.is_ok(),
            Conn::Own(c) => c.send_text(&text).await.is_ok(),
        }
    }

    /// Cancel safe.
    async fn recv(&mut self) -> In {
        match self {
            Conn::Tungstenite(ws) => match ws.next().await {
                Some(Ok(Message::Text(t))) => In::Text(t.to_string()),
                Some(Ok(Message::Close(frame))) => In::Close(frame.map(|f| u16::from(f.code))),
                Some(Ok(_)) => In::Other,
                Some(Err(e)) => In::Error(e.to_string()),
                None => In::End,
            },
            Conn::Own(c) => match c.next().await {
                Ok(Some(client::Incoming::Text(t))) => In::Text(t),
                Ok(Some(client::Incoming::Close(code))) => In::Close(code),
                Ok(None) => In::End,
                Err(e) => In::Error(e),
            },
        }
    }

    async fn close(&mut self) {
        match self {
            Conn::Tungstenite(ws) => {
                let _ = ws.close(None).await;
            }
            Conn::Own(c) => c.close().await,
        }
    }
}

async fn connect(url: &str, connector: Option<Connector>) -> Result<Ws, String> {
    tokio_tungstenite::connect_async_tls_with_config(url, None, true, connector)
        .await
        .map(|(ws, _)| ws)
        .map_err(|e| e.to_string())
}

/// A string field of a server message (our ids and offer_ids never contain escapes).
fn field<'a>(message: &'a str, name: &str) -> Option<&'a str> {
    let key = format!("\"{name}\":\"");
    let start = message.find(&key)? + key.len();
    let len = message[start..].find('"')?;
    Some(&message[start..start + len])
}

fn id(prefix: char, n: usize) -> String {
    format!("{prefix}{n:019}")
}

fn sdp_json(n: usize) -> String {
    serde_json::to_string(&SDP_FIXTURE.replace("{session}", &format!("{n:019}"))).unwrap()
}

fn announce(info_hash: &str, peer_id: &str, event: Option<&str>, offers: &[String]) -> String {
    let event = event
        .map(|e| format!(r#","event":"{e}""#))
        .unwrap_or_default();
    format!(
        r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"{peer_id}"{event},"numwant":{},"offers":[{}]}}"#,
        offers.len(),
        offers.join(",")
    )
}

fn answer(info_hash: &str, peer_id: &str, to: &str, offer_id: &str, sdp: &str) -> String {
    format!(
        r#"{{"action":"announce","info_hash":"{info_hash}","peer_id":"{peer_id}","to_peer_id":"{to}","answer":{{"type":"answer","sdp":{sdp}}},"offer_id":"{offer_id}"}}"#
    )
}

// ---- load ----

#[derive(Default)]
struct Counters {
    connected: AtomicU64,
    failed: AtomicU64,
    closed_early: AtomicU64,
    sent_announces: AtomicU64,
    sent_answers: AtomicU64,
    replies: AtomicU64,
    offers: AtomicU64,
    answers: AtomicU64,
    sent_stops: AtomicU64,
    /// Connections that negotiated permessage-deflate (`--deflate`).
    deflate: AtomicU64,
}

impl Counters {
    fn snapshot(&self, wire: &client::Wire) -> [u64; 8] {
        [
            self.sent_announces.load(Relaxed),
            self.sent_answers.load(Relaxed),
            self.replies.load(Relaxed),
            self.offers.load(Relaxed),
            self.answers.load(Relaxed),
            self.sent_stops.load(Relaxed),
            wire.read.load(Relaxed),
            wire.written.load(Relaxed),
        ]
    }
}

struct Load {
    url: String,
    connector: Option<Connector>,
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Own client offering permessage-deflate and compressing everything it sends.
    deflate: bool,
    wire: Arc<client::Wire>,
    swarms: usize,
    offers: Vec<Vec<String>>,
    interval: Duration,
    /// Swarms per client of its content (stream 0 = video, the others e.g. audio).
    streams: usize,
    /// Qualities of stream 0; a switch moves to the next one.
    qualities: usize,
    /// Quality switch period (zero: never) and how long the old swarm is kept.
    switch: Duration,
    overlap: Duration,
    counters: Counters,
    rtt: Mutex<Histogram<u64>>,
    failures: Mutex<std::collections::BTreeMap<String, u64>>,
    measuring: std::sync::atomic::AtomicBool,
    stop: tokio::sync::watch::Receiver<bool>,
}

async fn client(load: Arc<Load>, n: usize) {
    let connected = if load.deflate {
        client::connect(&load.url, load.tls.clone(), true, load.wire.clone())
            .await
            .map(|c| Conn::Own(Box::new(c)))
    } else {
        connect(&load.url, load.connector.clone())
            .await
            .map(|ws| Conn::Tungstenite(Box::new(ws)))
    };
    let mut ws = match connected {
        Ok(ws) => ws,
        Err(e) => {
            load.counters.failed.fetch_add(1, Relaxed);
            *load.failures.lock().unwrap().entry(e).or_default() += 1;
            return;
        }
    };
    load.counters.connected.fetch_add(1, Relaxed);
    if matches!(&ws, Conn::Own(c) if c.deflate()) {
        load.counters.deflate.fetch_add(1, Relaxed);
    }
    let peer_id = id('c', n);
    let content = n % load.swarms;
    // Every (content, stream, quality) is its own swarm; one stream and quality = one swarm
    // per content, as without these options.
    let stream_hash = |stream: usize, quality: usize| {
        id(
            'h',
            (content * load.streams + stream) * load.qualities + quality,
        )
    };
    let mut quality = 0;
    let mut hashes: Vec<String> = (0..load.streams).map(|k| stream_hash(k, 0)).collect();
    let offers = &load.offers[n % load.offers.len()];
    let answer_sdp = sdp_json(1_000_000 + n);
    let mut stop = load.stop.clone();

    let mut pending: Option<Instant> = Some(Instant::now());
    for info_hash in &hashes {
        let message = announce(info_hash, &peer_id, Some("started"), offers);
        if !ws.send(message).await {
            load.counters.closed_early.fetch_add(1, Relaxed);
            return;
        }
        load.counters.sent_announces.fetch_add(1, Relaxed);
    }
    // Spread re-announces (and switches) over their periods.
    let spread = (n % 1000) as f64 / 1000.0;
    let now = tokio::time::Instant::now();
    let mut tick = tokio::time::interval_at(now + load.interval.mul_f64(spread), load.interval);
    let switch_every = if load.switch.is_zero() {
        Duration::from_secs(1 << 30)
    } else {
        load.switch
    };
    let mut switch =
        tokio::time::interval_at(now + switch_every.mul_f64(spread.max(0.001)), switch_every);
    // Old qualities to stop: (deadline, info_hash).
    let mut stops: std::collections::VecDeque<(tokio::time::Instant, String)> = Default::default();
    let far = || tokio::time::Instant::now() + Duration::from_secs(1 << 30);

    loop {
        let next_stop = stops.front().map_or_else(far, |(at, _)| *at);
        tokio::select! {
            _ = stop.changed() => break,
            _ = tick.tick() => {
                pending.get_or_insert_with(Instant::now);
                for info_hash in &hashes {
                    if !ws.send(announce(info_hash, &peer_id, None, offers)).await {
                        load.counters.closed_early.fetch_add(1, Relaxed);
                        return;
                    }
                    load.counters.sent_announces.fetch_add(1, Relaxed);
                }
            }
            _ = switch.tick(), if !load.switch.is_zero() && load.qualities > 1 => {
                quality = (quality + 1) % load.qualities;
                let old = std::mem::replace(&mut hashes[0], stream_hash(0, quality));
                stops.push_back((tokio::time::Instant::now() + load.overlap, old));
                let message = announce(&hashes[0], &peer_id, Some("started"), offers);
                if !ws.send(message).await {
                    load.counters.closed_early.fetch_add(1, Relaxed);
                    return;
                }
                load.counters.sent_announces.fetch_add(1, Relaxed);
            }
            _ = tokio::time::sleep_until(next_stop), if !stops.is_empty() => {
                let (_, old) = stops.pop_front().unwrap();
                // Back to an old quality within the overlap: it is still in use.
                if !hashes.contains(&old) {
                    let message = announce(&old, &peer_id, Some("stopped"), &[]);
                    if !ws.send(message).await {
                        load.counters.closed_early.fetch_add(1, Relaxed);
                        return;
                    }
                    load.counters.sent_stops.fetch_add(1, Relaxed);
                }
            }
            message = ws.recv() => {
                let text = match message {
                    In::Text(text) => text,
                    In::Other => continue,
                    In::Close(code) => {
                        load.counters.closed_early.fetch_add(1, Relaxed);
                        let reason = format!("closed by server: {code:?}");
                        *load.failures.lock().unwrap().entry(reason).or_default() += 1;
                        return;
                    }
                    In::Error(e) => {
                        load.counters.closed_early.fetch_add(1, Relaxed);
                        *load.failures.lock().unwrap().entry(format!("read error: {e}")).or_default() += 1;
                        return;
                    }
                    In::End => {
                        load.counters.closed_early.fetch_add(1, Relaxed);
                        *load.failures.lock().unwrap().entry("stream ended".into()).or_default() += 1;
                        return;
                    }
                };
                if text.contains("\"interval\":") {
                    load.counters.replies.fetch_add(1, Relaxed);
                    if let Some(sent) = pending.take()
                        && load.measuring.load(Relaxed)
                    {
                        let _ = load.rtt.lock().unwrap().record(sent.elapsed().as_micros() as u64);
                    }
                } else if text.contains("\"offer\":{") {
                    load.counters.offers.fetch_add(1, Relaxed);
                    if let (Some(info_hash), Some(from), Some(offer_id)) =
                        (field(&text, "info_hash"), field(&text, "peer_id"), field(&text, "offer_id"))
                    {
                        let reply = answer(info_hash, &peer_id, from, offer_id, &answer_sdp);
                        if !ws.send(reply).await {
                            load.counters.closed_early.fetch_add(1, Relaxed);
                            return;
                        }
                        load.counters.sent_answers.fetch_add(1, Relaxed);
                    }
                } else if text.contains("\"answer\":") {
                    load.counters.answers.fetch_add(1, Relaxed);
                }
            }
        }
    }
    ws.close().await;
}

/// Cumulative CPU seconds and RSS bytes of a process.
fn process_usage(pid: u32) -> Option<(f64, u64)> {
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        let fields: Vec<&str> = stat.rsplit(')').next()?.split_whitespace().collect();
        let ticks: f64 =
            fields.get(11)?.parse::<f64>().ok()? + fields.get(12)?.parse::<f64>().ok()?;
        // VmRSS: independent of the page size.
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        let kb: u64 = status
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))?
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()?;
        return Some((ticks / 100.0, kb * 1024));
    }
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    let rss: u64 = parts.next()?.parse().ok()?;
    // [[H:]M]M:SS.ss
    let seconds = parts
        .next()?
        .split(':')
        .try_fold(0.0, |acc, p| p.parse::<f64>().ok().map(|v| acc * 60.0 + v))?;
    Some((seconds, rss * 1024))
}

async fn run_load(args: Vec<String>) -> Value {
    let url = arg(&args, "--url").expect("--url");
    let conns: usize = num(&args, "--conns", 5000);
    let swarms: usize = num(&args, "--swarms", 100);
    let offers_per: usize = num(&args, "--offers", 10);
    let interval = Duration::from_secs_f64(num(&args, "--interval", 5.0));
    let ramp: usize = num(&args, "--ramp", 1000);
    let duration = Duration::from_secs_f64(num(&args, "--duration", 30.0));
    let streams: usize = num(&args, "--streams", 1usize).max(1);
    let qualities: usize = num(&args, "--qualities", 1usize).max(1);
    let switch = Duration::from_secs_f64(num(&args, "--switch", 0.0));
    let overlap = Duration::from_secs_f64(num(&args, "--overlap", 5.0));
    let deflate = args.iter().any(|a| a == "--deflate");
    let server_pid: Option<u32> =
        arg(&args, "--server-pid").map(|p| p.parse().expect("--server-pid"));

    // 64 offer sets, reused round-robin (offer bodies differ, sizes are realistic).
    let offers: Vec<Vec<String>> = (0..64)
        .map(|set| {
            (0..offers_per)
                .map(|k| {
                    format!(
                        r#"{{"offer":{{"type":"offer","sdp":{}}},"offer_id":"o{set:09}{k:010}"}}"#,
                        sdp_json(set * offers_per + k)
                    )
                })
                .collect()
        })
        .collect();
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let load = Arc::new(Load {
        url: url.clone(),
        connector: connector(&args),
        tls: tls_config(&args),
        deflate,
        wire: Arc::default(),
        swarms,
        offers,
        interval,
        streams,
        qualities,
        switch,
        overlap,
        counters: Counters::default(),
        rtt: Mutex::new(Histogram::new(3).unwrap()),
        failures: Mutex::default(),
        measuring: false.into(),
        stop,
    });

    // Server memory before any connection (fixed costs: runtimes, preallocated buffers).
    let idle_rss = server_pid.and_then(process_usage).map(|(_, rss)| rss);

    // Evenly paced connects: bursts overflow small listen backlogs (macOS somaxconn = 128).
    let mut pace = tokio::time::interval(Duration::from_secs_f64(1.0 / ramp.max(1) as f64));
    pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut tasks = Vec::with_capacity(conns);
    for n in 0..conns {
        pace.tick().await;
        tasks.push(tokio::spawn(client(load.clone(), n)));
    }
    // Wait for the ramp to settle (all connections attempted).
    let settle = Instant::now();
    while (load.counters.connected.load(Relaxed) + load.counters.failed.load(Relaxed))
        < conns as u64
        && settle.elapsed() < Duration::from_secs(30)
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(interval).await;

    // Steady phase.
    let before = load.counters.snapshot(&load.wire);
    let usage_before = server_pid.and_then(process_usage);
    load.measuring.store(true, Relaxed);
    let started = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = started.elapsed().as_secs_f64();
    load.measuring.store(false, Relaxed);
    let after = load.counters.snapshot(&load.wire);
    let usage_after = server_pid.and_then(process_usage);

    let _ = stop_tx.send(true);
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    let d: Vec<f64> = (0..8)
        .map(|i| (after[i] - before[i]) as f64 / elapsed)
        .collect();
    let sent = d[0] + d[1] + d[5];
    let received = d[2] + d[3] + d[4];
    let rtt = load.rtt.lock().unwrap();
    let ms = |q: f64| rtt.value_at_quantile(q) as f64 / 1000.0;
    let connected = load.counters.connected.load(Relaxed);
    let server = match (usage_before, usage_after) {
        (Some((cpu0, _)), Some((cpu1, rss))) => {
            let cores = (cpu1 - cpu0) / elapsed;
            json!({
                "cpu_cores": cores,
                "cpu_us_per_message": if sent + received > 0.0 { cores * 1e6 / (sent + received) } else { 0.0 },
                "rss_bytes": rss,
                "rss_bytes_per_conn": rss as f64 / connected.max(1) as f64,
                "rss_idle_bytes": idle_rss,
                "rss_bytes_per_conn_above_idle": idle_rss
                    .map(|idle| rss.saturating_sub(idle) as f64 / connected.max(1) as f64),
            })
        }
        _ => Value::Null,
    };
    json!({
        "label": arg(&args, "--label").unwrap_or_else(|| url.clone()),
        "url": url,
        "conns": conns,
        "connected": connected,
        "failed": load.counters.failed.load(Relaxed),
        "failures": *load.failures.lock().unwrap(),
        "closed_early": load.counters.closed_early.load(Relaxed),
        "swarms": swarms,
        "offers": offers_per,
        "streams": streams,
        "qualities": qualities,
        "switch_s": switch.as_secs_f64(),
        "overlap_s": overlap.as_secs_f64(),
        "interval_s": interval.as_secs_f64(),
        "deflate": deflate,
        "deflate_negotiated": load.counters.deflate.load(Relaxed),
        "duration_s": elapsed,
        "per_second": {
            "announces": d[0], "answers_sent": d[1], "replies": d[2], "offers": d[3], "answers_received": d[4],
            "stops": d[5],
            // Own client only (--deflate): bytes on the wire, TLS included.
            "wire_in_bytes": deflate.then_some(d[6]),
            "wire_out_bytes": deflate.then_some(d[7]),
            "sent": sent, "received": received,
        },
        "rtt_ms": { "p50": ms(0.5), "p99": ms(0.99), "max": rtt.max() as f64 / 1000.0, "samples": rtt.len() },
        "server": server,
    })
}

// ---- smoke ----

async fn next_text(ws: &mut Ws) -> Option<String> {
    loop {
        match tokio::time::timeout(Duration::from_millis(300), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return Some(t.to_string()),
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            _ => return None,
        }
    }
}

/// Drains everything that arrives until 300 ms of silence.
async fn drain(ws: &mut Ws, into: &mut Vec<String>) {
    while let Some(m) = next_text(ws).await {
        into.push(m);
    }
}

/// A deterministic sequential script (full fan-out only, so no randomness): messages received
/// per client, sorted.
async fn run_smoke(args: Vec<String>) -> Value {
    let url = arg(&args, "--url").expect("--url");
    let connector = connector(&args);
    let mut clients = Vec::new();
    for _ in 0..3 {
        clients.push(connect(&url, connector.clone()).await.expect("connect"));
    }
    let mut got: Vec<Vec<String>> = vec![Vec::new(); 3];
    let offer = |k: usize| {
        format!(
            r#"{{"offer":{{"type":"offer","sdp":{}}},"offer_id":"s{k}"}}"#,
            sdp_json(k)
        )
    };
    let script: Vec<(usize, String)> = vec![
        (0, announce("hsmoke00000000000001", "pa", Some("started"), &[offer(0), offer(1)])),
        (1, announce("hsmoke00000000000001", "pb", Some("completed"), &[offer(2), offer(3)])),
        (2, announce("hsmoke00000000000001", "pc", None, &[offer(4), offer(5)])),
        (0, answer("hsmoke00000000000001", "pa", "pb", "s2", &sdp_json(9))),
        (2, announce("hsmoke00000000000002", "pc2", Some("started"), &[])),
        (1, r#"{"action":"scrape","info_hash":["hsmoke00000000000001","hsmoke00000000000002","nope"]}"#.into()),
        (0, r#"{"action":"announce","event":"stopped","info_hash":"hsmoke00000000000001","peer_id":"pa"}"#.into()),
        (1, r#"{"action":"scrape","info_hash":"hsmoke00000000000001"}"#.into()),
        (2, "{ not json".into()),
        (1, r#"{"action":"scrape","info_hash":"hsmoke00000000000001"}"#.into()),
    ];
    for (who, frame) in script {
        let _ = clients[who].send(Message::text(frame)).await;
        for (i, ws) in clients.iter_mut().enumerate() {
            drain(ws, &mut got[i]).await;
        }
    }
    for messages in &mut got {
        messages.sort();
    }
    json!({ "url": url, "received": got, "deflate": smoke_deflate(&url, &args).await })
}

/// permessage-deflate: the extension the server agrees to for Chrome's offer, and its replies
/// to compressed messages.
async fn smoke_deflate(url: &str, args: &[String]) -> Value {
    let mut c = client::connect(url, tls_config(args), true, Arc::default())
        .await
        .expect("connect");
    let extension = c.extension.clone();
    let mut received = Vec::new();
    for message in [
        announce("hsmoke00000000000003", "pd", Some("started"), &[]),
        r#"{"action":"scrape","info_hash":"hsmoke00000000000003"}"#.to_string(),
    ] {
        c.send_text(&message).await.expect("send");
        while let Ok(Ok(Some(client::Incoming::Text(t)))) =
            tokio::time::timeout(Duration::from_millis(300), c.next()).await
        {
            received.push(t);
        }
    }
    json!({ "extension": extension, "received": received })
}

fn gen_cert(dir: &str) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(format!("{dir}/cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(format!("{dir}/key.pem"), cert.signing_key.serialize_pem()).unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let threads: usize = num(
        &args,
        "--threads",
        std::thread::available_parallelism().map_or(4, |n| n.get()),
    );
    let mode = args.get(1).cloned().unwrap_or_default();
    match mode.as_str() {
        "gen-cert" => gen_cert(args.get(2).expect("gen-cert DIR")),
        "load" | "smoke" => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .enable_all()
                .build()
                .unwrap();
            let report = runtime.block_on(async {
                if mode == "load" {
                    run_load(args).await
                } else {
                    run_smoke(args).await
                }
            });
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        _ => {
            eprintln!(
                "usage: wt-loadgen load|smoke --url URL [options] | gen-cert DIR (see source header)"
            );
            std::process::exit(2);
        }
    }
}
