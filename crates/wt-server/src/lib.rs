//! Multi-core WebTorrent tracker server: N workers, each a thread with its own runtime, tracker
//! shard and connections; own WebSocket framing over TCP or TLS (rustls), reading into one
//! shared buffer per worker. Spec §13.

pub mod config;
mod conn;
pub mod echo;
#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod fuzz;
mod http;
pub mod logging;
mod metrics;
pub mod placement;
mod reasons;
mod stats;
mod tls;
mod worker;
mod ws;

use std::hash::BuildHasher;
use std::net::{SocketAddr, TcpListener as StdListener, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{mpsc, watch};

pub use config::Config;
use config::{AccessConfig, WebSocketsConfig};
use worker::{Event, WorkerListener};

/// State shared by all workers (read-only after start, plus atomics).
pub(crate) struct Shared {
    pub senders: Vec<mpsc::UnboundedSender<Vec<Event>>>,
    /// `hash` placement: routes an info_hash to its shard; the same seed in every worker.
    pub router: foldhash::fast::RandomState,
    pub seed: u64,
    pub workers: usize,
    pub settings: wt_core::Settings,
    pub max_backpressure: usize,
    pub access: AccessConfig,
    pub index_html: Option<Bytes>,
    pub listeners: Vec<ListenerInfo>,
    pub placement: placement::Mode,
    /// info_hash → owning worker (`content` placement).
    pub directory: placement::Directory,
    pub loads: placement::Loads,
    /// When the server started (uptime), and as seconds since the Unix epoch (`/metrics`).
    pub started: Instant,
    pub started_unix: u64,
}

pub(crate) struct ListenerInfo {
    /// `host:port` as configured (for stats).
    pub name: String,
    pub websockets: WebSocketsConfig,
    /// wss:// when set.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Open WebSocket connections on this listener, all workers.
    pub web_sockets: AtomicUsize,
}

/// What the workers should do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Running,
    /// Graceful shutdown: close every connection, stop when all are gone or at the deadline.
    Drain(Instant),
    /// Stop now.
    Stop,
}

/// A running server. Dropping it (or [`Server::shutdown`]) stops all workers at once;
/// [`Server::shutdown_gracefully`] closes the connections first.
pub struct Server {
    addrs: Vec<SocketAddr>,
    metrics_addr: Option<SocketAddr>,
    workers: usize,
    shutdown: watch::Sender<Phase>,
    threads: Vec<JoinHandle<()>>,
}

impl Server {
    /// Bound address of every listener, in config order (useful with port 0).
    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }

    /// Bound address of the `/metrics` listener, if configured.
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn shutdown(self) {}

    /// Stops accepting connections, closes every WebSocket with 1001 (Going Away) after the
    /// messages already queued for it, and returns when all are closed or after `timeout`
    /// (spec §13.6).
    pub fn shutdown_gracefully(mut self, timeout: Duration) {
        let _ = self.shutdown.send(Phase::Drain(Instant::now() + timeout));
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown.send(Phase::Stop);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

fn bind(addr: SocketAddr, reuse_port: bool) -> std::io::Result<StdListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(not(unix))]
    let _ = reuse_port;
    socket.bind(&addr.into())?;
    socket.listen(4096)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Binds every listener and starts the workers.
pub fn start(config: Config) -> Result<Server, String> {
    config.validate()?;
    let workers = config.worker_count();
    let reuse_port = config.reuse_port && cfg!(target_os = "linux");

    let mut listeners = Vec::new();
    let mut sockets: Vec<Vec<Option<StdListener>>> = Vec::new();
    let mut addrs = Vec::new();
    for item in &config.servers {
        let s = &item.server;
        let name = format!("{}:{}", s.host, s.port);
        let addr = (s.host.as_str(), s.port)
            .to_socket_addrs()
            .map_err(|e| format!("{name}: {e}"))?
            .next()
            .ok_or_else(|| format!("{name}: no address"))?;
        let first =
            bind(addr, reuse_port).map_err(|e| format!("failed to listen to {name}: {e}"))?;
        let local = first.local_addr().map_err(|e| e.to_string())?;
        let mut per_worker = vec![Some(first)];
        if reuse_port {
            for _ in 1..workers {
                per_worker.push(Some(bind(local, true).map_err(|e| format!("{name}: {e}"))?));
            }
        }
        let tls = match (&s.cert_file_name, &s.key_file_name) {
            (Some(cert), Some(key)) => Some(tls::server_config(cert, key)?),
            _ => None,
        };
        addrs.push(local);
        sockets.push(per_worker);
        listeners.push(ListenerInfo {
            name,
            websockets: item.websockets.clone(),
            tls,
            web_sockets: AtomicUsize::new(0),
        });
    }

    // Worker 0 accepts the metrics scrapers.
    let mut metrics = match &config.metrics {
        Some(m) => {
            let name = format!("{}:{}", m.host, m.port);
            let addr = (m.host.as_str(), m.port)
                .to_socket_addrs()
                .map_err(|e| format!("metrics {name}: {e}"))?
                .next()
                .ok_or_else(|| format!("metrics {name}: no address"))?;
            Some(bind(addr, false).map_err(|e| format!("failed to listen to {name}: {e}"))?)
        }
        None => None,
    };
    let metrics_addr = match &metrics {
        Some(socket) => Some(socket.local_addr().map_err(|e| e.to_string())?),
        None => None,
    };

    let index_path = config
        .index_html
        .clone()
        .unwrap_or_else(|| "index.html".into());
    let index_html = match std::fs::read(&index_path) {
        Ok(html) => Some(Bytes::from(html)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && config.index_html.is_none() => None,
        Err(e) => return Err(format!("{}: {e}", index_path.display())),
    };

    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..workers).map(|_| mpsc::unbounded_channel()).unzip();
    let router = foldhash::fast::RandomState::default();
    let seed = router.hash_one(0x5eed_u64);
    let shared = Arc::new(Shared {
        senders,
        router,
        seed,
        workers,
        settings: config.tracker_settings()?,
        max_backpressure: config.max_backpressure,
        access: config.websockets_access.clone(),
        index_html,
        listeners,
        placement: config.placement_mode()?,
        directory: placement::Directory::new(),
        loads: placement::Loads::new(workers),
        started: Instant::now(),
        started_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
    });

    let (shutdown, shutdown_rx) = watch::channel(Phase::Running);
    let mut threads = Vec::new();
    for (id, inbox) in receivers.into_iter().enumerate() {
        let mut own = Vec::new();
        for (index, per_worker) in sockets.iter_mut().enumerate() {
            let socket = if per_worker.len() == workers {
                per_worker[id].take().expect("one socket per worker")
            } else {
                per_worker[0]
                    .as_ref()
                    .expect("shared socket")
                    .try_clone()
                    .map_err(|e| e.to_string())?
            };
            own.push(WorkerListener { index, socket });
        }
        let shared = shared.clone();
        let shutdown_rx = shutdown_rx.clone();
        let metrics = metrics.take();
        threads.push(
            std::thread::Builder::new()
                .name(format!("wt-worker-{id}"))
                .spawn(move || worker::run(id, shared, inbox, own, metrics, shutdown_rx))
                .map_err(|e| e.to_string())?,
        );
    }

    Ok(Server {
        addrs,
        metrics_addr,
        workers,
        shutdown,
        threads,
    })
}
