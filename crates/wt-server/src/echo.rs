//! A minimal WebSocket echo server on the native transport (own framing + shared buffers), for
//! protocol conformance testing with the Autobahn testsuite (`loadtest/autobahn.sh`). Not used
//! by the tracker.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{LocalSet, spawn_local};

use crate::http;
use crate::worker::{Out, Pop};
use crate::ws::driver::{self, Endpoint, Flow, Io, Limits};

#[derive(Default)]
struct Echo {
    queue: RefCell<VecDeque<Out>>,
    wake: Notify,
    stop: Notify,
}

impl Endpoint for Echo {
    fn message(&self, text: bool, data: &[u8]) -> Result<Flow, u16> {
        let data = Bytes::copy_from_slice(data);
        self.queue.borrow_mut().push_back(if text {
            Out::Text(data)
        } else {
            Out::Binary(data)
        });
        self.wake.notify_one();
        Ok(Flow::Continue)
    }
    fn pop(&self) -> Pop {
        match self.queue.borrow_mut().pop_front() {
            Some(out) => Pop::Frame(out),
            None => Pop::Empty,
        }
    }
    fn wake(&self) -> &Notify {
        &self.wake
    }
    fn stop(&self) -> &Notify {
        &self.stop
    }
}

async fn serve(tcp: TcpStream, tls: Option<Arc<ServerConfig>>) {
    let _ = tcp.set_nodelay(true);
    let mut io = match tls {
        Some(config) => match Io::accept_tls(tcp, config).await {
            Ok(io) => io,
            Err(_) => return,
        },
        None => Io::Plain(tcp),
    };
    let Ok((head, rest)) = io.read_head().await else {
        return;
    };
    // Compression on, both directions, so Autobahn's 12.* / 13.* cases cover inflate and deflate.
    let (response, deflate) = match head
        .upgrade_websocket
        .then(|| http::upgrade_response(&head, true))
    {
        Some(Ok(response)) => response,
        _ => {
            let _ = io
                .write_all(&http::response("404 Not Found", None, b"404 Not Found"))
                .await;
            return io.shutdown().await;
        }
    };
    if io.write_all(response.as_bytes()).await.is_err() {
        return;
    }
    let limits = Limits {
        max_payload: 64 << 20,
        idle: Duration::ZERO,
        compress_min: 1,
    };
    // Echo never detaches.
    let _ = driver::run(io, rest, limits, deflate, &Echo::default()).await;
}

/// Serves until the process ends. `tls`: PEM certificate chain and key files.
pub fn run(addr: SocketAddr, tls: Option<(PathBuf, PathBuf)>) -> Result<(), String> {
    let config = match tls {
        Some((cert, key)) => Some(crate::tls::Tls::open(&cert, &key)?.config),
        None => None,
    };
    let std = std::net::TcpListener::bind(addr).map_err(|e| format!("{addr}: {e}"))?;
    std.set_nonblocking(true).map_err(|e| e.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    LocalSet::new().block_on(&runtime, async move {
        let listener = TcpListener::from_std(std).expect("listener");
        let config = Rc::new(config);
        loop {
            if let Ok((tcp, _)) = listener.accept().await {
                spawn_local(serve(tcp, (*config).clone()));
            }
        }
    })
}
