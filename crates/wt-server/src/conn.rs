//! Accepting connections: TLS (on shared buffers), the HTTP request head, routes, and the
//! WebSocket upgrade into the connection driver (spec §13.2).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use bytes::Bytes;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;
use tokio::time::timeout;
use wt_core::ConnId;

use crate::http;
use crate::stats;
use crate::worker::{Event, Handled, Pop, Worker};
use crate::ws::codec::close;
use crate::ws::deflate::Negotiated;
use crate::ws::driver::{self, Endpoint, Flow, Io, Limits, Parked};

/// TLS handshake + HTTP request head must complete within this time.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn accept_loop(me: Rc<Worker>, listener: usize, socket: TcpListener) {
    loop {
        match socket.accept().await {
            Ok((stream, _)) => {
                spawn_local(serve(me.clone(), listener, stream));
            }
            // e.g. EMFILE: back off instead of spinning.
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

async fn serve(me: Rc<Worker>, listener: usize, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let info = &me.shared.listeners[listener];
    let mut io = match &info.tls {
        Some(config) => {
            match timeout(HANDSHAKE_TIMEOUT, Io::accept_tls(stream, config.clone())).await {
                Ok(Ok(io)) => io,
                _ => return,
            }
        }
        None => Io::Plain(stream),
    };
    let Ok(Ok((head, rest))) = timeout(HANDSHAKE_TIMEOUT, io.read_head()).await else {
        return;
    };
    match route(&me, listener, &head).await {
        Route::Upgrade(response, deflate) => {
            if io.write_all(response.as_bytes()).await.is_err() {
                return;
            }
            info.web_sockets.fetch_add(1, Relaxed);
            let endpoint = TrackerEndpoint::new(&me, false);
            let parked = driver::run(io, rest, limits(&me, listener), deflate, &endpoint).await;
            let conn = endpoint.conn;
            match (parked, endpoint.moving.take()) {
                // Moved before anything was applied: no shard holds the connection.
                (Some(parked), Some((worker, first))) => {
                    me.remove(conn);
                    let adopt = Adopt {
                        listener,
                        first,
                        parked,
                    };
                    me.push_remote(worker, Event::Adopt(Box::new(adopt)));
                }
                _ => {
                    me.begin_close(conn);
                    me.remove(conn);
                    info.web_sockets.fetch_sub(1, Relaxed);
                }
            }
        }
        Route::Respond(bytes) => {
            let _ = io.write_all(&bytes).await;
            io.shutdown().await;
        }
        Route::Drop => {}
    }
}

fn limits(me: &Worker, listener: usize) -> Limits {
    let ws = &me.shared.listeners[listener].websockets;
    Limits {
        max_payload: ws.max_payload_length,
        idle: Duration::from_secs(ws.idle_timeout),
        compress_min: ws.compress_outgoing_min_size,
    }
}

/// A connection moving to another worker at its first message (spec §13.3).
pub(crate) struct Adopt {
    listener: usize,
    /// The message that moved it, handled first by the new worker.
    first: Bytes,
    parked: Parked,
}

/// Continues a connection that moved to this worker.
pub(crate) async fn adopted(me: Rc<Worker>, adopt: Adopt) {
    let endpoint = TrackerEndpoint::new(&me, true);
    let limits = limits(&me, adopt.listener);
    // Placed connections never detach again; a parked result would be dropped (closed).
    let Adopt {
        listener,
        first,
        parked,
    } = adopt;
    let _ = driver::resume(parked, first, limits, &endpoint).await;
    me.begin_close(endpoint.conn);
    me.remove(endpoint.conn);
    me.shared.listeners[listener]
        .web_sockets
        .fetch_sub(1, Relaxed);
}

/// What to do with a request head (spec §13.2).
enum Route {
    /// Upgrade with this `101` response (and permessage-deflate, if negotiated).
    Upgrade(String, Option<Negotiated>),
    /// Send this complete HTTP response, then close.
    Respond(Vec<u8>),
    /// Close the TCP connection without a response (denied upgrade).
    Drop,
}

async fn route(me: &Rc<Worker>, listener: usize, head: &http::Head) -> Route {
    let info = &me.shared.listeners[listener];
    if head.method == "GET"
        && head.upgrade_websocket
        && http::path_matches(&info.websockets.path, &head.path)
    {
        // Same check as uws-tracker (`webSocketsCount > maxConnections`): denied → TCP close.
        let max = info.websockets.max_connections;
        if max != 0 && info.web_sockets.load(Relaxed) > max {
            return Route::Drop;
        }
        if !http::origin_allowed(&me.shared.access, head.origin.as_deref()) {
            return Route::Drop;
        }
        return match http::upgrade_response(head, info.websockets.compression > 0) {
            Ok((response, deflate)) => Route::Upgrade(response, deflate),
            Err(_) => Route::Drop,
        };
    }
    let not_found = || http::response("404 Not Found", None, b"404 Not Found");
    let path = head.path.split('?').next().unwrap_or("");
    Route::Respond(match (head.method.as_str(), path) {
        ("GET", "/") => match &me.shared.index_html {
            Some(html) => http::response("200 OK", Some("text/html"), html),
            None => not_found(),
        },
        ("GET", "/stats.json") => {
            let body = stats::json(me).await;
            http::response("200 OK", Some("application/json"), body.as_bytes())
        }
        _ => not_found(),
    })
}

/// The tracker side of a native WebSocket connection.
struct TrackerEndpoint {
    me: Rc<Worker>,
    conn: ConnId,
    notify: Rc<tokio::sync::Notify>,
    closed: Rc<tokio::sync::Notify>,
    /// Set when the first message moves the connection: target worker and the message.
    moving: RefCell<Option<(usize, Bytes)>>,
}

impl TrackerEndpoint {
    fn new(me: &Rc<Worker>, placed: bool) -> Self {
        let (conn, notify, closed) = me.register(placed);
        if me.is_draining() {
            // Upgraded (or moved here) during a graceful shutdown.
            me.begin_close_with(conn, close::GOING_AWAY);
        }
        Self {
            me: me.clone(),
            conn,
            notify,
            closed,
            moving: RefCell::new(None),
        }
    }
}

impl Endpoint for TrackerEndpoint {
    fn message(&self, _text: bool, data: &[u8]) -> Result<Flow, u16> {
        match self.me.handle_message(self.conn, data) {
            Ok(Handled::Done) => Ok(Flow::Continue),
            Ok(Handled::Move(worker)) => {
                *self.moving.borrow_mut() = Some((worker, Bytes::copy_from_slice(data)));
                Ok(Flow::Detach)
            }
            Err(_) => Err(close::POLICY),
        }
    }
    fn pop(&self) -> Pop {
        self.me.pop(self.conn)
    }
    fn wake(&self) -> &tokio::sync::Notify {
        &self.notify
    }
    fn stop(&self) -> &tokio::sync::Notify {
        &self.closed
    }
}
