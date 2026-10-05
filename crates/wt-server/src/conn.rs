//! Accepting connections: TLS (on shared buffers), the HTTP request head, routes, and the
//! WebSocket upgrade into the connection driver (spec §13.2).

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bytes::Bytes;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;
use tokio::time::timeout;
use wt_core::ConnId;

use crate::http;
use crate::reasons::{CloseReason, HttpRoute};
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
            Ok((stream, peer)) => {
                spawn_local(serve(me.clone(), listener, stream, peer));
            }
            // e.g. EMFILE: back off instead of spinning.
            Err(e) => {
                crate::event_limited!(
                    Error,
                    "accept_failed",
                    listener = me.shared.listeners[listener].name,
                    error = e
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Counts a connection that ended and logs it (debug).
fn ended(me: &Worker, reason: CloseReason, peer: SocketAddr, opened: Instant) {
    me.count_close(reason);
    crate::event!(
        Debug,
        "connection_closed",
        reason = reason.as_str(),
        peer = peer,
        duration_s = format_args!("{:.3}", opened.elapsed().as_secs_f64()),
        worker = me.id
    );
}

async fn serve(me: Rc<Worker>, listener: usize, stream: TcpStream, peer: SocketAddr) {
    let opened = Instant::now();
    let _ = stream.set_nodelay(true);
    let info = &me.shared.listeners[listener];
    let mut io = match &info.tls {
        Some(tls) => {
            match timeout(
                HANDSHAKE_TIMEOUT,
                Io::accept_tls(stream, tls.config.clone()),
            )
            .await
            {
                Ok(Ok(io)) => io,
                failed => {
                    let error = match failed {
                        Ok(Err(e)) => e.to_string(),
                        _ => "timeout".into(),
                    };
                    crate::event!(Debug, "tls_handshake_failed", peer = peer, error = error);
                    return ended(&me, CloseReason::TlsHandshake, peer, opened);
                }
            }
        }
        None => Io::Plain(stream),
    };
    let Ok(Ok((head, rest))) = timeout(HANDSHAKE_TIMEOUT, io.read_head()).await else {
        return ended(&me, CloseReason::BadRequest, peer, opened);
    };
    match route(&me, listener, &head).await {
        Route::Upgrade(response, deflate) => {
            if io.write_all(response.as_bytes()).await.is_err() {
                return ended(&me, CloseReason::SocketError, peer, opened);
            }
            info.web_sockets.fetch_add(1, Relaxed);
            let endpoint = TrackerEndpoint::new(&me, false);
            let result = driver::run(io, rest, limits(&me, listener), deflate, &endpoint).await;
            let conn = endpoint.conn;
            match (result, endpoint.moving.take()) {
                // Moved before anything was applied: no shard holds the connection.
                (Ok(parked), Some((worker, first))) => {
                    me.remove(conn);
                    let adopt = Adopt {
                        listener,
                        first,
                        parked,
                        peer,
                        opened,
                    };
                    me.push_remote(worker, Event::Adopt(Box::new(adopt)));
                }
                (result, _) => {
                    me.begin_close(conn);
                    me.remove(conn);
                    info.web_sockets.fetch_sub(1, Relaxed);
                    let reason = result.err().unwrap_or(CloseReason::ServerClose);
                    ended(&me, reason, peer, opened);
                }
            }
        }
        Route::Respond(bytes, route) => {
            me.count_http(route);
            if route == HttpRoute::NotFound {
                crate::event!(Debug, "not_found", peer = peer, path = head.path);
            }
            let _ = io.write_all(&bytes).await;
            io.shutdown().await;
        }
        Route::Drop(reason) => {
            crate::event!(
                Debug,
                "upgrade_denied",
                reason = reason.as_str(),
                peer = peer
            );
            ended(&me, reason, peer, opened);
        }
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
    peer: SocketAddr,
    opened: Instant,
}

/// Continues a connection that moved to this worker.
pub(crate) async fn adopted(me: Rc<Worker>, adopt: Adopt) {
    let endpoint = TrackerEndpoint::new(&me, true);
    let limits = limits(&me, adopt.listener);
    let Adopt {
        listener,
        first,
        parked,
        peer,
        opened,
    } = adopt;
    // Placed connections never detach again; a parked result would be dropped (closed).
    let reason = driver::resume(parked, first, limits, &endpoint)
        .await
        .err()
        .unwrap_or(CloseReason::ServerClose);
    me.begin_close(endpoint.conn);
    me.remove(endpoint.conn);
    me.shared.listeners[listener]
        .web_sockets
        .fetch_sub(1, Relaxed);
    ended(&me, reason, peer, opened);
}

/// What to do with a request head (spec §13.2).
enum Route {
    /// Upgrade with this `101` response (and permessage-deflate, if negotiated).
    Upgrade(String, Option<Negotiated>),
    /// Send this complete HTTP response, then close.
    Respond(Vec<u8>, HttpRoute),
    /// Close the TCP connection without a response (denied upgrade).
    Drop(CloseReason),
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
            return Route::Drop(CloseReason::MaxConnections);
        }
        if !http::origin_allowed(&me.shared.access, head.origin.as_deref()) {
            return Route::Drop(CloseReason::OriginDenied);
        }
        return match http::upgrade_response(head, info.websockets.compression > 0) {
            Ok((response, deflate)) => Route::Upgrade(response, deflate),
            Err(_) => Route::Drop(CloseReason::BadUpgrade),
        };
    }
    let not_found = || {
        Route::Respond(
            http::response("404 Not Found", None, b"404 Not Found"),
            HttpRoute::NotFound,
        )
    };
    let (path, query) = head.path.split_once('?').unwrap_or((&head.path, ""));
    match (head.method.as_str(), path) {
        ("GET", "/") => match &me.shared.index_html {
            Some(html) => Route::Respond(
                http::response("200 OK", Some("text/html"), html),
                HttpRoute::Index,
            ),
            None => not_found(),
        },
        ("GET", "/stats.json") => {
            let response = match stats::query_param(query, "infoHash") {
                None => {
                    let body = stats::json(me).await;
                    http::response("200 OK", Some("application/json"), body.as_bytes())
                }
                Some(hex) => match stats::swarm_json(me, hex).await {
                    Some(body) => {
                        http::response("200 OK", Some("application/json"), body.as_bytes())
                    }
                    None => http::response(
                        "400 Bad Request",
                        None,
                        b"infoHash must be 2 to 80 hex digits",
                    ),
                },
            };
            Route::Respond(response, HttpRoute::Stats)
        }
        _ => not_found(),
    }
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
