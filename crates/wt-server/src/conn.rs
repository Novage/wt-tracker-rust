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
/// `Origin` values up to this length are kept as sent (spec §13.7): every valid origin fits
/// (`https://` + a 253-byte host + `:65535` is 267 bytes, other schemes a little more).
const MAX_ORIGIN: usize = 512;
/// A longer value keeps this many bytes (cut at a character boundary) and a marker.
const LONG_ORIGIN_PREFIX: usize = 64;

pub(crate) async fn accept_loop(me: Rc<Worker>, listener: usize, socket: TcpListener) {
    loop {
        match socket.accept().await {
            Ok((stream, peer)) => {
                spawn_local(serve(me.clone(), listener, stream, peer));
            }
            Err(e) => accept_failed(&me.shared.listeners[listener].name, e).await,
        }
    }
}

/// A failed `accept`. A connection the client ended before it was accepted (ECONNABORTED,
/// ECONNRESET) concerns only that connection: a debug event, and the next accept at once. Other
/// errors (e.g. EMFILE) are logged as errors, and the loop waits 10 ms instead of spinning.
pub(crate) async fn accept_failed(listener: &str, e: std::io::Error) {
    if client_aborted(&e) {
        crate::event!(Debug, "accept_aborted", listener = listener, error = e);
    } else {
        crate::event_limited!(Error, "accept_failed", listener = listener, error = e);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn client_aborted(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset
    )
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
            let endpoint = TrackerEndpoint::new(&me, false, origin_key(head.origin.as_deref()));
            let result = driver::run(io, rest, limits(&me, listener), deflate, &endpoint).await;
            let conn = endpoint.conn;
            match (result, endpoint.moving.take()) {
                // Moved before anything was applied: no shard holds the connection.
                (Ok(parked), Some((worker, first))) => {
                    let origin = me.remove(conn).unwrap_or_default();
                    let adopt = Adopt {
                        listener,
                        first,
                        parked,
                        peer,
                        opened,
                        origin,
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
    /// The request's `Origin`, counted on the worker that holds the connection.
    origin: Box<str>,
}

/// Continues a connection that moved to this worker.
pub(crate) async fn adopted(me: Rc<Worker>, adopt: Adopt) {
    let limits = limits(&me, adopt.listener);
    let Adopt {
        listener,
        first,
        parked,
        peer,
        opened,
        origin,
    } = adopt;
    let endpoint = TrackerEndpoint::new(&me, true, origin);
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
            Route::Respond(stats::response(me, query).await, HttpRoute::Stats)
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

/// The origin a connection is counted under: `""` without an `Origin` header; a value over
/// [`MAX_ORIGIN`] bytes as its first [`LONG_ORIGIN_PREFIX`] bytes, `…` and its length.
fn origin_key(origin: Option<&str>) -> Box<str> {
    let origin = origin.unwrap_or("");
    if origin.len() <= MAX_ORIGIN {
        return origin.into();
    }
    // Still names the site; a real origin has no space, so the marker cannot collide with one.
    let cut = origin.floor_char_boundary(LONG_ORIGIN_PREFIX);
    format!("{}… ({} bytes)", &origin[..cut], origin.len()).into()
}

impl TrackerEndpoint {
    fn new(me: &Rc<Worker>, placed: bool, origin: Box<str>) -> Self {
        let (conn, notify, closed) = me.register(placed, origin);
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

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    #[test]
    fn only_client_aborts_are_not_accept_errors() {
        assert!(super::client_aborted(&Error::from(
            ErrorKind::ConnectionAborted
        )));
        assert!(super::client_aborted(&Error::from(
            ErrorKind::ConnectionReset
        )));
        // EMFILE (24 on Linux and macOS) and others stay errors.
        assert!(!super::client_aborted(&Error::from_raw_os_error(24)));
        assert!(!super::client_aborted(&Error::from(ErrorKind::OutOfMemory)));
    }

    #[test]
    fn origins_are_kept_up_to_512_bytes_and_long_ones_shortened() {
        use super::origin_key;
        let valid = format!("https://{}:65535", "h".repeat(253)); // the longest https origin
        assert_eq!(&*origin_key(Some(&valid)), valid);
        let max = "é".repeat(256); // 512 bytes: kept; one byte more is shortened
        assert_eq!(&*origin_key(Some(&max)), max);
        assert!(origin_key(Some(&format!("{max}a"))).ends_with("… (513 bytes)"));
        let long = format!("https://{}{}", "a".repeat(55), "é".repeat(300)); // é at bytes 63..65
        let key = origin_key(Some(&long));
        assert_eq!(&*key, format!("https://{}… (663 bytes)", "a".repeat(55)));
    }
}
