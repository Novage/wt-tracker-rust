//! Accepting connections: TLS (on shared buffers), the HTTP request head, routes, and the
//! WebSocket upgrade into the connection driver (spec §13.2).

use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;
use tokio::time::timeout;
use wt_core::ConnId;

use crate::http;
use crate::stats;
use crate::worker::{Pop, Worker};
use crate::ws::codec::close;
use crate::ws::driver::{self, Endpoint, Io, Limits};

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
        Route::Upgrade(response) => {
            if io.write_all(response.as_bytes()).await.is_err() {
                return;
            }
            let limits = Limits {
                max_payload: info.websockets.max_payload_length,
                idle: Duration::from_secs(info.websockets.idle_timeout),
            };
            info.web_sockets.fetch_add(1, Relaxed);
            let (conn, notify, closed) = me.register();
            let endpoint = TrackerEndpoint {
                me: me.clone(),
                conn,
                notify,
                closed,
            };
            driver::run(io, rest, limits, &endpoint).await;
            me.begin_close(conn);
            me.remove(conn);
            info.web_sockets.fetch_sub(1, Relaxed);
        }
        Route::Respond(bytes) => {
            let _ = io.write_all(&bytes).await;
            io.shutdown().await;
        }
        Route::Drop => {}
    }
}

/// What to do with a request head (spec §13.2).
enum Route {
    /// Upgrade with this `101` response.
    Upgrade(String),
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
        return match http::upgrade_response(head) {
            Ok(response) => Route::Upgrade(response),
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
}

impl Endpoint for TrackerEndpoint {
    fn message(&self, _text: bool, data: &[u8]) -> Result<(), u16> {
        self.me
            .handle_message(self.conn, data)
            .map_err(|_| close::POLICY)
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
