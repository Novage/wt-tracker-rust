//! Accepting connections: TLS, HTTP routes, WebSocket upgrade, and the per-connection reader
//! and writer tasks (spec §13.2).

use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use bytes::Bytes;
use fastwebsockets::{
    FragmentCollectorRead, Frame, OpCode, Payload, Role, WebSocket, WebSocketWrite,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;
use tokio::time::{Instant, sleep_until, timeout};
use wt_core::ConnId;

use crate::Transport;
use crate::http::{self, Prefixed};
use crate::stats;
use crate::worker::{Out, Pop, Worker};

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
    match me.shared.listeners[listener].tls.clone() {
        Some(acceptor) => {
            if let Ok(Ok(tls)) = timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                serve_stream(me, listener, tls).await;
            }
        }
        None => serve_stream(me, listener, stream).await,
    }
}

async fn serve_stream<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    me: Rc<Worker>,
    listener: usize,
    mut stream: S,
) {
    let Ok(Ok((head, rest))) = timeout(HANDSHAKE_TIMEOUT, http::read_head(&mut stream)).await
    else {
        return;
    };
    let info = &me.shared.listeners[listener];
    let path = head.path.split('?').next().unwrap_or("");

    if head.method == "GET"
        && head.upgrade_websocket
        && http::path_matches(&info.websockets.path, &head.path)
    {
        // Same check as uws-tracker (`webSocketsCount > maxConnections`): denied → TCP close.
        let max = info.websockets.max_connections;
        if max != 0 && info.web_sockets.load(Relaxed) > max {
            return;
        }
        if !http::origin_allowed(&me.shared.access, head.origin.as_deref()) {
            return;
        }
        if http::accept_upgrade(&mut stream, &head).await.is_err() {
            return;
        }
        info.web_sockets.fetch_add(1, Relaxed);
        match me.shared.transport {
            Transport::Fastwebsockets => {
                websocket(me.clone(), listener, Prefixed::new(rest, stream)).await
            }
            Transport::Sockudo => websocket_sockudo(me.clone(), listener, stream, rest).await,
        }
        me.shared.listeners[listener]
            .web_sockets
            .fetch_sub(1, Relaxed);
        return;
    }

    let _ = match (head.method.as_str(), path) {
        ("GET", "/") => match &me.shared.index_html {
            Some(html) => http::respond(&mut stream, "200 OK", Some("text/html"), html).await,
            None => http::not_found(&mut stream).await,
        },
        ("GET", "/stats.json") => {
            let body = stats::json(&me).await;
            http::respond(
                &mut stream,
                "200 OK",
                Some("application/json"),
                body.as_bytes(),
            )
            .await
        }
        _ => http::not_found(&mut stream).await,
    };
}

/// Owned bytes of a frame payload; no copy for unfragmented frames.
fn payload_bytes(payload: Payload<'_>) -> Bytes {
    match payload {
        Payload::Bytes(b) => b.freeze(),
        Payload::Owned(v) => Bytes::from(v),
        other => Bytes::copy_from_slice(&other),
    }
}

async fn websocket<S: AsyncRead + AsyncWrite + Unpin + 'static>(
    me: Rc<Worker>,
    listener: usize,
    stream: S,
) {
    let settings = &me.shared.listeners[listener].websockets;
    let idle = Duration::from_secs(settings.idle_timeout);
    let max_payload = settings.max_payload_length;

    let ws = WebSocket::after_handshake(stream, Role::Server);
    let (mut read, write) = ws.split(tokio::io::split);
    read.set_max_message_size(max_payload + 1);
    // Close frames are handled here: the writer sends the reply.
    read.set_auto_close(false);
    let mut read = FragmentCollectorRead::new(read);

    let (conn, notify, closed) = me.register();
    spawn_local(writer(me.clone(), conn, write, notify, idle));

    // Pongs the library owes the peer go through this connection's write queue.
    let mut obligated = |frame: Frame<'_>| {
        if frame.opcode == OpCode::Pong {
            me.deliver_local(conn, Out::Pong(Bytes::copy_from_slice(&frame.payload)));
        }
        std::future::ready(Ok::<(), std::io::Error>(()))
    };

    loop {
        let frame = tokio::select! {
            frame = read_with_idle(&mut read, &mut obligated, idle) => match frame {
                Some(frame) => frame,
                None => break,
            },
            _ = closed.notified() => break,
        };
        match frame.opcode {
            OpCode::Text | OpCode::Binary => {
                if frame.payload.len() > max_payload {
                    break;
                }
                if me
                    .handle_frame(conn, payload_bytes(frame.payload))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            OpCode::Close => break,
            _ => {}
        }
    }
    me.begin_close(conn);
}

/// A frame, or `None` on error, end of stream or `idle` without any frame.
async fn read_with_idle<'f, R, F, Fut>(
    read: &mut FragmentCollectorRead<R>,
    obligated: &mut F,
    idle: Duration,
) -> Option<Frame<'f>>
where
    R: AsyncRead + Unpin,
    F: FnMut(Frame<'f>) -> Fut,
    Fut: std::future::Future<Output = Result<(), std::io::Error>>,
{
    if idle.is_zero() {
        read.read_frame(obligated).await.ok()
    } else {
        timeout(idle, read.read_frame(obligated)).await.ok()?.ok()
    }
}

async fn writer<W: AsyncWrite + Unpin>(
    me: Rc<Worker>,
    conn: ConnId,
    mut ws: WebSocketWrite<W>,
    notify: Rc<tokio::sync::Notify>,
    idle: Duration,
) {
    // Pings keep browsers sending pongs within the idle timeout.
    let ping_every = (!idle.is_zero()).then(|| idle / 2);
    let mut next_ping = ping_every.map(|d| Instant::now() + d);

    'outer: loop {
        loop {
            let result = match me.pop(conn) {
                Pop::Frame(Out::Text(m)) => {
                    ws.write_frame(Frame::text(Payload::Borrowed(&m))).await
                }
                Pop::Frame(Out::Pong(p)) => {
                    ws.write_frame(Frame::pong(Payload::Borrowed(&p))).await
                }
                Pop::Frame(Out::Close) => {
                    let _ = ws.write_frame(Frame::close(1000, b"")).await;
                    break 'outer;
                }
                Pop::Empty => break,
                Pop::Gone => break 'outer,
            };
            if result.is_err() {
                break 'outer;
            }
        }
        if ws.flush().await.is_err() {
            break;
        }
        match next_ping {
            Some(at) => tokio::select! {
                _ = notify.notified() => {}
                _ = sleep_until(at) => {
                    let ping = Frame::new(true, OpCode::Ping, None, Payload::Borrowed(b""));
                    if ws.write_frame(ping).await.is_err() || ws.flush().await.is_err() {
                        break;
                    }
                    next_ping = ping_every.map(|d| Instant::now() + d);
                }
            },
            None => notify.notified().await,
        }
    }
    me.begin_close(conn);
    me.remove(conn);
}

/// sockudo-ws connection: one task reads frames and writes the queue (feed + one flush per
/// wake-up). Its heartbeat implements the idle timeout and pings.
async fn websocket_sockudo<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    me: Rc<Worker>,
    listener: usize,
    stream: S,
    leftover: Bytes,
) {
    use futures_util::{SinkExt, StreamExt};
    use sockudo_ws::{Message, Role, WebSocketStream};

    let settings = &me.shared.listeners[listener].websockets;
    let idle = settings.idle_timeout.min(u32::MAX as u64) as u32;
    let config = sockudo_ws::Config {
        max_message_size: settings.max_payload_length,
        idle_timeout: idle,
        auto_ping: idle > 0,
        ping_interval: (idle / 2).max(1),
        pong_timeout: (idle / 2).max(1),
        // Our own queue applies `maxBackpressure`; keep the library's small.
        max_backpressure: 64 * 1024,
        ..sockudo_ws::Config::default()
    };
    let leftover = (!leftover.is_empty()).then_some(leftover);
    let mut ws = WebSocketStream::from_raw_with_leftover(stream, Role::Server, config, leftover);
    let (conn, notify, closed) = me.register();

    'conn: loop {
        tokio::select! {
            biased;
            _ = closed.notified() => break,
            _ = notify.notified() => {
                loop {
                    match me.pop(conn) {
                        Pop::Frame(Out::Text(m)) => {
                            if ws.feed(Message::Text(m)).await.is_err() {
                                break 'conn;
                            }
                        }
                        // The library answers pings itself.
                        Pop::Frame(Out::Pong(_)) => {}
                        Pop::Frame(Out::Close) | Pop::Gone => break 'conn,
                        Pop::Empty => break,
                    }
                }
                if ws.flush().await.is_err() {
                    break;
                }
            }
            message = ws.next() => match message {
                Some(Ok(Message::Text(frame) | Message::Binary(frame))) => {
                    if me.handle_frame(conn, frame).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
            },
        }
    }
    me.begin_close(conn);
    let _ = ws.close(1000, "").await;
    me.remove(conn);
}
