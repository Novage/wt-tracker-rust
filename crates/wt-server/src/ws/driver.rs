//! Connection driver for the native transport: one task per connection, readiness based
//! (`ready` + `try_read` / `try_write_vectored`). Reads go into one buffer shared by all
//! connections of the worker thread; a connection keeps bytes only while a frame (or a TLS
//! record) is incomplete. TLS uses rustls' unbuffered API so it shares the same buffers.
//! Spec §13.2.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rustls::ServerConfig;
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncodeTlsData, EncryptError, UnbufferedStatus,
};
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until, timeout};

use super::codec::{self, Data, Fragments, OpCode, Parsed, close};
use crate::worker::{Out, Pop};

/// Bytes read per readiness round before frames are processed.
const READ_ROUND: usize = 256 * 1024;
/// Frames written per vectored write.
const WRITE_BATCH: usize = 64;
/// Plaintext encrypted per TLS batch.
const TLS_BATCH: usize = 64 * 1024;
/// Close frame + flush + TLS close_notify must finish within this time.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
/// Per-connection buffers above this capacity are freed once empty.
const KEEP_CAPACITY: usize = 4096;

thread_local! {
    /// Plaintext being parsed: [partial frame of the connection][newly read bytes].
    static RX: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// Ciphertext being decrypted: [partial TLS record][newly read bytes].
    static TLS_IN: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// Plaintext frames to encrypt.
    static TX_PLAIN: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// Ciphertext to send.
    static TX_CIPHER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// What the driver does after a message.
pub(crate) enum Flow {
    Continue,
    /// Stop reading and hand the connection back ([`run`] returns it parked) to be resumed on
    /// another worker.
    Detach,
}

/// What a connection is attached to (the tracker worker, or the echo server).
pub(crate) trait Endpoint {
    /// A complete message; text is already UTF-8 validated. `Err(code)`: close with it.
    fn message(&self, text: bool, data: &[u8]) -> Result<Flow, u16>;
    /// Next queued outgoing frame.
    fn pop(&self) -> Pop;
    /// Notified when outgoing frames are queued.
    fn wake(&self) -> &Notify;
    /// Notified when the connection must close.
    fn stop(&self) -> &Notify;
}

pub(crate) struct Limits {
    pub max_payload: usize,
    /// No frame received for this long → close; pings every half of it. Zero: off.
    pub idle: Duration,
}

fn other<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

// ---- transport: plain TCP or TLS on shared buffers ----

pub(crate) enum Io {
    Plain(TcpStream),
    Tls(Box<TlsIo>),
}

pub(crate) struct TlsIo {
    tcp: TcpStream,
    conn: UnbufferedServerConnection,
    /// Incomplete TLS record (only while one is pending).
    pending_in: Vec<u8>,
    /// Ciphertext the socket did not take yet.
    pending_out: Vec<u8>,
}

enum Read {
    Data,
    WouldBlock,
    Eof,
}

fn encode_into(
    e: &mut EncodeTlsData<'_, ServerConnectionData>,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    let len = out.len();
    out.resize(len + 2048, 0);
    let written = match e.encode(&mut out[len..]) {
        Ok(n) => n,
        Err(EncodeError::InsufficientSize(s)) => {
            out.resize(len + s.required_size, 0);
            e.encode(&mut out[len..]).map_err(other)?
        }
        Err(e) => return Err(other(e)),
    };
    out.truncate(len + written);
    Ok(())
}

/// Reads whatever is available without waiting, appending to `buf` (up to about `limit`).
fn try_read_tcp(tcp: &TcpStream, buf: &mut Vec<u8>, limit: usize) -> io::Result<Read> {
    let mut any = false;
    loop {
        if buf.len() >= limit {
            return Ok(Read::Data);
        }
        buf.reserve((limit - buf.len()).clamp(4096, 64 * 1024));
        match tcp.try_read_buf(buf) {
            Ok(0) => return Ok(if any { Read::Data } else { Read::Eof }),
            Ok(_) => any = true,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(if any { Read::Data } else { Read::WouldBlock });
            }
            Err(e) => return Err(e),
        }
    }
}

/// Writes as much of `data` as the socket takes now; returns the bytes written.
fn try_write_tcp(tcp: &TcpStream, data: &[u8]) -> io::Result<usize> {
    let mut written = 0;
    while written < data.len() {
        match tcp.try_write(&data[written..]) {
            Ok(n) => written += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }
    Ok(written)
}

impl TlsIo {
    /// Runs the state machine over `incoming` (compacting it), decrypting into `plain`.
    /// Handshake output goes to `pending_out`. Returns whether the peer closed.
    fn process(&mut self, incoming: &mut Vec<u8>, plain: &mut Vec<u8>) -> io::Result<bool> {
        let mut used = incoming.len();
        let mut peer_closed = false;
        loop {
            let UnbufferedStatus { mut discard, state } =
                self.conn.process_tls_records(&mut incoming[..used]);
            let mut stop = false;
            match state.map_err(other)? {
                ConnectionState::ReadTraffic(mut traffic) => {
                    while let Some(record) = traffic.next_record() {
                        let record = record.map_err(other)?;
                        discard += record.discard;
                        plain.extend_from_slice(record.payload);
                    }
                }
                ConnectionState::EncodeTlsData(mut e) => {
                    encode_into(&mut e, &mut self.pending_out)?
                }
                // The bytes are in `pending_out`, which is always written before later output.
                ConnectionState::TransmitTlsData(t) => t.done(),
                ConnectionState::PeerClosed | ConnectionState::Closed => {
                    peer_closed = true;
                    stop = true;
                }
                // WriteTraffic / BlockedHandshake: nothing more to do with these bytes.
                _ => stop = true,
            }
            if discard > 0 {
                incoming.copy_within(discard..used, 0);
                used -= discard;
            }
            if stop {
                break;
            }
        }
        incoming.truncate(used);
        Ok(peer_closed)
    }

    fn try_read(&mut self, plain: &mut Vec<u8>, limit: usize) -> io::Result<Read> {
        TLS_IN.with(|cell| {
            let mut incoming = cell.borrow_mut();
            incoming.clear();
            incoming.extend_from_slice(&self.pending_in);
            let read = try_read_tcp(&self.tcp, &mut incoming, limit)?;
            let before = plain.len();
            // Always run the state machine: records decrypted earlier (e.g. app data that came
            // with the client's Finished during the handshake) wait inside the session.
            let peer_closed = self.process(&mut incoming, plain)?;
            self.pending_in.clear();
            self.pending_in.extend_from_slice(&incoming);
            if self.pending_in.is_empty() && self.pending_in.capacity() > KEEP_CAPACITY {
                self.pending_in = Vec::new();
            }
            Ok(match (plain.len() > before, peer_closed, read) {
                (true, _, _) => Read::Data,
                (false, true, _) | (false, _, Read::Eof) => Read::Eof,
                _ => Read::WouldBlock,
            })
        })
    }

    /// Encrypts `plain` and appends the ciphertext to `out`.
    fn encrypt(&mut self, plain: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        loop {
            let UnbufferedStatus { state, .. } = self.conn.process_tls_records(&mut []);
            match state.map_err(other)? {
                ConnectionState::WriteTraffic(mut w) => {
                    let len = out.len();
                    out.resize(len + plain.len() + plain.len() / 1024 + 512, 0);
                    let written = match w.encrypt(plain, &mut out[len..]) {
                        Ok(n) => n,
                        Err(EncryptError::InsufficientSize(s)) => {
                            out.resize(len + s.required_size, 0);
                            w.encrypt(plain, &mut out[len..]).map_err(other)?
                        }
                        Err(e) => return Err(other(e)),
                    };
                    out.truncate(len + written);
                    return Ok(());
                }
                ConnectionState::EncodeTlsData(mut e) => encode_into(&mut e, out)?,
                ConnectionState::TransmitTlsData(t) => t.done(),
                // Reported once; writing is still allowed afterwards.
                ConnectionState::PeerClosed => {}
                _ => return Err(io::ErrorKind::BrokenPipe.into()),
            }
        }
    }

    /// Writes `pending_out`; true when all of it was written.
    fn flush_pending(&mut self) -> io::Result<bool> {
        let n = try_write_tcp(&self.tcp, &self.pending_out)?;
        self.pending_out.drain(..n);
        if self.pending_out.is_empty() {
            if self.pending_out.capacity() > KEEP_CAPACITY {
                self.pending_out = Vec::new();
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Io {
    pub(crate) fn tcp(&self) -> &TcpStream {
        match self {
            Io::Plain(tcp) => tcp,
            Io::Tls(t) => &t.tcp,
        }
    }

    /// Server TLS handshake on the shared buffers.
    pub(crate) async fn accept_tls(tcp: TcpStream, config: Arc<ServerConfig>) -> io::Result<Io> {
        let conn = UnbufferedServerConnection::new(config).map_err(other)?;
        let mut tls = Box::new(TlsIo {
            tcp,
            conn,
            pending_in: Vec::new(),
            pending_out: Vec::new(),
        });
        let mut incoming = Vec::new();
        loop {
            let mut wait_read = false;
            {
                let UnbufferedStatus { discard, state } =
                    tls.conn.process_tls_records(&mut incoming);
                match state.map_err(other)? {
                    ConnectionState::EncodeTlsData(mut e) => {
                        encode_into(&mut e, &mut tls.pending_out)?
                    }
                    ConnectionState::TransmitTlsData(t) => t.done(),
                    ConnectionState::BlockedHandshake => wait_read = true,
                    // Handshake complete; app data (if any) stays queued in the connection.
                    ConnectionState::WriteTraffic(_) | ConnectionState::ReadTraffic(_) => {
                        incoming.drain(..discard);
                        tls.pending_in = incoming;
                        let mut io = Io::Tls(tls);
                        io.flush_all().await?;
                        return Ok(io);
                    }
                    _ => return Err(other("TLS connection closed during handshake")),
                }
                incoming.drain(..discard);
            }
            // Send handshake output before waiting for the peer.
            while !tls.flush_pending()? {
                tls.tcp.writable().await?;
            }
            if wait_read {
                tls.tcp.readable().await?;
                if let Read::Eof = try_read_tcp(&tls.tcp, &mut incoming, 64 * 1024)? {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
    }

    /// Appends available plaintext to `plain` without waiting.
    fn try_read(&mut self, plain: &mut Vec<u8>, limit: usize) -> io::Result<Read> {
        match self {
            Io::Plain(tcp) => try_read_tcp(tcp, plain, limit),
            Io::Tls(t) => t.try_read(plain, limit),
        }
    }

    fn has_pending_out(&self) -> bool {
        matches!(self, Io::Tls(t) if !t.pending_out.is_empty())
    }

    async fn flush_all(&mut self) -> io::Result<()> {
        if let Io::Tls(t) = self {
            while !t.flush_pending()? {
                t.tcp.writable().await?;
            }
        }
        Ok(())
    }

    /// Writes all of `data` as plaintext (HTTP responses before the upgrade).
    pub(crate) async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        match self {
            Io::Plain(tcp) => {
                let mut written = 0;
                while written < data.len() {
                    tcp.writable().await?;
                    written += try_write_tcp(tcp, &data[written..])?;
                }
                Ok(())
            }
            Io::Tls(t) => {
                let mut pending = std::mem::take(&mut t.pending_out);
                t.encrypt(data, &mut pending)?;
                t.pending_out = pending;
                self.flush_all().await
            }
        }
    }

    /// TLS close_notify (if TLS) and TCP write shutdown.
    pub(crate) async fn shutdown(&mut self) {
        if let Io::Tls(t) = self {
            let UnbufferedStatus { state, .. } = t.conn.process_tls_records(&mut []);
            if let Ok(ConnectionState::WriteTraffic(mut w)) = state {
                let len = t.pending_out.len();
                t.pending_out.resize(len + 64, 0);
                let n = w.queue_close_notify(&mut t.pending_out[len..]).unwrap_or(0);
                t.pending_out.truncate(len + n);
            }
            let _ = timeout(CLOSE_TIMEOUT, self.flush_all()).await;
        }
        let tcp = match self {
            Io::Plain(tcp) => tcp,
            Io::Tls(t) => &mut t.tcp,
        };
        let _ = tcp.shutdown().await;
    }

    /// Reads one HTTP request head through this transport. Returns it and the bytes after it.
    pub(crate) async fn read_head(&mut self) -> io::Result<(crate::http::Head, Vec<u8>)> {
        let mut buf = Vec::new();
        loop {
            match self.try_read(&mut buf, 8 * 1024)? {
                Read::Data => {
                    if let Some((head, len)) = crate::http::parse_head(&buf)? {
                        return Ok((head, buf.split_off(len)));
                    }
                }
                Read::WouldBlock => self.tcp().readable().await?,
                Read::Eof => return Err(io::ErrorKind::UnexpectedEof.into()),
            }
        }
    }
}

// ---- WebSocket connection ----

struct OutFrame {
    head: [u8; 10],
    head_len: u8,
    control: bool,
    payload: Bytes,
}

impl OutFrame {
    fn new(opcode: OpCode, payload: Bytes) -> Self {
        let (head, head_len) = codec::header(opcode, payload.len());
        Self {
            head,
            head_len: head_len as u8,
            control: opcode.is_control(),
            payload,
        }
    }

    fn len(&self) -> usize {
        self.head_len as usize + self.payload.len()
    }
}

fn close_frame(code: u16) -> OutFrame {
    OutFrame::new(OpCode::Close, Bytes::copy_from_slice(&code.to_be_bytes()))
}

struct Conn<'e, E: Endpoint> {
    io: Io,
    ep: &'e E,
    limits: Limits,
    /// Partial frame (only while one is incomplete).
    pending: Vec<u8>,
    fragments: Fragments,
    /// Frames taken from the endpoint queue; the first may be partly written (plain TCP).
    out: VecDeque<OutFrame>,
    out_offset: usize,
    /// Close with this code once the queue is written.
    closing: Option<u16>,
    last_rx: Instant,
    /// The endpoint asked to detach; the rest of the input is in `pending`.
    detached: bool,
}

impl<'e, E: Endpoint> Conn<'e, E> {
    fn push_control(&mut self, frame: OutFrame) {
        // Before queued data but after earlier control frames (pongs keep the order of their
        // pings), and never inside a partly written frame.
        let mut at = usize::from(self.out_offset > 0).min(self.out.len());
        while at < self.out.len() && self.out[at].control {
            at += 1;
        }
        self.out.insert(at, frame);
    }

    /// Moves queued frames from the endpoint into `out`.
    fn refill(&mut self) {
        while self.out.len() < WRITE_BATCH && self.closing.is_none() {
            match self.ep.pop() {
                Pop::Frame(Out::Text(m)) => self.out.push_back(OutFrame::new(OpCode::Text, m)),
                Pop::Frame(Out::Binary(m)) => self.out.push_back(OutFrame::new(OpCode::Binary, m)),
                Pop::Frame(Out::Close) | Pop::Gone => self.closing = Some(close::NORMAL),
                Pop::Empty => break,
            }
        }
    }

    /// Writes queued frames without waiting; true when everything was written.
    fn try_flush(&mut self) -> io::Result<bool> {
        loop {
            self.refill();
            match &mut self.io {
                Io::Plain(tcp) => {
                    if self.out.is_empty() {
                        return Ok(true);
                    }
                    let mut slices = [IoSlice::new(&[]); WRITE_BATCH * 2];
                    let mut n = 0;
                    let mut skip = self.out_offset;
                    for frame in self.out.iter().take(WRITE_BATCH) {
                        let head = &frame.head[..frame.head_len as usize];
                        for part in [head, &frame.payload[..]] {
                            if skip >= part.len() {
                                skip -= part.len();
                                continue;
                            }
                            slices[n] = IoSlice::new(&part[skip..]);
                            skip = 0;
                            n += 1;
                        }
                    }
                    let mut written = match tcp.try_write_vectored(&slices[..n]) {
                        Ok(w) => w,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                        Err(e) => return Err(e),
                    };
                    while written > 0 {
                        let left = self.out[0].len() - self.out_offset;
                        if written >= left {
                            written -= left;
                            self.out.pop_front();
                            self.out_offset = 0;
                        } else {
                            self.out_offset += written;
                            written = 0;
                        }
                    }
                }
                Io::Tls(t) => {
                    if !t.pending_out.is_empty() && !t.flush_pending()? {
                        return Ok(false);
                    }
                    if self.out.is_empty() {
                        return Ok(true);
                    }
                    let out = &mut self.out;
                    let blocked = TX_PLAIN.with(|p| {
                        TX_CIPHER.with(|c| -> io::Result<bool> {
                            let (mut plain, mut cipher) = (p.borrow_mut(), c.borrow_mut());
                            plain.clear();
                            while plain.len() < TLS_BATCH
                                && let Some(frame) = out.pop_front()
                            {
                                plain.extend_from_slice(&frame.head[..frame.head_len as usize]);
                                plain.extend_from_slice(&frame.payload);
                            }
                            cipher.clear();
                            t.encrypt(&plain, &mut cipher)?;
                            let n = try_write_tcp(&t.tcp, &cipher)?;
                            t.pending_out.extend_from_slice(&cipher[n..]);
                            Ok(n < cipher.len())
                        })
                    })?;
                    if blocked {
                        return Ok(false);
                    }
                }
            }
        }
    }

    /// Reads and handles available input. `Ok(false)`: end of stream.
    fn on_readable(&mut self) -> Result<bool, Option<u16>> {
        RX.with(|cell| {
            let mut rx = cell.borrow_mut();
            rx.clear();
            rx.extend_from_slice(&self.pending);
            let limit = READ_ROUND.max(self.limits.max_payload + 14);
            match self.io.try_read(&mut rx, limit) {
                Err(_) => Err(None),
                Ok(Read::Eof) => Ok(false),
                Ok(Read::WouldBlock) => Ok(true),
                Ok(Read::Data) => self.process(&mut rx).map(|_| true).map_err(Some),
            }
        })
    }

    /// Handles every complete frame in `rx`; keeps an incomplete tail in `pending`.
    fn process(&mut self, rx: &mut [u8]) -> Result<(), u16> {
        let mut pos = 0;
        let result = loop {
            match codec::parse_frame(&mut rx[pos..], self.limits.max_payload) {
                Parsed::Frame(frame, used) => {
                    let payload = pos + frame.payload.start..pos + frame.payload.end;
                    pos += used;
                    self.last_rx = Instant::now();
                    match self.on_frame(&frame, &rx[payload]) {
                        Ok(Flow::Continue) => {}
                        Ok(Flow::Detach) => {
                            self.detached = true;
                            break Ok(());
                        }
                        Err(code) => break Err(code),
                    }
                }
                Parsed::Incomplete(_) => break Ok(()),
                Parsed::Error(code) => break Err(code),
            }
        };
        self.pending.clear();
        if result.is_ok() && pos < rx.len() {
            self.pending.extend_from_slice(&rx[pos..]);
        } else if self.pending.capacity() > KEEP_CAPACITY {
            self.pending = Vec::new();
        }
        result
    }

    fn on_frame(&mut self, frame: &codec::Frame, payload: &[u8]) -> Result<Flow, u16> {
        match frame.opcode {
            OpCode::Ping => {
                self.push_control(OutFrame::new(OpCode::Pong, Bytes::copy_from_slice(payload)));
                Ok(Flow::Continue)
            }
            OpCode::Pong => Ok(Flow::Continue),
            // Answer with the peer's code (or 1000), then close.
            OpCode::Close => Err(codec::close_code(payload)?.unwrap_or(close::NORMAL)),
            _ => {
                let result = match self
                    .fragments
                    .push(frame, payload, self.limits.max_payload)?
                {
                    Data::Partial => Ok(Flow::Continue),
                    Data::Message(text, data) => {
                        if text && std::str::from_utf8(data).is_err() {
                            Err(close::INVALID_DATA)
                        } else {
                            self.ep.message(text, data)
                        }
                    }
                };
                self.fragments.reset();
                result
            }
        }
    }

    async fn finish(&mut self, code: Option<u16>) {
        if let Some(code) = code {
            // Keep a partly written frame (framing must stay valid), drop the rest.
            self.out.truncate(usize::from(self.out_offset > 0));
            self.closing = Some(code);
            self.out.push_back(close_frame(code));
            let flushed = timeout(CLOSE_TIMEOUT, async {
                loop {
                    match self.try_flush() {
                        Ok(true) => break,
                        Ok(false) => {
                            if self.io.tcp().writable().await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
            let _ = flushed.await;
        }
        self.io.shutdown().await;
    }
}

/// A connection detached from its endpoint, to be resumed on another worker thread: the socket
/// (deregistered from this thread's runtime), its TLS session, unparsed input and queued
/// control frames.
pub(crate) struct Parked {
    io: SendIo,
    pending: Vec<u8>,
    out: VecDeque<OutFrame>,
    out_offset: usize,
    last_rx: Instant,
}

enum SendIo {
    Plain(std::net::TcpStream),
    Tls(Box<SendTls>),
}

struct SendTls {
    tcp: std::net::TcpStream,
    conn: UnbufferedServerConnection,
    pending_in: Vec<u8>,
    pending_out: Vec<u8>,
}

impl Io {
    fn into_send(self) -> io::Result<SendIo> {
        Ok(match self {
            Io::Plain(tcp) => SendIo::Plain(tcp.into_std()?),
            Io::Tls(t) => {
                let TlsIo {
                    tcp,
                    conn,
                    pending_in,
                    pending_out,
                } = *t;
                SendIo::Tls(Box::new(SendTls {
                    tcp: tcp.into_std()?,
                    conn,
                    pending_in,
                    pending_out,
                }))
            }
        })
    }
}

impl SendIo {
    /// Registers the socket with the current thread's runtime.
    fn into_io(self) -> io::Result<Io> {
        Ok(match self {
            SendIo::Plain(tcp) => Io::Plain(TcpStream::from_std(tcp)?),
            SendIo::Tls(t) => {
                let SendTls {
                    tcp,
                    conn,
                    pending_in,
                    pending_out,
                } = *t;
                Io::Tls(Box::new(TlsIo {
                    tcp: TcpStream::from_std(tcp)?,
                    conn,
                    pending_in,
                    pending_out,
                }))
            }
        })
    }
}

enum End {
    Close(Option<u16>),
    Detach,
}

/// Runs a WebSocket connection until it closes, or until the endpoint detaches it: then it is
/// returned parked, untouched since the message that detached it. `pending`: bytes received
/// after the HTTP head.
pub(crate) async fn run<E: Endpoint>(
    io: Io,
    pending: Vec<u8>,
    limits: Limits,
    ep: &E,
) -> Option<Parked> {
    let conn = Conn {
        io,
        ep,
        limits,
        pending: Vec::new(),
        fragments: Fragments::default(),
        out: VecDeque::new(),
        out_offset: 0,
        closing: None,
        last_rx: Instant::now(),
        detached: false,
    };
    drive(conn, pending).await
}

/// Continues a parked connection on this thread: `first` is the message that detached it
/// (handled here first, then freed), then the rest of its input.
pub(crate) async fn resume<E: Endpoint>(
    parked: Parked,
    first: Bytes,
    limits: Limits,
    ep: &E,
) -> Option<Parked> {
    let Ok(io) = parked.io.into_io() else {
        return None;
    };
    let mut conn = Conn {
        io,
        ep,
        limits,
        pending: Vec::new(),
        fragments: Fragments::default(),
        out: parked.out,
        out_offset: parked.out_offset,
        closing: None,
        last_rx: parked.last_rx,
        detached: false,
    };
    let handled = ep.message(true, &first);
    drop(first);
    match handled {
        Ok(Flow::Continue) => {}
        Ok(Flow::Detach) => conn.detached = true,
        Err(code) => {
            conn.finish(Some(code)).await;
            return None;
        }
    }
    if conn.detached {
        conn.pending = parked.pending;
        return conn.park();
    }
    drive(conn, parked.pending).await
}

impl<E: Endpoint> Conn<'_, E> {
    fn park(self) -> Option<Parked> {
        Some(Parked {
            io: self.io.into_send().ok()?,
            pending: self.pending,
            out: self.out,
            out_offset: self.out_offset,
            last_rx: self.last_rx,
        })
    }
}

async fn drive<E: Endpoint>(mut conn: Conn<'_, E>, pending: Vec<u8>) -> Option<Parked> {
    let ep = conn.ep;
    let idle = conn.limits.idle;
    if !pending.is_empty() {
        let mut rx = pending;
        if let Err(code) = conn.process(&mut rx) {
            conn.finish(Some(code)).await;
            return None;
        }
        if conn.detached {
            return conn.park();
        }
    }

    let ping_every = idle / 2;
    let mut next_ping = Instant::now() + ping_every;
    let mut want_write = !conn.out.is_empty();
    let end = loop {
        if want_write || conn.io.has_pending_out() {
            match conn.try_flush() {
                Ok(done) => want_write = !done,
                Err(_) => break End::Close(None),
            }
        }
        if !want_write && let Some(code) = conn.closing {
            break End::Close(Some(code));
        }
        let interest = if want_write {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        tokio::select! {
            biased;
            _ = ep.stop().notified() => {
                // The queue ends with the close request: write it all, then close.
                conn.refill();
                if conn.closing.is_none() {
                    conn.closing = Some(close::NORMAL);
                }
                want_write = true;
            }
            _ = ep.wake().notified(), if !want_write => want_write = true,
            ready = conn.io.tcp().ready(interest) => {
                let Ok(ready) = ready else { break End::Close(None) };
                if ready.is_readable() {
                    match conn.on_readable() {
                        Ok(true) if conn.detached => break End::Detach,
                        Ok(true) => {}
                        Ok(false) => break End::Close(None),
                        Err(code) => break End::Close(code),
                    }
                    // Replies were probably queued while handling the input: send them now.
                    want_write = true;
                }
                if ready.is_writable() {
                    want_write = true;
                }
            }
            _ = sleep_until(next_ping), if !idle.is_zero() => {
                conn.push_control(OutFrame::new(OpCode::Ping, Bytes::new()));
                next_ping = Instant::now() + ping_every;
                want_write = true;
            }
            _ = sleep_until(conn.last_rx + idle), if !idle.is_zero() => {
                break End::Close(Some(close::NORMAL));
            }
        }
    };
    match end {
        End::Detach => conn.park(),
        End::Close(code) => {
            conn.finish(code).await;
            None
        }
    }
}
