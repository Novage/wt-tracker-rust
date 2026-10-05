//! Graceful shutdown (spec §13.6): stop accepting, close every WebSocket with 1001 after its
//! queued messages, stop when all are closed or at the timeout; SIGTERM in the binary.

mod common;

use std::time::{Duration, Instant};

use common::*;
use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const H: &str = "hshutdown00000000001";

/// The close code the server sends next (skipping other messages).
async fn close_code(ws: &mut Ws) -> Option<CloseCode> {
    loop {
        match tokio::time::timeout(WAIT, ws.next()).await.ok()?? {
            Ok(Message::Close(frame)) => return frame.map(|f| f.code),
            Ok(_) => {}
            Err(_) => return None,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown_closes_every_connection_with_going_away() {
    let server = plain(4);
    let addr = server.local_addrs()[0];
    let mut clients = Vec::new();
    for i in 0..6 {
        let mut ws = connect(&server).await;
        send(&mut ws, &announce(H, &format!("p{i}"), 0)).await;
        assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, i + 1));
        clients.push(ws);
    }
    let started = Instant::now();
    let shutdown =
        tokio::task::spawn_blocking(move || server.shutdown_gracefully(Duration::from_secs(5)));
    for ws in &mut clients {
        assert_eq!(close_code(ws).await, Some(CloseCode::Away));
    }
    shutdown.await.unwrap();
    // Returned once the connections were closed, long before the timeout.
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zero_timeout_stops_without_waiting() {
    let server = plain(2);
    // A client that never reads or answers the close.
    let raw = Raw::connect(&server, &frame(true, 1, announce(H, "p", 0).as_bytes())).await;
    let started = Instant::now();
    tokio::task::spawn_blocking(move || server.shutdown_gracefully(Duration::ZERO))
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    drop(raw);
}

/// Kills the child `wt-tracker` if the test panics: a server left running keeps the test's
/// output pipes open, and the test run hangs instead of failing.
#[cfg(unix)]
struct KillOnPanic(u32);

#[cfg(unix)]
impl Drop for KillOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &self.0.to_string()])
                .status();
        }
    }
}

/// Sends `signal` (e.g. `TERM`) to process `pid`.
#[cfg(unix)]
fn kill(signal: &str, pid: u32) {
    let status = std::process::Command::new("kill")
        .args([&format!("-{signal}"), &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

/// The binary: SIGTERM → close 1001 → exit 0.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigterm_shuts_the_binary_down_gracefully() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let dir = std::env::temp_dir().join(format!("wt-sigterm-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.json");
    std::fs::write(
        &config,
        r#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":2,"shutdownTimeout":5}"#,
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_wt-tracker"))
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _kill_on_panic = KillOnPanic(child.id());
    // Log lines (logfmt on stderr): `... level=info event=listening addr=127.0.0.1:PORT`.
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut line = String::new();
    let addr = loop {
        line.clear();
        assert!(
            stderr.read_line(&mut line).unwrap() > 0,
            "no listening line"
        );
        if line.contains(" event=listening ") {
            break line.trim().rsplit_once("addr=").expect(&line).1.to_string();
        }
    };

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
        .await
        .unwrap();
    send(&mut ws, &announce(H, "p1", 0)).await;
    assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, 1));

    let started = Instant::now();
    kill("TERM", child.id());
    assert_eq!(close_code(&mut ws).await, Some(CloseCode::Away));
    let exit = tokio::task::spawn_blocking(move || {
        // The rest of the log, so the child never blocks on a full pipe.
        let rest: Vec<String> = stderr.lines().map_while(Result::ok).collect();
        (child.wait().unwrap(), rest)
    })
    .await
    .unwrap();
    let (exit, rest) = exit;
    assert!(
        rest.iter().any(|l| l.contains(" event=stopped")),
        "{rest:?}"
    );
    assert!(exit.success(), "{exit:?}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
}

/// SIGHUP reloads the certificate files of the wss:// listeners (spec §13.6): the process keeps
/// running and its connections stay open; a second SIGHUP without changes logs `tls_unchanged`.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sighup_reloads_the_certificate_and_keeps_running() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;

    let dir = std::env::temp_dir().join(format!("wt-sighup-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
    let write = |cert: &rcgen::CertifiedKey<rcgen::KeyPair>| {
        std::fs::write(&cert_file, cert.cert.pem()).unwrap();
        std::fs::write(&key_file, cert.signing_key.serialize_pem()).unwrap();
    };
    let (a, b) = (
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap(),
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap(),
    );
    write(&a);
    let mut roots = rustls::RootCertStore::empty();
    roots.add(a.cert.der().clone()).unwrap();
    roots.add(b.cert.der().clone()).unwrap();
    let config = dir.join("config.json");
    std::fs::write(
        &config,
        format!(
            r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}},{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":2}}"#,
            serde_json::to_string(&cert_file).unwrap(),
            serde_json::to_string(&key_file).unwrap()
        ),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_wt-tracker"))
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _kill_on_panic = KillOnPanic(child.id());
    // Log lines from a reader thread.
    let (lines_tx, lines) = mpsc::channel::<String>();
    let stderr = BufReader::new(child.stderr.take().unwrap());
    std::thread::spawn(move || {
        for line in stderr.lines().map_while(Result::ok) {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });
    let next = |event: &str| -> String {
        loop {
            let line = lines
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| panic!("no {event} line"));
            if line.contains(&format!(" event={event}")) {
                return line;
            }
        }
    };
    // One line per listener, in config order: plain ws://, then wss://.
    let mut listening = (0..2).map(|_| {
        let line = next("listening");
        line.rsplit_once("addr=").unwrap().1.to_string()
    });
    let (addr, wss) = (listening.next().unwrap(), listening.next().unwrap());
    let wss: std::net::SocketAddr = wss.parse().unwrap();
    // The certificate the wss:// listener serves to a new client (a fresh client config: a
    // resumed session would report the certificate of its first handshake).
    let served = || tokio::task::block_in_place(|| tls_connect(wss, &tls_client(&roots)).1);
    assert_eq!(served(), a.cert.der().to_vec());

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
        .await
        .unwrap();
    send(&mut ws, &announce(H, "p1", 0)).await;
    assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, 1));

    write(&b);
    kill("HUP", child.id());
    let reloaded = tokio::task::block_in_place(|| next("tls_reloaded"));
    // The new certificate's expiry, parsed.
    assert!(
        reloaded.contains(" not_after=") && !reloaded.contains("not_after=unknown"),
        "{reloaded}"
    );
    // The listener really serves the new certificate.
    assert_eq!(served(), b.cert.der().to_vec());
    kill("HUP", child.id());
    tokio::task::block_in_place(|| next("tls_unchanged"));

    // Still the same process and connection.
    send(&mut ws, &announce(H, "p1", 0)).await;
    assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, 1));
    assert!(child.try_wait().unwrap().is_none());

    kill("TERM", child.id());
    let exit = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
    assert!(exit.success(), "{exit:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A SIGHUP while the binary is still starting does not kill it (spec §13.6): the handler is
/// installed before the configuration is read, and the signal becomes a reload once it runs.
/// The configuration comes through a FIFO, so the process is provably mid-startup (blocked
/// reading it) when the signal arrives.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sighup_during_startup_does_not_kill_the_process() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};

    let dir = std::env::temp_dir().join(format!("wt-early-hup-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let fifo = dir.join("config.json");
    let _ = std::fs::remove_file(&fifo);
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_wt-tracker"))
        .arg(&fifo)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _kill_on_panic = KillOnPanic(child.id());
    // Wait until the child opens the FIFO to read its configuration, which it does after
    // installing the SIGHUP handler: until then a non-blocking open for writing fails (ENXIO).
    // No fixed sleep, so a slow start (cold binary, loaded CI) cannot make the test fail.
    let deadline = Instant::now() + WAIT;
    let mut writer = loop {
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo)
        {
            Ok(writer) => break writer,
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) => {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "the process exited before reading its configuration"
                );
                assert!(
                    Instant::now() < deadline,
                    "the configuration was never opened"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("{}: {e}", fifo.display()),
        }
    };
    // The child now blocks reading the configuration (no data yet, a writer is open): it is
    // still starting when the signal arrives.
    kill("HUP", child.id());
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        child.try_wait().unwrap().is_none(),
        "SIGHUP during startup killed the process"
    );

    writer
        .write_all(br#"{"servers":[{"server":{"host":"127.0.0.1","port":0}}],"workers":1}"#)
        .unwrap();
    drop(writer);
    let stderr = BufReader::new(child.stderr.take().unwrap());
    let lines = tokio::task::spawn_blocking(move || {
        stderr
            .lines()
            .map_while(Result::ok)
            .take_while(|l| !l.contains(" event=reload_requested"))
            .collect::<Vec<_>>()
    });
    let lines = tokio::time::timeout(WAIT, lines)
        .await
        .expect("the early SIGHUP was not handled as a reload")
        .unwrap();
    assert!(
        lines.iter().any(|l| l.contains(" event=started")),
        "{lines:?}"
    );
    assert!(child.try_wait().unwrap().is_none());
    kill("TERM", child.id());
    assert!(child.wait().unwrap().success());
    std::fs::remove_dir_all(&dir).unwrap();
}
