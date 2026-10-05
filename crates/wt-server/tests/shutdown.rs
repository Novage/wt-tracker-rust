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
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
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
    write(&rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap());
    let config = dir.join("config.json");
    std::fs::write(
        &config,
        format!(
            r#"{{"servers":[{{"server":{{"host":"127.0.0.1","port":0}}}},{{"server":{{"host":"127.0.0.1","port":0,"cert_file_name":{},"key_file_name":{}}}}}],"workers":2,"tlsReloadInterval":0}}"#,
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
    let line = next("listening");
    let addr = line.rsplit_once("addr=").unwrap().1.to_string();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
        .await
        .unwrap();
    send(&mut ws, &announce(H, "p1", 0)).await;
    assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, 1));

    write(&rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap());
    let hup = |pid: u32| {
        let status = Command::new("kill")
            .args(["-HUP", &pid.to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    };
    hup(child.id());
    let reloaded = tokio::task::block_in_place(|| next("tls_reloaded"));
    assert!(reloaded.contains("trigger=signal"), "{reloaded}");
    hup(child.id());
    tokio::task::block_in_place(|| next("tls_unchanged"));

    // Still the same process and connection.
    send(&mut ws, &announce(H, "p1", 0)).await;
    assert_eq!(recv(&mut ws).await.unwrap(), reply(H, 0, 1));
    assert!(child.try_wait().unwrap().is_none());

    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();
    let exit = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
    assert!(exit.success(), "{exit:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}
