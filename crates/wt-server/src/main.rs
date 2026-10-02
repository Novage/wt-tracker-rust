//! `wt-tracker [config.json]`: like the JS tracker, reads the given file, or `./config.json` if it
//! exists, or uses defaults. SIGTERM / SIGINT: graceful shutdown (a second one stops at once).

use std::process::ExitCode;
use std::time::Duration;

use wt_server::Config;

fn main() -> ExitCode {
    let text = match std::env::args().nth(1) {
        Some(path) => match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) => {
                eprintln!("failed to read configuration file {path}: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => match std::fs::read_to_string("config.json") {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => "{}".into(),
            Err(e) => {
                eprintln!("failed to read configuration file: {e}");
                return ExitCode::FAILURE;
            }
        },
    };

    let config = match Config::from_json(&text) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    for warning in config.warnings() {
        eprintln!("warning: {warning}");
    }
    let timeout = Duration::from_secs(config.shutdown_timeout);

    let server = match wt_server::start(config) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("failed to start the server: {e}");
            return ExitCode::FAILURE;
        }
    };
    for addr in server.local_addrs() {
        println!("listening {addr}");
    }
    println!("{} workers", server.workers());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("signal runtime");
    runtime.block_on(async {
        signal().await;
        println!(
            "shutting down: closing connections (up to {timeout:?}; a second signal stops now)"
        );
        let graceful = tokio::task::spawn_blocking(move || server.shutdown_gracefully(timeout));
        tokio::select! {
            _ = graceful => {
                println!("stopped");
                ExitCode::SUCCESS
            }
            _ = signal() => {
                eprintln!("stopped without waiting for connections");
                // Not a return: dropping the runtime would wait for the graceful shutdown.
                std::process::exit(1)
            }
        }
    })
}

/// SIGINT (Ctrl-C) or, on Unix, SIGTERM (docker stop, systemd).
async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
