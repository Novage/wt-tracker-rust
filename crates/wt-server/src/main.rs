//! `wt-tracker [config.json]`: like the JS tracker, reads the given file, or `./config.json` if it
//! exists, or uses defaults. SIGTERM / SIGINT: graceful shutdown (a second one stops at once).
//! SIGHUP: reload the TLS certificates.

use std::process::ExitCode;
use std::time::Duration;

use wt_server::{Config, event};

fn main() -> ExitCode {
    let (path, read) = match std::env::args().nth(1) {
        Some(path) => {
            let read = std::fs::read_to_string(&path);
            (path, read)
        }
        None => match std::fs::read_to_string("config.json") {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ("".into(), Ok("{}".into())),
            read => ("config.json".into(), read),
        },
    };
    let text = match read {
        Ok(text) => text,
        Err(e) => {
            event!(Error, "config_read_failed", path = path, error = e);
            return ExitCode::FAILURE;
        }
    };

    let config = match Config::from_json(&text) {
        Ok(config) => config,
        Err(e) => {
            event!(Error, "config_invalid", path = path, error = e);
            return ExitCode::FAILURE;
        }
    };
    // Validated by `from_json`.
    wt_server::logging::init(
        config
            .log_level()
            .unwrap_or(wt_server::logging::Level::Info),
    );
    for warning in config.warnings() {
        event!(Warn, "config_warning", message = warning);
    }
    let timeout = Duration::from_secs(config.shutdown_timeout);
    let placement = config.placement.clone().unwrap_or_else(|| "content".into());

    let server = match wt_server::start(config) {
        Ok(server) => server,
        Err(e) => {
            event!(Error, "start_failed", error = e);
            return ExitCode::FAILURE;
        }
    };
    for addr in server.local_addrs() {
        event!(Info, "listening", addr = addr);
    }
    if let Some(addr) = server.metrics_addr() {
        event!(Info, "metrics_listening", addr = addr);
    }
    event!(
        Info,
        "started",
        version = env!("CARGO_PKG_VERSION"),
        workers = server.workers(),
        placement = placement
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("signal runtime");
    runtime.block_on(async {
        reload_on_hangup(&server).await;
        event!(
            Info,
            "shutting_down",
            timeout_s = timeout.as_secs(),
            note = "closing connections; a second signal stops now"
        );
        let graceful = tokio::task::spawn_blocking(move || server.shutdown_gracefully(timeout));
        tokio::select! {
            _ = graceful => {
                event!(Info, "stopped");
                ExitCode::SUCCESS
            }
            _ = signal() => {
                event!(Warn, "stopped_without_waiting");
                // Not a return: dropping the runtime would wait for the graceful shutdown.
                std::process::exit(1)
            }
        }
    })
}

/// Until SIGINT / SIGTERM: on Unix, every SIGHUP reloads the TLS certificates (spec §13.6).
async fn reload_on_hangup(server: &wt_server::Server) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal as unix_signal};
        let mut hangup = unix_signal(SignalKind::hangup()).expect("SIGHUP handler");
        let stop = signal();
        tokio::pin!(stop);
        loop {
            tokio::select! {
                _ = &mut stop => return,
                _ = hangup.recv() => {
                    event!(Info, "reload_requested", signal = "SIGHUP");
                    server.reload_tls();
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = server;
        signal().await;
    }
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
