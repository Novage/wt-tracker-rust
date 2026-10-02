//! `wt-tracker [config.json]`: like the JS tracker, reads the given file, or `./config.json` if it
//! exists, or uses defaults.

use std::process::ExitCode;

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
    let _ = runtime.block_on(tokio::signal::ctrl_c());
    server.shutdown();
    ExitCode::SUCCESS
}
