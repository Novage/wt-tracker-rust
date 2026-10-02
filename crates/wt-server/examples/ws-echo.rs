//! WebSocket echo server on the native transport, for the Autobahn testsuite.
//!
//! `cargo run --release -p wt-server --example ws-echo -- PORT [CERT.pem KEY.pem]`

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = args.get(1).and_then(|p| p.parse().ok()).unwrap_or(9001);
    let tls = match (args.get(2), args.get(3)) {
        (Some(cert), Some(key)) => Some((cert.into(), key.into())),
        _ => None,
    };
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    println!(
        "ws-echo listening on {addr}{}",
        if tls.is_some() { " (TLS)" } else { "" }
    );
    if let Err(e) = wt_server::echo::run(addr, tls) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
