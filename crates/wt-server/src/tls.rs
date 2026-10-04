//! wss:// listeners: rustls server config from PEM files.

use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// rustls server config from PEM files.
pub fn server_config(cert_file: &Path, key_file: &Path) -> Result<Arc<ServerConfig>, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_file)
        .and_then(|certs| certs.collect())
        .map_err(|e| format!("{}: {e}", cert_file.display()))?;
    if certs.is_empty() {
        return Err(format!("{}: no certificates found", cert_file.display()));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| format!("{}: {e}", key_file.display()))?;
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("TLS certificate/key: {e}"))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // Session resumption with stateless tickets (spec §13.2): a reconnecting client resumes its
    // session without the certificate exchange (~3.5 KB less egress per reconnect, no signature).
    // Ticket keys rotate every 6 h; tickets work on every worker and need no server memory. One
    // ticket per handshake: a client uses one per reconnect and gets a new one each time.
    config.ticketer = rustls::crypto::ring::Ticketer::new().map_err(|e| e.to_string())?;
    config.send_tls13_tickets = 1;
    Ok(Arc::new(config))
}
