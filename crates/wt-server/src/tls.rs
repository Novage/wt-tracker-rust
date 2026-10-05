//! wss:// listeners: rustls server config whose certificate can be reloaded from its PEM files
//! while the server runs (SIGHUP, spec §13.2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

/// What a reload did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reload {
    /// A new certificate is served; its `notAfter` in Unix seconds (`None`: not parsed).
    Reloaded { not_after: Option<i64> },
    /// The files did not change (or hold the certificate already served).
    Unchanged,
    /// The files could not be loaded; the previous certificate is still served.
    Failed(String),
}

/// The TLS side of a wss:// listener.
pub(crate) struct Tls {
    pub config: Arc<ServerConfig>,
    pub cert: Arc<CertStore>,
}

impl Tls {
    /// Loads the certificate chain and key; fails if they cannot be used together.
    pub(crate) fn open(cert_file: &Path, key_file: &Path) -> Result<Self, String> {
        let cert = CertStore::open(cert_file, key_file)?;
        Ok(Self {
            config: server_config(cert.clone())?,
            cert,
        })
    }
}

/// The served certificate and its `notAfter` (Unix seconds; `None` if not parsed).
struct Served {
    key: Arc<CertifiedKey>,
    not_after: Option<i64>,
}

/// The certificate of a listener, read from its PEM files, swapped on reload. rustls asks it for
/// the certificate on every full handshake, so the TLS config (and its session ticket keys)
/// stays the same across reloads. Only the [`Reloader`] thread swaps it.
pub(crate) struct CertStore {
    cert_file: PathBuf,
    key_file: PathBuf,
    served: RwLock<Served>,
    pub(crate) reloads_ok: AtomicU64,
    pub(crate) reloads_failed: AtomicU64,
}

impl std::fmt::Debug for CertStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertStore")
            .field("cert_file", &self.cert_file)
            .field("key_file", &self.key_file)
            .finish_non_exhaustive()
    }
}

impl CertStore {
    fn open(cert_file: &Path, key_file: &Path) -> Result<Arc<Self>, String> {
        let served = load(cert_file, key_file)?;
        Ok(Arc::new(Self {
            cert_file: cert_file.to_path_buf(),
            key_file: key_file.to_path_buf(),
            served: RwLock::new(served),
            reloads_ok: AtomicU64::new(0),
            reloads_failed: AtomicU64::new(0),
        }))
    }

    /// `notAfter` of the served certificate in Unix seconds (`None`: not parsed).
    pub(crate) fn not_after(&self) -> Option<i64> {
        self.served
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .not_after
    }

    /// Loads the files and serves them if they hold another certificate; counts the result.
    /// The comparison and the swap happen under one write lock. The files are read outside it
    /// (handshakes must not wait for file I/O), so the order of reloads, newer files last, comes
    /// from calling this only on the reload thread.
    fn reload(&self) -> Reload {
        match load(&self.cert_file, &self.key_file) {
            Ok(served) => {
                let mut current = self.served.write().unwrap_or_else(|e| e.into_inner());
                if current.key.cert == served.key.cert {
                    return Reload::Unchanged;
                }
                let not_after = served.not_after;
                *current = served;
                drop(current);
                self.reloads_ok.fetch_add(1, Relaxed);
                Reload::Reloaded { not_after }
            }
            Err(e) => {
                self.reloads_failed.fetch_add(1, Relaxed);
                Reload::Failed(e)
            }
        }
    }
}

impl ResolvesServerCert for CertStore {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(
            self.served
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .key
                .clone(),
        )
    }
}

/// The chain and key from PEM files, checked to belong together, and the leaf's `notAfter`.
fn load(cert_file: &Path, key_file: &Path) -> Result<Served, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_file)
        .and_then(|certs| certs.collect())
        .map_err(|e| format!("{}: {e}", cert_file.display()))?;
    if certs.is_empty() {
        return Err(format!("{}: no certificates found", cert_file.display()));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| format!("{}: {e}", key_file.display()))?;
    let not_after = not_after(&certs[0]);
    let key = CertifiedKey::from_der(certs, key, &rustls::crypto::ring::default_provider())
        .map_err(|e| format!("TLS certificate/key: {e}"))?;
    Ok(Served {
        key: Arc::new(key),
        not_after,
    })
}

/// rustls server config serving the store's certificate.
fn server_config(store: Arc<CertStore>) -> Result<Arc<ServerConfig>, String> {
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
            .with_cert_resolver(store);
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // Session resumption with stateless tickets (spec §13.2): a reconnecting client resumes its
    // session without the certificate exchange (~3.5 KB less egress per reconnect, no signature).
    // Ticket keys rotate every 6 h; tickets work on every worker and need no server memory. One
    // ticket per handshake: a client uses one per reconnect and gets a new one each time.
    config.ticketer = rustls::crypto::ring::Ticketer::new().map_err(|e| e.to_string())?;
    config.send_tls13_tickets = 1;
    Ok(Arc::new(config))
}

/// The `(listener, result)` of every reload of a request.
type Results = Vec<(String, Reload)>;

/// A reload request; `Some`: send the results back.
type Request = Option<mpsc::Sender<Results>>;

/// Reloads the certificates of the wss:// listeners on one thread (`wt-tls`), off the workers,
/// on request ([`Reloader::reload`], [`Reloader::request`]: SIGHUP). Being the only thread
/// that reloads, it installs newer files last. The thread ends when the `Reloader` is dropped.
pub(crate) struct Reloader(mpsc::Sender<Request>);

impl Reloader {
    pub(crate) fn spawn(certs: Vec<(String, Arc<CertStore>)>) -> std::io::Result<Self> {
        let (requests, rx) = mpsc::channel::<Request>();
        std::thread::Builder::new()
            .name("wt-tls".into())
            .spawn(move || {
                for reply in rx {
                    let results: Results = certs
                        .iter()
                        .map(|(name, cert)| {
                            let result = cert.reload();
                            log(name, &result);
                            (name.clone(), result)
                        })
                        .collect();
                    if let Some(reply) = reply {
                        let _ = reply.send(results);
                    }
                }
            })?;
        Ok(Self(requests))
    }

    /// Reloads every certificate from its files and waits for the results.
    pub(crate) fn reload(&self) -> Results {
        let (reply, results) = mpsc::channel();
        if self.0.send(Some(reply)).is_err() {
            stopped();
            return Vec::new();
        }
        results.recv().unwrap_or_else(|_| {
            stopped();
            Vec::new()
        })
    }

    /// Asks for a reload of every certificate without waiting (the thread logs the results).
    pub(crate) fn request(&self) {
        if self.0.send(None).is_err() {
            stopped();
        }
    }
}

/// The reload thread is gone (it panicked): reloads cannot happen until a restart.
fn stopped() {
    crate::event!(
        Error,
        "tls_reload_failed",
        listener = "*",
        error = "the certificate reload thread is not running; restart to reload certificates"
    );
}

/// Logs a reload of `listener` (spec §13.8).
fn log(listener: &str, result: &Reload) {
    match result {
        Reload::Reloaded { not_after } => crate::event!(
            Info,
            "tls_reloaded",
            listener = listener,
            not_after = not_after.map_or_else(|| "unknown".into(), crate::logging::timestamp)
        ),
        Reload::Unchanged => crate::event!(Info, "tls_unchanged", listener = listener),
        Reload::Failed(error) => crate::event!(
            Error,
            "tls_reload_failed",
            listener = listener,
            error = error
        ),
    }
}

/// `notAfter` of an X.509 certificate in Unix seconds: `Certificate` → `tbsCertificate` →
/// (`[0] version`), `serialNumber`, `signature`, `issuer`, `validity` → the second time.
fn not_after(cert: &[u8]) -> Option<i64> {
    let (_, cert, _) = der(cert)?;
    let (_, mut tbs, _) = der(cert)?;
    let (tag, _, rest) = der(tbs)?;
    if tag == 0xa0 {
        tbs = rest; // explicit version
    }
    for _ in 0..3 {
        tbs = der(tbs)?.2; // serial, signature algorithm, issuer
    }
    let (_, validity, _) = der(tbs)?;
    let (_, _, validity) = der(validity)?; // notBefore
    let (tag, time, _) = der(validity)?;
    let time = std::str::from_utf8(time).ok()?;
    let (year, rest) = match tag {
        0x17 => {
            // UTCTime: YYMMDDHHMMSSZ, 50..99 = 19xx.
            let yy: i64 = time.get(..2)?.parse().ok()?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, time.get(2..)?)
        }
        0x18 => (time.get(..4)?.parse().ok()?, time.get(4..)?),
        _ => return None,
    };
    let field = |i: usize| -> Option<i64> { rest.get(i..i + 2)?.parse().ok() };
    let (month, day) = (field(0)?, field(2)?);
    let (hour, minute, second) = (field(4)?, field(6)?, field(8)?);
    let days = crate::logging::days_from_civil(year, month as u32, day as u32);
    Some(days * 86_400 + hour * 3600 + minute * 60 + second)
}

/// One DER element: `(tag, content, rest)`.
fn der(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, data) = data.split_first()?;
    let (&first, mut data) = data.split_first()?;
    let len = if first < 0x80 {
        first as usize
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || data.len() < n {
            return None;
        }
        let len = data[..n].iter().fold(0usize, |l, &b| (l << 8) | b as usize);
        data = &data[n..];
        len
    };
    (data.len() >= len).then(|| (tag, &data[..len], &data[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert(not_after: (i32, u8, u8)) -> (rcgen::Certificate, rcgen::KeyPair) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
        let key = rcgen::KeyPair::generate().unwrap();
        (params.self_signed(&key).unwrap(), key)
    }

    #[test]
    fn not_after_reads_utc_and_generalized_time() {
        // 2027-01-01 (UTCTime) and 2051-06-30 (GeneralizedTime from 2050 on).
        let (a, _) = cert((2027, 1, 1));
        assert_eq!(not_after(a.der()), Some(1_798_761_600));
        let (b, _) = cert((2051, 6, 30));
        assert_eq!(not_after(b.der()), Some(2_571_696_000));
        assert_eq!(not_after(b"\x30\x03\x02\x01"), None);
    }

    /// Certificate files in a temporary directory.
    struct Files {
        dir: PathBuf,
        cert: PathBuf,
        key: PathBuf,
    }

    impl Files {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("wt-tls-{name}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
            Self { dir, cert, key }
        }

        fn cert(&self, c: &rcgen::Certificate) {
            std::fs::write(&self.cert, c.pem()).unwrap();
        }

        fn key(&self, k: &rcgen::KeyPair) {
            std::fs::write(&self.key, k.serialize_pem()).unwrap();
        }
    }

    impl Drop for Files {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn served(store: &CertStore) -> CertificateDer<'static> {
        store.served.read().unwrap().key.cert[0].clone()
    }

    fn counts(store: &CertStore) -> (u64, u64) {
        (
            store.reloads_ok.load(Relaxed),
            store.reloads_failed.load(Relaxed),
        )
    }

    #[test]
    fn reload_swaps_valid_files_and_keeps_the_old_certificate_otherwise() {
        let files = Files::new("swap");
        let ((a, ka), (b, kb)) = (cert((2027, 1, 1)), cert((2051, 6, 30)));
        files.cert(&a);
        files.key(&ka);
        let store = CertStore::open(&files.cert, &files.key).unwrap();
        assert_eq!(served(&store), *a.der());
        assert_eq!(store.reload(), Reload::Unchanged, "same certificate");

        files.cert(&b);
        files.key(&kb);
        assert_eq!(
            store.reload(),
            Reload::Reloaded {
                not_after: Some(2_571_696_000)
            }
        );
        assert_eq!(served(&store), *b.der());
        assert_eq!(store.not_after(), Some(2_571_696_000));

        // A new certificate with the old key: rejected and reported on every reload, b stays.
        files.cert(&a);
        for _ in 0..2 {
            assert!(matches!(store.reload(), Reload::Failed(_)));
        }
        assert_eq!(served(&store), *b.der());
        assert_eq!(counts(&store), (1, 2));
    }
}
