//! wss:// listeners: rustls server config whose certificate can be reloaded from its PEM files
//! while the server runs (SIGHUP or a file change, spec §13.2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

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

/// What starts a reload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trigger {
    /// SIGHUP: always reads the files.
    Signal,
    /// The file watcher: reads the files only after they changed.
    File,
}

impl Trigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Signal => "signal",
            Self::File => "file",
        }
    }
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

/// Modification time and length of both files (following symlinks): a change starts a reload.
type Stamp = Option<[(SystemTime, u64); 2]>;

/// The served certificate and its `notAfter` (Unix seconds; `None` if not parsed).
struct Served {
    key: Arc<CertifiedKey>,
    not_after: Option<i64>,
}

/// The certificate of a listener, read from its PEM files, swapped on reload. rustls asks it for
/// the certificate on every full handshake, so the TLS config (and its session ticket keys)
/// stays the same across reloads.
pub(crate) struct CertStore {
    cert_file: PathBuf,
    key_file: PathBuf,
    served: RwLock<Served>,
    seen: Mutex<Stamp>,
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
        let seen = stamp(cert_file, key_file);
        let served = load(cert_file, key_file)?;
        Ok(Arc::new(Self {
            cert_file: cert_file.to_path_buf(),
            key_file: key_file.to_path_buf(),
            served: RwLock::new(served),
            seen: Mutex::new(seen),
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

    /// Reloads if the files changed since the last attempt (any case for [`Trigger::Signal`]).
    /// A missing file waits for the next check of the watcher (a renewal may be replacing it).
    ///
    /// Call it only from the [`Reloader`] thread: comparing with the served certificate and
    /// installing the new one are separate steps, safe because no other reload runs meanwhile.
    fn reload(&self, trigger: Trigger) -> Reload {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let now = stamp(&self.cert_file, &self.key_file);
        if trigger == Trigger::File && (now.is_none() || *seen == now) {
            return Reload::Unchanged;
        }
        *seen = now;
        match load(&self.cert_file, &self.key_file) {
            Ok(served) => {
                let unchanged = {
                    let current = self.served.read().unwrap_or_else(|e| e.into_inner());
                    current.key.cert == served.key.cert
                };
                if unchanged {
                    return Reload::Unchanged;
                }
                let not_after = served.not_after;
                *self.served.write().unwrap_or_else(|e| e.into_inner()) = served;
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

fn stamp(cert_file: &Path, key_file: &Path) -> Stamp {
    let one = |path: &Path| {
        let meta = std::fs::metadata(path).ok()?;
        Some((meta.modified().ok()?, meta.len()))
    };
    Some([one(cert_file)?, one(key_file)?])
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

/// Reloads the certificates of the wss:// listeners on one thread (`wt-tls`), off the workers:
/// on request ([`Reloader::reload`], SIGHUP) and, every `interval` (zero: never), when their
/// files changed. Being the only thread that reloads, it installs newer files last. The thread
/// ends when the `Reloader` is dropped.
pub(crate) struct Reloader(mpsc::Sender<mpsc::Sender<Results>>);

impl Reloader {
    pub(crate) fn spawn(
        certs: Vec<(String, Arc<CertStore>)>,
        interval: Duration,
    ) -> std::io::Result<Self> {
        let (requests, rx) = mpsc::channel::<mpsc::Sender<Results>>();
        std::thread::Builder::new()
            .name("wt-tls".into())
            .spawn(move || {
                let reload_all = |trigger: Trigger| -> Results {
                    certs
                        .iter()
                        .map(|(name, cert)| {
                            let result = cert.reload(trigger);
                            log(name, &result, trigger);
                            (name.clone(), result)
                        })
                        .collect()
                };
                let mut check = Instant::now() + interval;
                loop {
                    let request = if interval.is_zero() {
                        rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
                    } else {
                        rx.recv_timeout(check.saturating_duration_since(Instant::now()))
                    };
                    match request {
                        Ok(reply) => {
                            let _ = reply.send(reload_all(Trigger::Signal));
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            reload_all(Trigger::File);
                            check = Instant::now() + interval;
                        }
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
            })?;
        Ok(Self(requests))
    }

    /// Reloads every certificate from its files and waits for the results.
    pub(crate) fn reload(&self) -> Results {
        let (reply, results) = mpsc::channel();
        if self.0.send(reply).is_err() {
            return Vec::new();
        }
        results.recv().unwrap_or_default()
    }
}

/// Logs a reload of `listener` (spec §13.8).
fn log(listener: &str, result: &Reload, trigger: Trigger) {
    match result {
        Reload::Reloaded { not_after } => crate::event!(
            Info,
            "tls_reloaded",
            listener = listener,
            not_after = not_after.map_or_else(|| "unknown".into(), crate::logging::timestamp),
            trigger = trigger.as_str()
        ),
        Reload::Unchanged if trigger == Trigger::Signal => {
            crate::event!(Info, "tls_unchanged", listener = listener)
        }
        Reload::Unchanged => {}
        Reload::Failed(error) => crate::event!(
            Error,
            "tls_reload_failed",
            listener = listener,
            error = error,
            trigger = trigger.as_str()
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

    #[test]
    fn reload_swaps_valid_files_and_keeps_the_old_certificate_otherwise() {
        let dir = std::env::temp_dir().join(format!("wt-tls-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
        let write = |c: &rcgen::Certificate, k: &rcgen::KeyPair| {
            std::fs::write(&cert_file, c.pem()).unwrap();
            std::fs::write(&key_file, k.serialize_pem()).unwrap();
        };
        let (a, ka) = cert((2027, 1, 1));
        write(&a, &ka);
        let store = CertStore::open(&cert_file, &key_file).unwrap();
        let served = |store: &CertStore| store.served.read().unwrap().key.cert[0].clone();
        assert_eq!(served(&store), *a.der());
        assert_eq!(store.reload(Trigger::File), Reload::Unchanged);
        assert_eq!(
            store.reload(Trigger::Signal),
            Reload::Unchanged,
            "same certificate"
        );

        let (b, kb) = cert((2051, 6, 30));
        write(&b, &kb);
        assert_eq!(
            store.reload(Trigger::File),
            Reload::Reloaded {
                not_after: Some(2_571_696_000)
            }
        );
        assert_eq!(served(&store), *b.der());
        assert_eq!(store.not_after(), Some(2_571_696_000));

        // A new certificate with the old key: rejected, b stays.
        std::fs::write(&cert_file, a.pem()).unwrap();
        assert!(matches!(store.reload(Trigger::File), Reload::Failed(_)));
        assert_eq!(served(&store), *b.der());
        assert_eq!(
            store.reload(Trigger::File),
            Reload::Unchanged,
            "tried once per change"
        );
        assert_eq!(
            (
                store.reloads_ok.load(Relaxed),
                store.reloads_failed.load(Relaxed)
            ),
            (1, 1)
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
