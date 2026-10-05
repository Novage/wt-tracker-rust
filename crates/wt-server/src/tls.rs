//! wss:// listeners: rustls server config whose certificate can be reloaded from its PEM files
//! while the server runs (SIGHUP or a file change, spec §13.2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, RwLock};
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
/// stays the same across reloads. Only the [`Reloader`] thread swaps it.
pub(crate) struct CertStore {
    cert_file: PathBuf,
    key_file: PathBuf,
    served: RwLock<Served>,
    /// The files' stamp when they were first loaded (the watcher starts from it).
    opened: Stamp,
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
        let opened = stamp(cert_file, key_file);
        let served = load(cert_file, key_file)?;
        Ok(Arc::new(Self {
            cert_file: cert_file.to_path_buf(),
            key_file: key_file.to_path_buf(),
            served: RwLock::new(served),
            opened,
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

    fn stamp(&self) -> Stamp {
        stamp(&self.cert_file, &self.key_file)
    }

    /// Loads the files and serves them if they hold another certificate. Not counted or logged
    /// ([`Watch::run`] decides). The comparison and the swap are separate steps: only the
    /// reload thread calls this.
    fn swap_in(&self) -> Reload {
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
                Reload::Reloaded { not_after }
            }
            Err(e) => Reload::Failed(e),
        }
    }
}

/// The reload thread's state for one certificate.
struct Watch {
    /// Stamp of the files last loaded (or found to hold the served certificate).
    seen: Stamp,
    /// Stamp of files that failed to load, and whether that failure was reported.
    failed: Option<(Stamp, bool)>,
}

impl Watch {
    fn new(cert: &CertStore) -> Self {
        Self {
            seen: cert.opened,
            failed: None,
        }
    }

    /// One reload of `cert`, counted in its metrics; the result to log, or `None` when there is
    /// nothing to report.
    ///
    /// - [`Trigger::Signal`] always reads the files and reports a failure at once.
    /// - [`Trigger::File`] reads them when their stamp changed, or again on every check after
    ///   a failure (a read may have caught a file mid-write, and the finished file can have the
    ///   same stamp). A failure is reported only when the same files fail on two checks in a
    ///   row, so a renewal caught between replacing the key and the certificate is not one. A
    ///   missing file waits for the next check (a renewal may be replacing it).
    fn run(&mut self, cert: &CertStore, trigger: Trigger) -> Option<Reload> {
        let now = cert.stamp();
        let failed_before = matches!(self.failed, Some((stamp, _)) if stamp == now);
        if trigger == Trigger::File && (now.is_none() || (now == self.seen && !failed_before)) {
            return None;
        }
        let result = cert.swap_in();
        match &result {
            Reload::Failed(_) => {
                let reported = failed_before && matches!(self.failed, Some((_, true)));
                let report = !reported && (trigger == Trigger::Signal || failed_before);
                self.failed = Some((now, reported || report));
                if !report {
                    return None;
                }
                cert.reloads_failed.fetch_add(1, Relaxed);
            }
            Reload::Reloaded { .. } => {
                (self.seen, self.failed) = (now, None);
                cert.reloads_ok.fetch_add(1, Relaxed);
            }
            Reload::Unchanged => (self.seen, self.failed) = (now, None),
        }
        Some(result)
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

/// A reload request; `Some`: send the results back.
type Request = Option<mpsc::Sender<Results>>;

/// Reloads the certificates of the wss:// listeners on one thread (`wt-tls`), off the workers:
/// on request ([`Reloader::reload`], [`Reloader::request`]: SIGHUP) and, every `interval`
/// (zero: never), when their files changed. Being the only thread that reloads, it installs
/// newer files last. The thread ends when the `Reloader` is dropped.
pub(crate) struct Reloader(mpsc::Sender<Request>);

impl Reloader {
    pub(crate) fn spawn(
        certs: Vec<(String, Arc<CertStore>)>,
        interval: Duration,
    ) -> std::io::Result<Self> {
        let (requests, rx) = mpsc::channel::<Request>();
        std::thread::Builder::new()
            .name("wt-tls".into())
            .spawn(move || {
                let mut watches: Vec<Watch> = certs.iter().map(|(_, c)| Watch::new(c)).collect();
                let mut reload_all = |trigger: Trigger| -> Results {
                    certs
                        .iter()
                        .zip(&mut watches)
                        .filter_map(|((name, cert), watch)| {
                            let result = watch.run(cert, trigger)?;
                            log(name, &result, trigger);
                            Some((name.clone(), result))
                        })
                        .collect()
                };
                // `None`: no periodic check (interval zero, or too far to represent).
                let next = || match interval.is_zero() {
                    true => None,
                    false => Instant::now().checked_add(interval),
                };
                let mut check = next();
                loop {
                    let request = match check {
                        None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                        Some(at) => rx.recv_timeout(at.saturating_duration_since(Instant::now())),
                    };
                    match request {
                        Ok(reply) => {
                            let results = reload_all(Trigger::Signal);
                            if let Some(reply) = reply {
                                let _ = reply.send(results);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            reload_all(Trigger::File);
                            check = next();
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
        let mut watch = Watch::new(&store);
        assert_eq!(served(&store), *a.der());
        assert_eq!(watch.run(&store, Trigger::File), None, "files unchanged");
        assert_eq!(
            watch.run(&store, Trigger::Signal),
            Some(Reload::Unchanged),
            "same certificate"
        );

        files.cert(&b);
        files.key(&kb);
        assert_eq!(
            watch.run(&store, Trigger::File),
            Some(Reload::Reloaded {
                not_after: Some(2_571_696_000)
            })
        );
        assert_eq!(served(&store), *b.der());
        assert_eq!(store.not_after(), Some(2_571_696_000));
        assert_eq!(counts(&store), (1, 0));
    }

    /// A check between the replacement of the key and of the certificate (a normal renewal)
    /// is not a failure: retried on the next check, not counted.
    #[test]
    fn a_half_replaced_pair_is_retried_and_not_reported() {
        let files = Files::new("half");
        let ((a, ka), (b, kb)) = (cert((2027, 1, 1)), cert((2051, 6, 30)));
        files.cert(&a);
        files.key(&ka);
        let store = CertStore::open(&files.cert, &files.key).unwrap();
        let mut watch = Watch::new(&store);
        files.key(&kb);
        assert_eq!(
            watch.run(&store, Trigger::File),
            None,
            "first failure: retry"
        );
        files.cert(&b);
        assert!(matches!(
            watch.run(&store, Trigger::File),
            Some(Reload::Reloaded { .. })
        ));
        assert_eq!(counts(&store), (1, 0));
    }

    /// Files that keep failing are reported once (on the second check) and read again on every
    /// check, even with an unchanged stamp: a file fixed in place with the same length and
    /// modification time is still picked up.
    #[test]
    fn a_lasting_failure_is_reported_once_and_retried_every_check() {
        let files = Files::new("lasting");
        let (a, ka) = cert((2027, 1, 1));
        files.cert(&a);
        files.key(&ka);
        let store = CertStore::open(&files.cert, &files.key).unwrap();
        let mut watch = Watch::new(&store);
        // Same length, no certificate in it.
        let broken = a.pem().replace("BEGIN CERTIFICATE", "BEGIN CERTIFICATX");
        std::fs::write(&files.cert, &broken).unwrap();
        assert_eq!(watch.run(&store, Trigger::File), None);
        assert!(matches!(
            watch.run(&store, Trigger::File),
            Some(Reload::Failed(_))
        ));
        assert_eq!(watch.run(&store, Trigger::File), None, "reported once");
        assert_eq!(counts(&store), (0, 1));
        assert_eq!(served(&store), *a.der());

        // Fixed in place, same length and modification time: the stamp does not change.
        let (stamp, mtime) = (
            store.stamp(),
            std::fs::metadata(&files.cert).unwrap().modified().unwrap(),
        );
        files.cert(&a);
        std::fs::File::options()
            .write(true)
            .open(&files.cert)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        assert_eq!(store.stamp(), stamp);
        assert_eq!(
            watch.run(&store, Trigger::File),
            Some(Reload::Unchanged),
            "read again although the stamp is the same"
        );
        assert!(watch.failed.is_none());
    }

    /// SIGHUP reports a failure at once; the watcher then does not report the same files again.
    #[test]
    fn a_signal_reports_a_failure_at_once() {
        let files = Files::new("signal");
        let ((a, ka), (b, _)) = (cert((2027, 1, 1)), cert((2051, 6, 30)));
        files.cert(&a);
        files.key(&ka);
        let store = CertStore::open(&files.cert, &files.key).unwrap();
        let mut watch = Watch::new(&store);
        files.cert(&b);
        assert!(matches!(
            watch.run(&store, Trigger::Signal),
            Some(Reload::Failed(_))
        ));
        assert_eq!(watch.run(&store, Trigger::File), None);
        assert_eq!(watch.run(&store, Trigger::File), None);
        assert_eq!(counts(&store), (0, 1));
    }
}
