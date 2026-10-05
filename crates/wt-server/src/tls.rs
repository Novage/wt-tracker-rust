//! wss:// listeners: rustls server config whose certificate can be reloaded from its PEM files
//! while the server runs (SIGHUP or a file change, spec §13.2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

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

/// `CertStore::not_after` of a certificate whose `notAfter` could not be parsed.
pub(crate) const UNKNOWN: i64 = i64::MIN;

/// Modification time and length of both files (following symlinks): a change starts a reload.
type Stamp = Option<[(SystemTime, u64); 2]>;

/// The certificate of a listener, read from its PEM files, swapped on reload. rustls asks it for
/// the certificate on every full handshake, so the TLS config (and its session ticket keys)
/// stays the same across reloads.
pub(crate) struct CertStore {
    cert_file: PathBuf,
    key_file: PathBuf,
    current: RwLock<Arc<CertifiedKey>>,
    seen: Mutex<Stamp>,
    /// `notAfter` of the served certificate (Unix seconds; [`UNKNOWN`] if not parsed).
    pub(crate) not_after: AtomicI64,
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
    /// Loads the certificate chain and key; fails if they cannot be used together.
    pub(crate) fn open(cert_file: &Path, key_file: &Path) -> Result<Arc<Self>, String> {
        let seen = stamp(cert_file, key_file);
        let (key, not_after) = load(cert_file, key_file)?;
        Ok(Arc::new(Self {
            cert_file: cert_file.to_path_buf(),
            key_file: key_file.to_path_buf(),
            current: RwLock::new(Arc::new(key)),
            seen: Mutex::new(seen),
            not_after: AtomicI64::new(not_after.unwrap_or(UNKNOWN)),
            reloads_ok: AtomicU64::new(0),
            reloads_failed: AtomicU64::new(0),
        }))
    }

    /// Reloads if the files changed since the last attempt (`force`: in any case). A missing
    /// file without `force` waits for the next check (a renewal may be replacing it).
    ///
    /// Reloads run one at a time (the file watcher on worker 0 and SIGHUP on the main thread):
    /// the `seen` lock is held from reading the stamp to installing the certificate, so a slower
    /// reload of older files can never finish after a newer one and put the old certificate back.
    pub(crate) fn reload(&self, force: bool) -> Reload {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let now = stamp(&self.cert_file, &self.key_file);
        if !force && (now.is_none() || *seen == now) {
            return Reload::Unchanged;
        }
        *seen = now;
        match load(&self.cert_file, &self.key_file) {
            Ok((key, not_after)) => {
                let mut current = self.current.write().unwrap_or_else(|e| e.into_inner());
                if current.cert == key.cert {
                    return Reload::Unchanged;
                }
                *current = Arc::new(key);
                self.not_after.store(not_after.unwrap_or(UNKNOWN), Relaxed);
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
            self.current
                .read()
                .unwrap_or_else(|e| e.into_inner())
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

/// The chain and key from PEM files, checked to belong together, and the leaf's `notAfter`
/// (`None` if it could not be parsed).
fn load(cert_file: &Path, key_file: &Path) -> Result<(CertifiedKey, Option<i64>), String> {
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
    Ok((key, not_after))
}

/// rustls server config serving the store's certificate.
pub(crate) fn server_config(store: Arc<CertStore>) -> Result<Arc<ServerConfig>, String> {
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

/// Logs a reload of `listener` (spec §13.8). `trigger`: `signal` or `file`.
pub(crate) fn log(listener: &str, result: &Reload, trigger: &str) {
    match result {
        Reload::Reloaded { not_after } => crate::event!(
            Info,
            "tls_reloaded",
            listener = listener,
            not_after = not_after.map_or_else(|| "unknown".into(), crate::logging::timestamp),
            trigger = trigger
        ),
        Reload::Unchanged if trigger == "signal" => {
            crate::event!(Info, "tls_unchanged", listener = listener)
        }
        Reload::Unchanged => {}
        Reload::Failed(error) => crate::event!(
            Error,
            "tls_reload_failed",
            listener = listener,
            error = error,
            trigger = trigger
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
        let served = |store: &CertStore| store.current.read().unwrap().cert[0].clone();
        assert_eq!(served(&store), *a.der());
        assert_eq!(store.reload(false), Reload::Unchanged);
        assert_eq!(store.reload(true), Reload::Unchanged, "same certificate");

        let (b, kb) = cert((2051, 6, 30));
        write(&b, &kb);
        assert_eq!(
            store.reload(false),
            Reload::Reloaded {
                not_after: Some(2_571_696_000)
            }
        );
        assert_eq!(served(&store), *b.der());

        // A new certificate with the old key: rejected, b stays.
        std::fs::write(&cert_file, a.pem()).unwrap();
        assert!(matches!(store.reload(false), Reload::Failed(_)));
        assert_eq!(served(&store), *b.der());
        assert_eq!(
            store.reload(false),
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

    /// A reload keeps its lock until the certificate is installed (the SIGHUP and the file
    /// watcher run on different threads), so the newer files are always installed last.
    #[test]
    fn reloads_run_one_at_a_time() {
        let dir = std::env::temp_dir().join(format!("wt-tls-serial-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert_file, key_file) = (dir.join("cert.pem"), dir.join("key.pem"));
        let (a, ka) = cert((2027, 1, 1));
        std::fs::write(&cert_file, a.pem()).unwrap();
        std::fs::write(&key_file, ka.serialize_pem()).unwrap();
        let store = CertStore::open(&cert_file, &key_file).unwrap();
        let (b, kb) = cert((2051, 6, 30));
        std::fs::write(&cert_file, b.pem()).unwrap();
        std::fs::write(&key_file, kb.serialize_pem()).unwrap();

        // A handshake reading the certificate blocks the swap of this reload: it is stuck
        // between loading the files and installing them, and must still hold the reload lock,
        // so another reload (of possibly older files) cannot finish after it.
        let handshake = store.current.read().unwrap();
        let (done_tx, done) = std::sync::mpsc::channel();
        let reloading = {
            let store = store.clone();
            std::thread::spawn(move || done_tx.send(store.reload(true)).unwrap())
        };
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            done.try_recv().is_err(),
            "the swap should wait for the handshake"
        );
        assert!(
            store.seen.try_lock().is_err(),
            "the reload lock was released before the certificate was installed"
        );
        drop(handshake);
        assert!(matches!(done.recv().unwrap(), Reload::Reloaded { .. }));
        reloading.join().unwrap();
        assert_eq!(store.current.read().unwrap().cert[0], *b.der());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
