//! Configuration: the JS wt-tracker `config.json` format, plus a few server options.

use std::path::PathBuf;

use serde::Deserialize;
use wt_core::{OfferSelection, Settings};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    /// Listeners. Default: one plain ws:// listener on 0.0.0.0:8000.
    #[serde(default = "default_servers")]
    pub servers: Vec<ServerItem>,
    #[serde(default)]
    pub tracker: TrackerConfig,
    #[serde(default)]
    pub websockets_access: AccessConfig,
    /// Worker threads (= shards). Default: available parallelism. At most 64.
    #[serde(default)]
    pub workers: Option<usize>,
    /// Linux: one `SO_REUSEPORT` socket per worker (kernel load balancing). Otherwise all
    /// workers accept from one shared socket.
    #[serde(default)]
    pub reuse_port: bool,
    /// Per-connection queued outgoing bytes; further messages to that connection are dropped.
    #[serde(default = "default_max_backpressure")]
    pub max_backpressure: usize,
    /// `index.html` served at `/`. Default: `./index.html` if it exists.
    #[serde(default)]
    pub index_html: Option<PathBuf>,
    /// Seconds a graceful shutdown (SIGTERM / SIGINT) waits for connections to close.
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout: u64,
    /// Which shard owns an info_hash: `content` (default; swarms follow the content and
    /// connections move to them, spec §13.3) or `hash` (`foldhash(info_hash) % workers`).
    #[serde(default)]
    pub placement: Option<String>,
    /// `error`, `warn`, `info` (default) or `debug` (spec §13.8).
    #[serde(default)]
    pub log_level: Option<String>,
    /// Removed (an earlier file watcher for the certificates): accepted with a startup warning.
    #[serde(default)]
    pub tls_reload_interval: Option<serde_json::Value>,
    /// Prometheus `/metrics`, `/swarms` and `/stats.json` on a separate listener; off when
    /// absent (spec §13.7).
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_host")]
    pub host: String,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    /// PEM private key; together with `cert_file_name` enables HTTPS.
    pub key_file_name: Option<PathBuf>,
    /// PEM certificate chain.
    pub cert_file_name: Option<PathBuf>,
    /// HTTP basic auth for every route; together with `password`.
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServerItem {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub websockets: WebSocketsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// PEM private key; together with `cert_file_name` enables wss://.
    pub key_file_name: Option<PathBuf>,
    /// PEM certificate chain.
    pub cert_file_name: Option<PathBuf>,
    // Accepted for compatibility with the JS config, not supported by rustls:
    pub passphrase: Option<String>,
    pub dh_params_file_name: Option<String>,
    pub ca_file_name: Option<String>,
    pub ssl_ciphers: Option<String>,
    pub ssl_prefer_low_memory_usage: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketsConfig {
    /// URL pattern for upgrades: `/*` = any path, `/prefix/*`, or an exact path.
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_max_payload")]
    pub max_payload_length: usize,
    /// Seconds without any received frame before the connection is closed (0 = never).
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: u64,
    /// permessage-deflate (spec §13.2): 0 = off; 1 (default, like the JS tracker) = negotiated
    /// without context takeover, client messages inflated; other values (JS 2 = dedicated
    /// compressor) are treated as 1 with a startup warning.
    #[serde(default = "default_compression")]
    pub compression: u32,
    /// With permessage-deflate negotiated, outgoing messages at least this long are compressed
    /// (offers; replies and answers are shorter). Default 1024; 0 = never (like the JS tracker).
    #[serde(default = "default_compress_outgoing_min_size")]
    pub compress_outgoing_min_size: usize,
    /// 0 = no limit.
    #[serde(default)]
    pub max_connections: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackerConfig {
    #[serde(default = "default_max_offers")]
    pub max_offers: u32,
    #[serde(default = "default_announce_interval")]
    pub announce_interval: u32,
    /// `sample` (default), `window` or `round_robin` (spec §5.2).
    #[serde(default)]
    pub offer_selection: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessConfig {
    pub allow_origins: Option<Vec<String>>,
    pub deny_origins: Option<Vec<String>>,
    #[serde(default)]
    pub deny_empty_origin: bool,
}

fn default_servers() -> Vec<ServerItem> {
    vec![ServerItem::default()]
}
fn default_metrics_host() -> String {
    "127.0.0.1".into()
}
fn default_metrics_port() -> u16 {
    9100
}
fn default_shutdown_timeout() -> u64 {
    5
}

fn default_max_backpressure() -> usize {
    1 << 20
}
fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    8000
}
fn default_path() -> String {
    "/*".into()
}
fn default_max_payload() -> usize {
    64 * 1024
}
fn default_compress_outgoing_min_size() -> usize {
    1024
}

fn default_compression() -> u32 {
    1
}

fn default_idle_timeout() -> u64 {
    240
}
fn default_max_offers() -> u32 {
    20
}
fn default_announce_interval() -> u32 {
    20
}

impl Default for ServerConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl Default for WebSocketsConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl Default for TrackerConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl Default for Config {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl Config {
    pub fn from_json(text: &str) -> Result<Self, String> {
        let config: Config =
            serde_json::from_str(text).map_err(|e| format!("invalid configuration: {e}"))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        let a = &self.websockets_access;
        if a.allow_origins.is_some() && a.deny_origins.is_some() {
            return Err("allowOrigins and denyOrigins can't be set simultaneously".into());
        }
        if self.servers.is_empty() {
            return Err("'servers' must not be empty".into());
        }
        for item in &self.servers {
            if item.server.key_file_name.is_some() != item.server.cert_file_name.is_some() {
                return Err("key_file_name and cert_file_name must be set together".into());
            }
        }
        if let Some(m) = &self.metrics {
            if m.key_file_name.is_some() != m.cert_file_name.is_some() {
                return Err(
                    "metrics: key_file_name and cert_file_name must be set together".into(),
                );
            }
            match (&m.username, &m.password) {
                (None, None) => {}
                (Some(user), Some(password)) => {
                    if user.is_empty() || password.is_empty() {
                        return Err("metrics: username and password must not be empty".into());
                    }
                    if user.contains(':') {
                        return Err("metrics: username must not contain ':'".into());
                    }
                }
                _ => return Err("metrics: username and password must be set together".into()),
            }
        }
        if !(1..=64).contains(&self.worker_count()) {
            return Err("'workers' must be between 1 and 64".into());
        }
        self.tracker_settings()?;
        self.placement_mode()?;
        self.log_level()?;
        Ok(())
    }

    pub fn log_level(&self) -> Result<crate::logging::Level, String> {
        match self.log_level.as_deref() {
            None => Ok(crate::logging::Level::Info),
            Some(name) => crate::logging::Level::parse(name)
                .ok_or_else(|| format!("unknown logLevel '{name}'")),
        }
    }

    pub fn placement_mode(&self) -> Result<crate::placement::Mode, String> {
        match self.placement.as_deref() {
            None | Some("content") => Ok(crate::placement::Mode::Content),
            Some("hash") => Ok(crate::placement::Mode::Hash),
            Some(other) => Err(format!("unknown placement '{other}'")),
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .min(64)
        })
    }

    pub fn tracker_settings(&self) -> Result<Settings, String> {
        let offer_selection = match self.tracker.offer_selection.as_deref() {
            None | Some("sample") => OfferSelection::RandomSample,
            Some("window") => OfferSelection::RandomWindow,
            Some("round_robin") => OfferSelection::RoundRobin,
            Some(other) => return Err(format!("unknown tracker.offerSelection '{other}'")),
        };
        Ok(Settings {
            max_offers: self.tracker.max_offers,
            announce_interval: self.tracker.announce_interval,
            offer_selection,
        })
    }

    /// Options accepted for JS compatibility that this server ignores.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        for item in &self.servers {
            let s = &item.server;
            let name = format!("{}:{}", s.host, s.port);
            for (set, field) in [
                (s.passphrase.is_some(), "passphrase (encrypted keys)"),
                (s.dh_params_file_name.is_some(), "dh_params_file_name"),
                (s.ca_file_name.is_some(), "ca_file_name"),
                (s.ssl_ciphers.is_some(), "ssl_ciphers"),
                (
                    s.ssl_prefer_low_memory_usage.is_some(),
                    "ssl_prefer_low_memory_usage",
                ),
            ] {
                if set {
                    warnings.push(format!("{name}: {field} is not supported and ignored"));
                }
            }
            if item.websockets.compression > 1 {
                warnings.push(format!(
                    "{name}: compression {} is not supported; using 1 (shared, no context takeover)",
                    item.websockets.compression
                ));
            }
        }
        if self.tls_reload_interval.is_some() {
            warnings.push(
                "tlsReloadInterval was removed and is ignored: certificate files are not watched; \
                 reload them with SIGHUP (systemctl reload)"
                    .into(),
            );
        }
        if let Some(m) = &self.metrics
            && m.username.is_some()
            && m.cert_file_name.is_none()
        {
            warnings.push(
                "metrics: basic auth without TLS sends the password in clear text \
                 (set cert_file_name and key_file_name)"
                    .into(),
            );
        }
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_removed_tls_reload_interval_is_warned_about() {
        assert!(Config::from_json("{}").unwrap().warnings().is_empty());
        let warnings = Config::from_json(r#"{"tlsReloadInterval":60}"#)
            .unwrap()
            .warnings();
        assert!(
            warnings.len() == 1 && warnings[0].contains("SIGHUP"),
            "{warnings:?}"
        );
    }

    #[test]
    fn metrics_tls_and_basic_auth_are_set_in_pairs() {
        let metrics = |fields: &str| Config::from_json(&format!(r#"{{"metrics":{{{fields}}}}}"#));
        assert!(metrics("").is_ok());
        assert!(metrics(r#""cert_file_name":"c.pem""#).is_err());
        assert!(metrics(r#""key_file_name":"k.pem""#).is_err());
        assert!(metrics(r#""username":"u""#).is_err());
        assert!(metrics(r#""password":"p""#).is_err());
        assert!(metrics(r#""username":"u","password":"""#).is_err());
        assert!(metrics(r#""username":"","password":"p""#).is_err());
        assert!(metrics(r#""username":"a:b","password":"p""#).is_err());

        let plain = metrics(r#""username":"u","password":"p""#).unwrap();
        let warnings = plain.warnings();
        assert!(
            warnings.len() == 1 && warnings[0].contains("clear text"),
            "{warnings:?}"
        );
        let tls = metrics(
            r#""username":"u","password":"p","cert_file_name":"c.pem","key_file_name":"k.pem""#,
        )
        .unwrap();
        assert!(tls.warnings().is_empty());
    }
}
