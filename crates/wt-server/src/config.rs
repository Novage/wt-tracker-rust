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
    /// Which shard owns an info_hash: `content` (default; swarms follow the content and
    /// connections move to them, spec §13.3) or `hash` (`foldhash(info_hash) % workers`).
    #[serde(default)]
    pub placement: Option<String>,
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
    /// Accepted for compatibility; permessage-deflate is not negotiated.
    #[serde(default)]
    pub compression: u32,
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
        if !(1..=64).contains(&self.worker_count()) {
            return Err("'workers' must be between 1 and 64".into());
        }
        self.tracker_settings()?;
        self.placement_mode()?;
        Ok(())
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
            if item.websockets.compression > 0 {
                warnings.push(format!(
                    "{name}: compression is not supported; permessage-deflate is not negotiated"
                ));
            }
        }
        warnings
    }
}
