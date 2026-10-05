//! HTTP basic auth for the metrics listener (spec §13.7).

use base64::Engine;
use sha1::{Digest, Sha1};

/// The expected `username:password`, kept as a digest: a check compares two digests of the same
/// length in constant time, so neither the content nor the length of the secret shows in timing.
pub(crate) struct Credentials([u8; 20]);

impl Credentials {
    pub(crate) fn new(username: &str, password: &str) -> Self {
        Self(digest(format!("{username}:{password}").as_bytes()))
    }

    /// Whether an `Authorization` header value carries these credentials (`Basic`, any case).
    pub(crate) fn check(&self, header: Option<&str>) -> bool {
        let Some((scheme, token)) = header.and_then(|h| h.trim().split_once(' ')) else {
            return false;
        };
        if !scheme.eq_ignore_ascii_case("basic") {
            return false;
        }
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(token.trim()) else {
            return false;
        };
        let given = digest(&decoded);
        given
            .iter()
            .zip(&self.0)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
    }
}

fn digest(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn basic(text: &str) -> String {
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(text)
        )
    }

    #[test]
    fn checks_basic_credentials() {
        let c = Credentials::new("grafana", "s3cret");
        assert!(c.check(Some(&basic("grafana:s3cret"))));
        assert!(c.check(Some(&basic("grafana:s3cret").replace("Basic", "basic"))));
        assert!(c.check(Some(&format!("  {}  ", basic("grafana:s3cret")))));
        for wrong in [
            basic("grafana:s3cre"),
            basic("grafana:s3cret "),
            basic("grafan:s3cret"),
            basic("grafanas3cret"),
            basic(""),
            basic("grafana:s3cret").replace("Basic", "Bearer"),
            "Basic not-base64!".into(),
            "Basic".into(),
            String::new(),
        ] {
            assert!(!c.check(Some(&wrong)), "{wrong:?}");
        }
        assert!(!c.check(None));
    }
}
