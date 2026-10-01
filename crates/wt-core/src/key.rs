use std::fmt;

/// Maximum length in bytes of an `info_hash` / `peer_id` after JSON unescaping.
///
/// WebTorrent ids are 20-character binary strings; characters >= 0x80 take two bytes
/// in UTF-8, so 40 bytes covers every 20-character id.
pub const MAX_KEY_LEN: usize = 40;

/// An `info_hash` or `peer_id`, stored inline (no heap allocation).
#[derive(Clone, Copy)]
pub struct Key {
    len: u8,
    bytes: [u8; MAX_KEY_LEN],
}

impl Key {
    /// Returns `None` if `bytes` is longer than [`MAX_KEY_LEN`].
    #[inline]
    pub fn new(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_KEY_LEN {
            return None;
        }
        let mut key = Self {
            len: bytes.len() as u8,
            bytes: [0; MAX_KEY_LEN],
        };
        key.bytes[..bytes.len()].copy_from_slice(bytes);
        Some(key)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl PartialEq for Key {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Key {}

impl PartialEq<[u8]> for Key {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

impl std::hash::Hash for Key {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.as_bytes()) {
            Ok(s) => write!(f, "Key({s:?})"),
            Err(_) => write!(f, "Key({:02x?})", self.as_bytes()),
        }
    }
}
