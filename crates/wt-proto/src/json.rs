//! Small JSON helpers on raw token text (already validated by the parser backend).

use std::borrow::Cow;

use wt_core::{Key, MAX_KEY_LEN};

/// The token is a JSON string (`"…"`).
#[inline]
pub(crate) fn is_string(raw: &[u8]) -> bool {
    raw.first() == Some(&b'"')
}

/// A JSON number token as `f64` (`None` for any other token).
pub(crate) fn number(raw: &[u8]) -> Option<f64> {
    match raw.first() {
        Some(b'-' | b'0'..=b'9') => std::str::from_utf8(raw).ok()?.parse().ok(),
        _ => None,
    }
}

/// Error from unescaping: a lone UTF-16 surrogate, which has no UTF-8 encoding.
#[derive(Debug)]
pub(crate) struct LoneSurrogate;

/// Unescapes the content of a JSON string token, feeding decoded bytes to `out` in chunks.
/// Stops early (returning `Ok(false)`) when `out` returns `false`.
fn unescape(raw: &[u8], mut out: impl FnMut(&[u8]) -> bool) -> Result<bool, LoneSurrogate> {
    let inner = &raw[1..raw.len() - 1];
    let mut i = 0;
    while i < inner.len() {
        let start = i;
        while i < inner.len() && inner[i] != b'\\' {
            i += 1;
        }
        if start < i && !out(&inner[start..i]) {
            return Ok(false);
        }
        if i == inner.len() {
            break;
        }
        // The backend validated the escape syntax.
        let simple = match inner[i + 1] {
            b'"' => Some(b'"'),
            b'\\' => Some(b'\\'),
            b'/' => Some(b'/'),
            b'b' => Some(0x08),
            b'f' => Some(0x0c),
            b'n' => Some(b'\n'),
            b'r' => Some(b'\r'),
            b't' => Some(b'\t'),
            _ => None,
        };
        if let Some(byte) = simple {
            i += 2;
            if !out(&[byte]) {
                return Ok(false);
            }
            continue;
        }

        let hex = |at: usize| -> u32 {
            inner[at..at + 4].iter().fold(0, |acc, &h| {
                acc * 16 + (h as char).to_digit(16).unwrap_or(0)
            })
        };
        let mut cp = hex(i + 2);
        i += 6;
        if (0xD800..0xDC00).contains(&cp) {
            if inner.get(i) != Some(&b'\\') || inner.get(i + 1) != Some(&b'u') {
                return Err(LoneSurrogate);
            }
            let low = hex(i + 2);
            if !(0xDC00..0xE000).contains(&low) {
                return Err(LoneSurrogate);
            }
            cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
            i += 6;
        } else if (0xDC00..0xE000).contains(&cp) {
            return Err(LoneSurrogate);
        }
        let mut utf8 = [0; 4];
        let ch = char::from_u32(cp).ok_or(LoneSurrogate)?;
        if !out(ch.encode_utf8(&mut utf8).as_bytes()) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Decodes a JSON string token into an inline [`Key`]. `Ok(None)` if the decoded string is
/// longer than [`MAX_KEY_LEN`]. No allocation.
pub(crate) fn decode_key(raw: &[u8]) -> Result<Option<Key>, LoneSurrogate> {
    // Fast path: no escapes.
    if !raw.contains(&b'\\') {
        return Ok(Key::new(&raw[1..raw.len() - 1]));
    }
    let mut buf = [0u8; MAX_KEY_LEN];
    let mut len = 0;
    let fits = unescape(raw, |chunk| {
        if len + chunk.len() > MAX_KEY_LEN {
            return false;
        }
        buf[len..len + chunk.len()].copy_from_slice(chunk);
        len += chunk.len();
        true
    })?;
    Ok(if fits { Key::new(&buf[..len]) } else { None })
}

/// Decodes a JSON string token of any length; borrows when it has no escapes.
pub(crate) fn decode_cow(raw: &[u8]) -> Result<Cow<'_, [u8]>, LoneSurrogate> {
    if !raw.contains(&b'\\') {
        return Ok(Cow::Borrowed(&raw[1..raw.len() - 1]));
    }
    let mut out = Vec::with_capacity(raw.len());
    unescape(raw, |chunk| {
        out.extend_from_slice(chunk);
        true
    })?;
    Ok(Cow::Owned(out))
}

/// Compares a JSON string token with a plain ASCII literal, allowing escaped spellings.
pub(crate) fn string_equals(raw: &[u8], literal: &str) -> bool {
    if !raw.contains(&b'\\') {
        return &raw[1..raw.len() - 1] == literal.as_bytes();
    }
    matches!(decode_key(raw), Ok(Some(k)) if k.as_bytes() == literal.as_bytes())
}

/// Appends `s` as a JSON string exactly like JavaScript's `JSON.stringify`: `"` `\` and control
/// characters are escaped (`\b \t \n \f \r`, others `\u00xx`), everything else is copied.
pub(crate) fn write_string(buf: &mut Vec<u8>, s: &[u8]) {
    buf.push(b'"');
    let mut start = 0;
    for (i, &b) in s.iter().enumerate() {
        let escape: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            0x08 => b"\\b",
            b'\t' => b"\\t",
            b'\n' => b"\\n",
            0x0c => b"\\f",
            b'\r' => b"\\r",
            0x00..=0x1f => b"",
            _ => continue,
        };
        buf.extend_from_slice(&s[start..i]);
        if escape.is_empty() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            buf.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(b >> 4) as usize],
                HEX[(b & 0xf) as usize],
            ]);
        } else {
            buf.extend_from_slice(escape);
        }
        start = i + 1;
    }
    buf.extend_from_slice(&s[start..]);
    buf.push(b'"');
}

#[inline]
pub(crate) fn write_u32(buf: &mut Vec<u8>, n: u32) {
    buf.extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
}
