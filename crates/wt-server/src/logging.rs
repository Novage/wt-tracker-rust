//! Log events as logfmt lines on stderr: `level=warn event=accept_failed error="..."` (spec
//! §13.8). Use [`event!`](crate::event) and [`event_limited!`](crate::event_limited).

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Severity; a line is written when its level is at most the configured one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
        }
    }

    /// `"error"`, `"warn"`, `"info"` or `"debug"`.
    pub fn parse(name: &str) -> Option<Self> {
        [Level::Error, Level::Warn, Level::Info, Level::Debug]
            .into_iter()
            .find(|l| l.as_str() == name)
    }
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);
static TIMESTAMPS: AtomicBool = AtomicBool::new(true);

/// A rate-limited event is written at most once per this interval per worker thread.
pub const LIMIT_INTERVAL: Duration = Duration::from_secs(10);

/// Sets the level, adds `ts=` unless stderr goes to journald (it stamps lines itself), and
/// forwards warnings and errors of libraries (rustls) as `event=library`.
pub fn init(level: Level) {
    LEVEL.store(level as u8, Relaxed);
    TIMESTAMPS.store(std::env::var_os("JOURNAL_STREAM").is_none(), Relaxed);
    if log::set_logger(&LIBRARY).is_ok() {
        log::set_max_level(log::LevelFilter::Warn);
    }
}

pub fn enabled(level: Level) -> bool {
    level as u8 <= LEVEL.load(Relaxed)
}

/// Writes one line. `suppressed`: lines of this event skipped by the rate limit since the
/// last one (0: no field).
pub fn write(level: Level, event: &str, fields: &[(&str, &dyn fmt::Display)], suppressed: u64) {
    let mut line = String::with_capacity(128);
    if TIMESTAMPS.load(Relaxed) {
        line.push_str("ts=");
        push_timestamp(&mut line, SystemTime::now());
        line.push(' ');
    }
    let _ = write!(line, "level={} event={event}", level.as_str());
    let mut value = String::new();
    for (key, display) in fields {
        value.clear();
        let _ = write!(value, "{display}");
        line.push(' ');
        line.push_str(key);
        line.push('=');
        push_value(&mut line, &value);
    }
    if suppressed > 0 {
        let _ = write!(line, " suppressed={suppressed}");
    }
    line.push('\n');
    // One write per line (and captured by the test harness).
    eprint!("{line}");
}

/// Rate limit per worker thread: `Some(skipped)` if `event` may be written now.
pub fn allow(event: &'static str) -> Option<u64> {
    thread_local! {
        static LAST: RefCell<HashMap<&'static str, (Instant, u64)>> = RefCell::new(HashMap::new());
    }
    LAST.with(|last| {
        let mut last = last.borrow_mut();
        let now = Instant::now();
        match last.get_mut(event) {
            Some((at, skipped)) if now.duration_since(*at) < LIMIT_INTERVAL => {
                *skipped += 1;
                None
            }
            Some((at, skipped)) => {
                *at = now;
                Some(std::mem::take(skipped))
            }
            None => {
                last.insert(event, (now, 0));
                Some(0)
            }
        }
    })
}

/// A value as is, or quoted (with `"`, `\` and control characters escaped) if it is empty or
/// contains a space, `=`, `"` or a control character.
fn push_value(line: &mut String, value: &str) {
    let plain = !value.is_empty()
        && !value
            .chars()
            .any(|c| c == ' ' || c == '=' || c == '"' || c == '\\' || c.is_control());
    if plain {
        line.push_str(value);
        return;
    }
    line.push('"');
    for c in value.chars() {
        match c {
            '"' => line.push_str("\\\""),
            '\\' => line.push_str("\\\\"),
            '\n' => line.push_str("\\n"),
            '\r' => line.push_str("\\r"),
            '\t' => line.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(line, "\\u{{{:x}}}", c as u32);
            }
            c => line.push(c),
        }
    }
    line.push('"');
}

/// RFC 3339 in UTC with milliseconds, e.g. `2026-10-04T12:56:42.123Z`.
fn push_timestamp(line: &mut String, time: SystemTime) {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs();
    let (days, rest) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days as i64);
    let _ = write!(
        line,
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3600,
        rest / 60 % 60,
        rest % 60,
        since.subsec_millis()
    );
}

/// Unix seconds as RFC 3339 UTC without fractions, e.g. `2027-01-01T00:00:00Z`.
pub(crate) fn timestamp(unix: i64) -> String {
    let (days, rest) = (unix.div_euclid(86_400), unix.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3600,
        rest / 60 % 60,
        rest % 60
    )
}

/// (year, month, day) → days since 1970-01-01, the inverse of [`civil_from_days`].
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year.rem_euclid(400);
    let mp = (i64::from(month) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian (H. Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Library log records (the `log` crate: rustls) as `event=library` lines.
struct Library;

static LIBRARY: Library = Library;

impl log::Log for Library {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn && enabled(library_level(metadata.level()))
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            write(
                library_level(record.level()),
                "library",
                &[("target", &record.target()), ("message", record.args())],
                0,
            );
        }
    }

    fn flush(&self) {}
}

fn library_level(level: log::Level) -> Level {
    match level {
        log::Level::Error => Level::Error,
        log::Level::Warn => Level::Warn,
        log::Level::Info => Level::Info,
        _ => Level::Debug,
    }
}

/// Writes a log event if its level is enabled:
/// `event!(Warn, "accept_failed", error = e, listener = name)`. Values are `Display`.
#[macro_export]
macro_rules! event {
    ($level:ident, $event:expr $(, $key:ident = $value:expr)* $(,)?) => {
        if $crate::logging::enabled($crate::logging::Level::$level) {
            $crate::logging::write(
                $crate::logging::Level::$level,
                $event,
                &[$((stringify!($key), &$value as &dyn ::std::fmt::Display)),*],
                0,
            );
        }
    };
}

/// [`event!`] at most once per [`LIMIT_INTERVAL`] per worker thread; the next line written
/// carries `suppressed=N`.
#[macro_export]
macro_rules! event_limited {
    ($level:ident, $event:expr $(, $key:ident = $value:expr)* $(,)?) => {
        if $crate::logging::enabled($crate::logging::Level::$level)
            && let Some(suppressed) = $crate::logging::allow($event)
        {
            $crate::logging::write(
                $crate::logging::Level::$level,
                $event,
                &[$((stringify!($key), &$value as &dyn ::std::fmt::Display)),*],
                suppressed,
            );
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(v: &str) -> String {
        let mut line = String::new();
        push_value(&mut line, v);
        line
    }

    #[test]
    fn values_are_quoted_only_when_needed() {
        assert_eq!(value("1.2.3.4:5"), "1.2.3.4:5");
        assert_eq!(value(""), "\"\"");
        assert_eq!(value("invalid JSON"), "\"invalid JSON\"");
        assert_eq!(value("a=b"), "\"a=b\"");
        assert_eq!(value("say \"hi\"\n"), "\"say \\\"hi\\\"\\n\"");
        assert_eq!(value("back\\slash"), "\"back\\\\slash\"");
    }

    #[test]
    fn timestamps_are_rfc3339_utc() {
        let mut line = String::new();
        push_timestamp(
            &mut line,
            UNIX_EPOCH + Duration::from_millis(1_791_118_602_123),
        );
        assert_eq!(line, "2026-10-04T12:56:42.123Z");
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        for days in [-1, 0, 11_016, 20_819, 47_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(timestamp(1_798_761_600), "2027-01-01T00:00:00Z");
    }

    #[test]
    fn rate_limit_counts_skipped_events() {
        assert_eq!(allow("test_event"), Some(0));
        assert_eq!(allow("test_event"), None);
        assert_eq!(allow("test_event"), None);
        assert_eq!(allow("other_event"), Some(0));
    }

    #[test]
    fn levels_parse() {
        assert_eq!(Level::parse("debug"), Some(Level::Debug));
        assert_eq!(Level::parse("verbose"), None);
        assert!(Level::Error < Level::Debug);
    }
}
