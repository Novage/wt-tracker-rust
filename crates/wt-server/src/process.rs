//! Process and host numbers for `/metrics` (spec §13.7), read from Linux `/proc` at scrape time;
//! `None` (no sample) elsewhere or when a file cannot be read.

/// Clock ticks per second of `/proc` CPU times (`USER_HZ`: 100 on x86 and arm64 Linux).
const USER_HZ: f64 = 100.0;

/// CPU time of this process (user + system), in seconds.
pub(crate) fn cpu_seconds() -> Option<f64> {
    stat_cpu_seconds(&std::fs::read_to_string("/proc/self/stat").ok()?)
}

/// CPU time of thread `tid` of this process (user + system), in seconds.
pub(crate) fn thread_cpu_seconds(tid: i32) -> Option<f64> {
    let path = format!("/proc/self/task/{tid}/stat");
    stat_cpu_seconds(&std::fs::read_to_string(path).ok()?)
}

/// Kernel thread id of the calling thread (`/proc/thread-self` → `<pid>/task/<tid>`).
pub(crate) fn current_tid() -> Option<i32> {
    let link = std::fs::read_link("/proc/thread-self").ok()?;
    link.file_name()?.to_str()?.parse().ok()
}

/// Open file descriptors of this process.
pub(crate) fn open_fds() -> Option<usize> {
    Some(std::fs::read_dir("/proc/self/fd").ok()?.count())
}

/// The soft limit on open file descriptors (`None` if unlimited).
pub(crate) fn max_fds() -> Option<u64> {
    limits_max_open_files(&std::fs::read_to_string("/proc/self/limits").ok()?)
}

/// Connections the kernel dropped because a listen (accept) queue was full: `TcpExt
/// ListenOverflows` of the network namespace.
pub(crate) fn listen_overflows() -> Option<u64> {
    netstat_value(
        &std::fs::read_to_string("/proc/net/netstat").ok()?,
        "TcpExt:",
        "ListenOverflows",
    )
}

/// `utime + stime` (fields 14 and 15) of a `stat` file, in seconds. The command name (field 2)
/// may contain spaces and parentheses, so fields are counted after its last `)`.
fn stat_cpu_seconds(stat: &str) -> Option<f64> {
    let mut fields = stat.rsplit_once(')')?.1.split_whitespace().skip(11);
    let utime: u64 = fields.next()?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some((utime + stime) as f64 / USER_HZ)
}

/// The soft limit of the `Max open files` line of a `limits` file.
fn limits_max_open_files(limits: &str) -> Option<u64> {
    let line = limits.lines().find(|l| l.starts_with("Max open files"))?;
    line["Max open files".len()..]
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// A value of a `/proc/net/netstat`-style file: a line of names, then a line of values, both
/// starting with `prefix`.
fn netstat_value(text: &str, prefix: &str, name: &str) -> Option<u64> {
    let mut lines = text.lines().filter(|l| l.starts_with(prefix));
    let names = lines.next()?.split_whitespace();
    let values = lines.next()?.split_whitespace();
    names
        .zip(values)
        .find(|(n, _)| *n == name)
        .and_then(|(_, v)| v.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_files() {
        let stat = "123 (wt (worker) 0) S 1 123 123 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 3 0";
        assert_eq!(stat_cpu_seconds(stat), Some(3.0));
        assert_eq!(stat_cpu_seconds("123 (x) S 1"), None);

        let limits = "Limit                     Soft Limit           Hard Limit           Units\n\
                      Max cpu time              unlimited            unlimited            seconds\n\
                      Max open files            1048576              1048576              files\n";
        assert_eq!(limits_max_open_files(limits), Some(1_048_576));
        assert_eq!(
            limits_max_open_files("Max open files            unlimited  unlimited  files"),
            None
        );

        let netstat = "TcpExt: SyncookiesSent ListenOverflows ListenDrops\n\
                       TcpExt: 0 3648925 3648933\n\
                       IpExt: InNoRoutes\nIpExt: 0\n";
        assert_eq!(
            netstat_value(netstat, "TcpExt:", "ListenOverflows"),
            Some(3_648_925)
        );
        assert_eq!(netstat_value(netstat, "TcpExt:", "Missing"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_process() {
        assert!(cpu_seconds().is_some());
        let tid = current_tid().unwrap();
        assert!(thread_cpu_seconds(tid).is_some());
        assert!(open_fds().unwrap() > 0);
        assert!(listen_overflows().is_some());
    }
}
