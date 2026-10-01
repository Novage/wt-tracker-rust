//! Memory benchmark: heap held by a shard after building a scenario state.
//!
//! Reports two numbers per state, each measured in a fresh child process:
//! - `bytes`: live requested heap bytes (counting global allocator), including unused `Vec`
//!   capacity from growth doubling;
//! - `rss_bytes`: resident set size growth, i.e. memory actually touched.
//!
//! Prints JSON to stdout.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::Command;
use std::sync::atomic::{AtomicIsize, Ordering};

use serde_json::{Value, json};
use wt_bench::*;
use wt_core::Shard;

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const STATES: [&str; 2] = ["multi_peer_join", "join_many_swarms"];

fn rss_bytes() -> i64 {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<i64>()
        .expect("rss")
        * 1024
}

/// Child process: builds one state and prints its measurement.
fn measure(name: &str) -> Value {
    let ids = Ids::new(MANY_SWARMS_PEERS, MANY_SWARMS_SWARMS);
    let build: fn(&mut Shard, &Ids) = match name {
        "multi_peer_join" => |s, ids| run_multi_peer_join(s, &mut Counter::default(), ids, 0),
        "join_many_swarms" => |s, ids| run_join_many_swarms(s, &mut Counter::default(), ids),
        _ => panic!("unknown state {name}"),
    };

    let rss_before = rss_bytes();
    let before = LIVE.load(Ordering::Relaxed);
    let mut shard = new_shard();
    build(&mut shard, &ids);
    let bytes = LIVE.load(Ordering::Relaxed) - before;
    let rss = rss_bytes() - rss_before;

    let memberships = shard.membership_count();
    json!({
        "name": name,
        "bytes": bytes,
        "rss_bytes": rss,
        "memberships": memberships,
        "peers": shard.peer_count(),
        "swarms": shard.swarm_count(),
        "bytes_per_membership": bytes as f64 / memberships as f64,
        "rss_bytes_per_membership": rss as f64 / memberships as f64,
    })
}

fn main() {
    if let Some(name) = std::env::args().nth(1) {
        println!("{}", measure(&name));
        return;
    }

    let exe = std::env::current_exe().unwrap();
    let memory: Vec<Value> = STATES
        .iter()
        .map(|name| {
            let out = Command::new(&exe).arg(name).output().expect("child");
            let v: Value = serde_json::from_slice(&out.stdout).expect("child json");
            eprintln!(
                "{name:<20} heap {:>7.1} MiB {:>6.1} B/membership   rss {:>7.1} MiB {:>6.1} B/membership",
                v["bytes"].as_f64().unwrap() / (1 << 20) as f64,
                v["bytes_per_membership"].as_f64().unwrap(),
                v["rss_bytes"].as_f64().unwrap() / (1 << 20) as f64,
                v["rss_bytes_per_membership"].as_f64().unwrap(),
            );
            v
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({ "runtime": "rust", "memory": memory })).unwrap()
    );
}
