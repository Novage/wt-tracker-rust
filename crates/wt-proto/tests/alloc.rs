//! Steady-state allocation guarantees (spec §7.4), after warm-up, for `handle` + `Encoder` on
//! re-announces with 10 offers (~2 KB SDP each), answers and scrapes:
//! - `serde_json` backend: zero heap allocations;
//! - `sonic` backend: no copies of frame data (its lazy iterators allocate small per-member
//!   key strings only), checked as: no single allocation of 256 bytes or more (an SDP copy
//!   would be ~2 KB).

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use common::backends;
use wt_core::{ConnId, Settings, Shard};
use wt_proto::Encoder;

struct Counting;

thread_local! {
    // Const-initialized: accessing them from the allocator never allocates.
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn record(size: usize) {
    ALLOCATIONS.with(|c| c.set(c.get() + 1));
    LARGEST.with(|c| c.set(c.get().max(size)));
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations() -> u64 {
    ALLOCATIONS.with(|c| c.get())
}

fn sdp(i: usize) -> String {
    // ~2 KB with CRLF escapes, like a real offer.
    let mut s = String::from(
        "v=0\\r\\no=- 4611731400430051336 2 IN IP4 127.0.0.1\\r\\ns=-\\r\\nt=0 0\\r\\n",
    );
    for k in 0..40 {
        s.push_str(&format!(
            "a=candidate:{k} 1 udp 2122260223 192.168.1.{i} 5{k:04} typ host\\r\\n"
        ));
    }
    s
}

#[test]
fn steady_state_handling_does_not_allocate() {
    let offers: Vec<String> = (0..10)
        .map(|i| {
            format!(
                r#"{{"offer":{{"type":"offer","sdp":"{}"}},"offer_id":"offer{i:015}"}}"#,
                sdp(i)
            )
        })
        .collect();
    let reannounce = format!(
        r#"{{"action":"announce","info_hash":"h0000000000000000000","peer_id":"p0000000000000000005","numwant":10,"uploaded":0,"downloaded":0,"offers":[{}]}}"#,
        offers.join(",")
    );
    let answer = format!(
        r#"{{"action":"announce","info_hash":"h0000000000000000000","peer_id":"p0000000000000000005","to_peer_id":"p0000000000000000007","answer":{{"type":"answer","sdp":"{}"}},"offer_id":"offer000000000000003"}}"#,
        sdp(3)
    );
    let scrape = r#"{"action":"scrape","info_hash":"h0000000000000000000"}"#.to_string();
    let frames = [reannounce, answer, scrape];

    for (name, _, handle) in backends() {
        let mut shard = Shard::new(Settings::default(), 1);
        let mut out = Encoder::new();
        for c in 0..30u64 {
            let join = format!(
                r#"{{"action":"announce","event":"started","info_hash":"h0000000000000000000","peer_id":"p{c:019}"}}"#
            );
            handle(&mut shard, 0, ConnId(c), join.as_bytes(), &mut out).unwrap();
        }
        // Warm-up grows the encoder buffers to their steady size.
        for _ in 0..3 {
            for frame in &frames {
                out.clear();
                handle(&mut shard, 0, ConnId(5), frame.as_bytes(), &mut out).unwrap();
            }
        }

        let before = allocations();
        LARGEST.with(|c| c.set(0));
        let mut messages = 0;
        for _ in 0..100 {
            for frame in &frames {
                out.clear();
                handle(&mut shard, 0, ConnId(5), frame.as_bytes(), &mut out).unwrap();
                messages += out.messages().len();
            }
        }
        let count = allocations() - before;
        let largest = LARGEST.with(|c| c.get());
        assert_eq!(messages, 100 * (1 + 10 + 1 + 1), "{name}");
        eprintln!(
            "{name}: {} allocations per frame, largest {largest} bytes",
            count as f64 / 300.0
        );
        match name {
            "serde_json" => assert_eq!(count, 0, "{name}: {count} allocations in 300 frames"),
            _ => assert!(
                largest < 256,
                "{name}: an allocation of {largest} bytes copies frame data"
            ),
        }
    }
}
