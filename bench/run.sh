#!/usr/bin/env bash
# Runs the Rust and JS tracker-math benchmarks and regenerates the performance tables in the spec.
# The JS tracker is taken from ../wt-tracker (override with WT_TRACKER_DIR).
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p bench/results
cargo build --release -p wt-bench
echo "== Rust ==" >&2
./target/release/wt-bench > bench/results/rust.json
./target/release/wt-bench-mem > bench/results/rust-mem.json
echo "== JS ==" >&2
node --expose-gc bench/js/bench.ts > bench/results/js.json
echo >&2
node bench/compare.ts
