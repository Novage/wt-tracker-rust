#!/usr/bin/env bash
# Load test: the JS tracker (../wt-tracker, uWebSockets.js) vs the Rust server, over ws:// and
# wss://, with the same load generator (crates/wt-loadgen). Also a wire smoke check (same
# messages from both servers). Writes bench/results/load.json and regenerates spec §11.
#
# Three load profiles (env overrides): light = LIGHT_CONNS=3000 conns re-announcing every 5 s (ws
# and wss); heavy = HEAVY_CONNS=4000 conns every 1 s (ws); media = MEDIA_CONNS=3000 conns in a
# video and an audio swarm of their content, switching between 4 video qualities every 10 s
# (old quality stopped after 5 s), re-announcing every 5 s (ws); deflate = DEFLATE_CONNS=3000 like
# light, but clients offer permessage-deflate and compress everything they send (as browsers do),
# against servers with compression 1 (rust-*: compressing outgoing messages of at least 1 KiB, the
# default; rust-n-in: compressOutgoingMinSize 0, inflating only, like JS; rust-n-off: compression 0,
# the uncompressed baseline). SWARMS=100 DURATION=15 RAMP=1000.
# After each run the Rust server's placement counters (from /metrics) are kept (Local % column).
# On macOS keep conns below ~12000 (ephemeral ports per destination). The client shares the
# machine with the server.
set -euo pipefail
cd "$(dirname "$0")/.."

WT_TRACKER_DIR=${WT_TRACKER_DIR:-../wt-tracker}
LIGHT_CONNS=${LIGHT_CONNS:-3000}
HEAVY_CONNS=${HEAVY_CONNS:-4000}
MEDIA_CONNS=${MEDIA_CONNS:-3000}
DEFLATE_CONNS=${DEFLATE_CONNS:-3000}
SWARMS=${SWARMS:-100}
DURATION=${DURATION:-15}
RAMP=${RAMP:-1000}
# profile|conns|interval|protocols|extra load generator options
PROFILES=(
  "light|$LIGHT_CONNS|5|ws wss|"
  "heavy|$HEAVY_CONNS|1|ws|"
  "media|$MEDIA_CONNS|5|ws|--streams 2 --qualities 4 --switch 10 --overlap 5"
  "deflate|$DEFLATE_CONNS|5|ws|--deflate"
)
WS_PORT=18100
WSS_PORT=18443
METRICS_PORT=18190
OUT=target/loadtest

ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"
cargo build --release -q -p wt-server -p wt-loadgen
mkdir -p "$OUT" bench/results
./target/release/wt-loadgen gen-cert "$OUT"

# $1 = workers ("" = default), $2 = placement ("" = default), $3 = compression (default 0),
# $4 = more websockets settings (,"key":value)
config() {
  local workers="" ws="\"compression\":${3:-0}${4:-}"
  [ -n "$1" ] && workers=",\"workers\":$1"
  [ -n "${2:-}" ] && workers="$workers,\"placement\":\"$2\""
  cat <<EOF
{"servers":[
  {"server":{"host":"127.0.0.1","port":$WS_PORT},"websockets":{$ws}},
  {"server":{"host":"127.0.0.1","port":$WSS_PORT,"key_file_name":"$OUT/key.pem","cert_file_name":"$OUT/cert.pem"},"websockets":{$ws}}
 ],"tracker":{"announceInterval":120}$workers,"metrics":{"host":"127.0.0.1","port":$METRICS_PORT}}
EOF
}
# The placement counters from wt-tracker's /metrics, as {"placement":{...}} (Local % column).
placement_json() {
  curl -s "http://127.0.0.1:$1/metrics" | awk '
    /^wt_routed_requests_total/ { if ($0 ~ /target="local"/) l += $NF; else r += $NF }
    /^wt_moved_connections_total/ { m += $NF }
    END { printf "{\"placement\":{\"localRequests\":%d,\"remoteRequests\":%d,\"movedConnections\":%d}}\n", l, r, m }'
}
config 1 > "$OUT/config-1.json"
config "" > "$OUT/config-n.json"
config 1 "" 1 > "$OUT/config-1-deflate.json"
config "" "" 1 > "$OUT/config-n-deflate.json"
config "" "" 1 ',"compressOutgoingMinSize":0' > "$OUT/config-n-deflate-in.json"
config "" hash > "$OUT/config-n-hash.json"

wait_port() {
  for _ in $(seq 100); do
    (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.1
  done
  return 1
}

SERVER_PID=""
start_server() { # name, command...
  shift
  "$@" > "$OUT/server.log" 2>&1 &
  SERVER_PID=$!
  wait_port "$WS_PORT" && wait_port "$WSS_PORT"
}
stop_server() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  sleep 1
}

declare -a TARGETS=(
  "js|node $WT_TRACKER_DIR/src/run-tracker.ts $OUT/config-1.json"
  "js-workers|node $WT_TRACKER_DIR/src/run-worker-tracker.ts $OUT/config-1.json"
  "rust-1|./target/release/wt-tracker $OUT/config-1.json"
  "rust-n|./target/release/wt-tracker $OUT/config-n.json"
  "rust-n-hash|./target/release/wt-tracker $OUT/config-n-hash.json"
)
# The deflate profile: servers with compression 1 (JS's default).
declare -a DEFLATE_TARGETS=(
  "js|node $WT_TRACKER_DIR/src/run-tracker.ts $OUT/config-1-deflate.json"
  "rust-1|./target/release/wt-tracker $OUT/config-1-deflate.json"
  "rust-n|./target/release/wt-tracker $OUT/config-n-deflate.json"
  "rust-n-in|./target/release/wt-tracker $OUT/config-n-deflate-in.json"
  "rust-n-off|./target/release/wt-tracker $OUT/config-n.json"
)

rm -f "$OUT"/run-*.json
for profile in "${PROFILES[@]}"; do
  IFS='|' read -r pname conns interval protos extra <<< "$profile"
  if [ "$pname" = deflate ]; then targets=("${DEFLATE_TARGETS[@]}"); else targets=("${TARGETS[@]}"); fi
  for target in "${targets[@]}"; do
    name=${target%%|*}
    command=${target#*|}
    for proto in $protos; do
      if [ "$proto" = ws ]; then url="ws://127.0.0.1:$WS_PORT/"; else url="wss://localhost:$WSS_PORT/"; fi
      echo "== $pname $name $proto" >&2
      # shellcheck disable=SC2086
      if ! start_server "$name" $command; then
        echo "   $name did not start, skipped (see $OUT/server.log)" >&2
        stop_server
        continue
      fi
      run="$OUT/run-$pname-$name-$proto.json"
      # shellcheck disable=SC2086
      ./target/release/wt-loadgen load --url "$url" --ca "$OUT/cert.pem" --conns "$conns" --swarms "$SWARMS" \
        --interval "$interval" --duration "$DURATION" --ramp "$RAMP" --server-pid "$SERVER_PID" $extra \
        --label "$pname $name $proto" > "$run" || echo "   load generator failed" >&2
      # Rust only: where requests were applied (local vs another worker's shard).
      placement_json "$METRICS_PORT" > "$OUT/stats.json" 2>/dev/null || true
      node -e '
const fs = require("fs"), [run, stats] = process.argv.slice(1);
try {
  const p = JSON.parse(fs.readFileSync(stats)).placement, r = JSON.parse(fs.readFileSync(run));
  if (p) fs.writeFileSync(run, JSON.stringify({ ...r, placement: p }, null, 2));
} catch {}
' "$run" "$OUT/stats.json"
      stop_server
    done
  done
done

echo "== smoke check" >&2
# With compression 1 (the JS default), so the permessage-deflate step is compared too.
for target in "${DEFLATE_TARGETS[0]}" "${DEFLATE_TARGETS[2]}"; do
  name=${target%%|*}
  # shellcheck disable=SC2086
  start_server "$name" ${target#*|}
  ./target/release/wt-loadgen smoke --url "ws://127.0.0.1:$WS_PORT/" > "$OUT/smoke-$name.json"
  stop_server
done

node -e '
const fs = require("fs"), os = require("os"), dir = process.argv[1];
const runs = fs.readdirSync(dir).filter((f) => f.startsWith("run-")).sort().map((f) => JSON.parse(fs.readFileSync(dir + "/" + f)));
const smoke = (n) => {
  const s = JSON.parse(fs.readFileSync(dir + "/smoke-" + n + ".json"));
  return { received: s.received, deflate: s.deflate };
};
const same = JSON.stringify(smoke("js")) === JSON.stringify(smoke("rust-n"));
const env = { cpu: os.cpus()[0].model, cores: os.availableParallelism(), os: process.platform + " " + os.release(), node: process.version };
fs.writeFileSync("bench/results/load.json", JSON.stringify({ env, smoke_same: same, runs }, null, 2));
console.error("smoke check: " + (same ? "same messages" : "DIFFERENT (see " + dir + "/smoke-*.json)"));
' "$OUT"
node bench/compare.ts > /dev/null
echo "load results: bench/results/load.json (spec §11 regenerated)" >&2
