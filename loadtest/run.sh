#!/usr/bin/env bash
# Load test: the JS tracker (../wt-tracker, uWebSockets.js) vs the Rust server, over ws:// and
# wss://, with the same load generator (crates/wt-loadgen). Also a wire smoke check (same
# messages from both servers). Writes bench/results/load.json and regenerates spec §11.
#
# Two load profiles (env overrides): light = LIGHT_CONNS=3000 conns re-announcing every 5 s (ws
# and wss); heavy = HEAVY_CONNS=4000 conns every 1 s (ws). SWARMS=100 DURATION=15 RAMP=1000.
# On macOS keep conns below ~12000 (ephemeral ports per destination). The client shares the
# machine with the server.
set -euo pipefail
cd "$(dirname "$0")/.."

WT_TRACKER_DIR=${WT_TRACKER_DIR:-../wt-tracker}
LIGHT_CONNS=${LIGHT_CONNS:-3000}
HEAVY_CONNS=${HEAVY_CONNS:-4000}
SWARMS=${SWARMS:-100}
DURATION=${DURATION:-15}
RAMP=${RAMP:-1000}
# profile|conns|interval|protocols
PROFILES=("light|$LIGHT_CONNS|5|ws wss" "heavy|$HEAVY_CONNS|1|ws")
WS_PORT=18100
WSS_PORT=18443
OUT=target/loadtest

ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"
cargo build --release -q -p wt-server -p wt-loadgen
mkdir -p "$OUT" bench/results
./target/release/wt-loadgen gen-cert "$OUT"

config() { # $1 = workers ("" = default), $2 = transport
  local workers=""
  [ -n "$1" ] && workers=",\"workers\":$1"
  workers="$workers,\"transport\":\"$2\""
  cat <<EOF
{"servers":[
  {"server":{"host":"127.0.0.1","port":$WS_PORT},"websockets":{"compression":0}},
  {"server":{"host":"127.0.0.1","port":$WSS_PORT,"key_file_name":"$OUT/key.pem","cert_file_name":"$OUT/cert.pem"},"websockets":{"compression":0}}
 ],"tracker":{"announceInterval":120}$workers}
EOF
}
config 1 fastwebsockets > "$OUT/config-1.json"
config "" fastwebsockets > "$OUT/config-n.json"
config 1 sockudo > "$OUT/config-1-sockudo.json"
config "" sockudo > "$OUT/config-n-sockudo.json"

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
  "rust-1-sockudo|./target/release/wt-tracker $OUT/config-1-sockudo.json"
  "rust-n-sockudo|./target/release/wt-tracker $OUT/config-n-sockudo.json"
)

rm -f "$OUT"/run-*.json
for profile in "${PROFILES[@]}"; do
  IFS='|' read -r pname conns interval protos <<< "$profile"
  for target in "${TARGETS[@]}"; do
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
      ./target/release/wt-loadgen load --url "$url" --ca "$OUT/cert.pem" --conns "$conns" --swarms "$SWARMS" \
        --interval "$interval" --duration "$DURATION" --ramp "$RAMP" --server-pid "$SERVER_PID" \
        --label "$pname $name $proto" > "$OUT/run-$pname-$name-$proto.json" || echo "   load generator failed" >&2
      stop_server
    done
  done
done

echo "== smoke check" >&2
for name in js rust-n; do
  for target in "${TARGETS[@]}"; do
    [ "${target%%|*}" = "$name" ] || continue
    # shellcheck disable=SC2086
    start_server "$name" ${target#*|}
    ./target/release/wt-loadgen smoke --url "ws://127.0.0.1:$WS_PORT/" > "$OUT/smoke-$name.json"
    stop_server
  done
done

node -e '
const fs = require("fs"), os = require("os"), dir = process.argv[1];
const runs = fs.readdirSync(dir).filter((f) => f.startsWith("run-")).sort().map((f) => JSON.parse(fs.readFileSync(dir + "/" + f)));
const smoke = (n) => JSON.parse(fs.readFileSync(dir + "/smoke-" + n + ".json")).received;
const same = JSON.stringify(smoke("js")) === JSON.stringify(smoke("rust-n"));
const env = { cpu: os.cpus()[0].model, cores: os.availableParallelism(), os: process.platform + " " + os.release(), node: process.version };
fs.writeFileSync("bench/results/load.json", JSON.stringify({ env, smoke_same: same, runs }, null, 2));
console.error("smoke check: " + (same ? "same messages" : "DIFFERENT (see " + dir + "/smoke-*.json)"));
' "$OUT"
node bench/compare.ts > /dev/null
echo "load results: bench/results/load.json (spec §11 regenerated)" >&2
