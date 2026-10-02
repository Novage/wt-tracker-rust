#!/usr/bin/env bash
# Runs inside the loadtest/aquatic.sh container: every target × profile, one fresh server
# process per run, the same load generator as loadtest/run.sh. Results: /out/run-*.json.
set -euo pipefail
OUT=/out
# A new port per run: aquatic leaves server-side TIME_WAIT sockets on its port, and its listener
# does not set SO_REUSEADDR, so a plain bind to that port fails for a while (EADDRINUSE).
PORT=18100
N=$(nproc)
LIGHT_CONNS=${LIGHT_CONNS:-3000}
HEAVY_CONNS=${HEAVY_CONNS:-4000}
MEDIA_CONNS=${MEDIA_CONNS:-3000}
SWARMS=${SWARMS:-100}
DURATION=${DURATION:-15}
RAMP=${RAMP:-1000}
PROFILES=(
  "light|$LIGHT_CONNS|5|ws wss|"
  "heavy|$HEAVY_CONNS|1|ws|"
  "media|$MEDIA_CONNS|5|ws|--streams 2 --qualities 4 --switch 10 --overlap 5"
)
# aquatic-n: N threads like rust-n, most of them socket workers (they do the WebSocket and TLS
# work; swarm workers only the tracker state).
SWARM_N=$(( N / 4 > 0 ? N / 4 : 1 ))
SOCKET_N=$(( N - SWARM_N ))
TARGETS=("aquatic-1|1|1" "aquatic-n|$SOCKET_N|$SWARM_N" "rust-1|1|" "rust-n|$N|")

ulimit -n 65536
mkdir -p "$OUT"
rm -f "$OUT"/run-*.json
wt-loadgen gen-cert "$OUT"
aquatic_ws -p > "$OUT/aquatic-default.toml"

aquatic_config() { # socket_workers swarm_workers tls
  sed -e "s/^socket_workers = .*/socket_workers = $1/" \
      -e "s/^swarm_workers = .*/swarm_workers = $2/" \
      -e "s/^address = .*/address = \"127.0.0.1:$PORT\"/" \
      -e "s/^enable_tls = .*/enable_tls = $3/" \
      -e "s|^tls_certificate_path = .*|tls_certificate_path = \"$OUT/cert.pem\"|" \
      -e "s|^tls_private_key_path = .*|tls_private_key_path = \"$OUT/key.pem\"|" \
      -e "s/^peer_announce_interval = .*/peer_announce_interval = 120/" \
      "$OUT/aquatic-default.toml"
}
rust_config() { # workers tls
  local tls="" reuse=""
  [ "$2" = true ] && tls=",\"key_file_name\":\"$OUT/key.pem\",\"cert_file_name\":\"$OUT/cert.pem\""
  [ "$1" -gt 1 ] && reuse=",\"reusePort\":true"
  echo "{\"servers\":[{\"server\":{\"host\":\"127.0.0.1\",\"port\":$PORT$tls}}],\"workers\":$1$reuse,\"tracker\":{\"announceInterval\":120}}"
}

wait_port() {
  for _ in $(seq 100); do
    (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
    sleep 0.1
  done
  return 1
}

for profile in "${PROFILES[@]}"; do
  IFS='|' read -r pname conns interval protos extra <<< "$profile"
  for target in "${TARGETS[@]}"; do
    IFS='|' read -r name a b <<< "$target"
    for proto in $protos; do
      PORT=$((PORT + 1))
      tls=false; url="ws://127.0.0.1:$PORT/"
      [ "$proto" = wss ] && tls=true && url="wss://localhost:$PORT/"
      echo "== $pname $name $proto" >&2
      if [[ $name == aquatic* ]]; then
        aquatic_config "$a" "$b" "$tls" > "$OUT/aquatic.toml"
        aquatic_ws -c "$OUT/aquatic.toml" > "$OUT/server.log" 2>&1 &
      else
        rust_config "$a" "$tls" > "$OUT/rust.json"
        wt-tracker "$OUT/rust.json" > "$OUT/server.log" 2>&1 &
      fi
      pid=$!
      if ! wait_port; then
        echo "   $name did not start:" >&2; tail -5 "$OUT/server.log" >&2
        kill "$pid" 2>/dev/null || true; continue
      fi
      run="$OUT/run-$pname-$name-$proto.json"
      # shellcheck disable=SC2086
      wt-loadgen load --url "$url" --ca "$OUT/cert.pem" --conns "$conns" --swarms "$SWARMS" \
        --interval "$interval" --duration "$DURATION" --ramp "$RAMP" --server-pid "$pid" $extra \
        --label "$pname $name $proto" > "$run" || echo "   load generator failed" >&2
      if [[ $name == rust* ]] && [ "$proto" = ws ]; then
        curl -s "http://127.0.0.1:$PORT/stats.json" > "$OUT/stats-$pname-$name.json" || true
      fi
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      sleep 1
    done
  done
done
{ nproc; uname -srm; aquatic_ws -V 2>/dev/null || true; } > "$OUT/env.txt"
