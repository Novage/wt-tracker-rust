#!/usr/bin/env bash
# Temporary (spec §12): why does wstest run out of memory on case 12.4.10 over ws:// on some CI
# runners (15 GB, killed), when the same case passes over wss:// in about 3 s? Runs one case
# (CASE, default 12.4.10) over ws:// against the ws-echo example and against Autobahn's own
# server (wstest -m testeeserver: an echo with permessage-deflate), with a 6 GB memory limit,
# sampling the wstest client's memory every 0.5 s. The ws-echo run also records the first 256
# bytes of each packet (tcpdump). Output in
# target/autobahn-debug/: mem-<server>.txt, wstest-<server>.log, reports-<server>/, ws-echo.pcap.
set -uo pipefail
cd "$(dirname "$0")/.."

OUT=target/autobahn-debug
IMAGE=${IMAGE:-crossbario/autobahn-testsuite}
CASE=${CASE:-12.4.10}
rm -rf "$OUT" && mkdir -p "$OUT"
cargo build --release -q -p wt-server --example ws-echo
./target/release/examples/ws-echo 19001 > "$OUT/echo.log" 2>&1 &
ECHO=$!
docker run -d --name ref-echo -p 19002:9001 "$IMAGE" wstest -m testeeserver -w ws://0.0.0.0:9001 > /dev/null
trap 'kill $ECHO 2>/dev/null; docker rm -f ref-echo > /dev/null 2>&1' EXIT
sleep 3

# $1 = server name, $2 = url
run() {
  cat > "$OUT/spec-$1.json" <<EOF
{ "outdir": "/out/reports-$1", "servers": [{ "agent": "$1", "url": "$2" }],
  "cases": ["$CASE"], "exclude-cases": [], "exclude-agent-cases": {} }
EOF
  docker run -d --name "wstest-$1" --memory 6g --add-host=host.docker.internal:host-gateway \
    -e PYTHONUNBUFFERED=1 -v "$PWD/$OUT:/out" "$IMAGE" \
    wstest -m fuzzingclient -s "/out/spec-$1.json" > /dev/null
  while [ "$(docker inspect -f '{{.State.Running}}' "wstest-$1")" = true ]; do
    echo "$(date -u +%T.%N | cut -c1-12) $(docker stats --no-stream --format '{{.MemUsage}} {{.CPUPerc}}' "wstest-$1")"
    sleep 0.5
  done > "$OUT/mem-$1.txt"
  docker logs "wstest-$1" > "$OUT/wstest-$1.log" 2>&1
  echo "== $1: $(docker inspect -f 'exit={{.State.ExitCode}} oom_killed={{.State.OOMKilled}}' "wstest-$1"), $(wc -l < "$OUT/mem-$1.txt") samples"
  tail -4 "$OUT/mem-$1.txt"
  grep -ho '"behavior": "[A-Z-]*"\|"duration": [0-9]*' "$OUT"/reports-"$1"/*.json 2>/dev/null | head -2
  docker rm -f "wstest-$1" > /dev/null
}

sudo tcpdump -i any -s 256 -c 300000 -w "$OUT/ws-echo.pcap" 'tcp port 19001' 2> "$OUT/tcpdump.log" &
DUMP=$!
sleep 1
run ws-echo ws://host.docker.internal:19001
sudo kill -INT $DUMP 2>/dev/null; wait $DUMP 2>/dev/null
run autobahn-testee ws://host.docker.internal:19002
sudo chmod a+r "$OUT/ws-echo.pcap" 2>/dev/null || true
