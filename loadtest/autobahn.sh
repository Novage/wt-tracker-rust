#!/usr/bin/env bash
# WebSocket protocol conformance of the native transport (own framing): the Autobahn testsuite
# fuzzing client (docker image crossbario/autobahn-testsuite) against the ws-echo example, over
# ws:// and wss://. Compression cases (12.*, 13.*) are excluded: permessage-deflate is not
# negotiated. Reports: target/autobahn/reports/index.html. Exits non-zero if any case FAILED.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=target/autobahn
PORT=${PORT:-19001}
TLS_PORT=${TLS_PORT:-19443}
IMAGE=${IMAGE:-crossbario/autobahn-testsuite}
PLATFORM=${PLATFORM:-linux/amd64}

mkdir -p "$OUT"
rm -rf "$OUT/reports"
cargo build --release -q -p wt-server --example ws-echo
cargo build --release -q -p wt-loadgen
./target/release/wt-loadgen gen-cert "$OUT"

./target/release/examples/ws-echo "$PORT" > "$OUT/echo.log" 2>&1 &
ECHO=$!
./target/release/examples/ws-echo "$TLS_PORT" "$OUT/cert.pem" "$OUT/key.pem" > "$OUT/echo-tls.log" 2>&1 &
ECHO_TLS=$!
trap 'kill $ECHO $ECHO_TLS 2>/dev/null || true' EXIT
sleep 1

cat > "$OUT/fuzzingclient.json" <<EOF
{
  "outdir": "/out/reports",
  "servers": [
    { "agent": "wt-native", "url": "ws://host.docker.internal:$PORT" },
    { "agent": "wt-native-tls", "url": "wss://host.docker.internal:$TLS_PORT" }
  ],
  "cases": ["*"],
  "exclude-cases": ["12.*", "13.*"],
  "exclude-agent-cases": {}
}
EOF

docker run --rm --platform "$PLATFORM" -v "$PWD/$OUT:/out" "$IMAGE" \
  wstest -m fuzzingclient -s /out/fuzzingclient.json > "$OUT/wstest.log" 2>&1 \
  || { tail -20 "$OUT/wstest.log"; exit 1; }

node -e '
const index = require(process.argv[1]);
let bad = 0;
for (const [agent, cases] of Object.entries(index)) {
  const by = {};
  for (const [id, r] of Object.entries(cases)) {
    const key = r.behavior + (r.behaviorClose !== "OK" && r.behaviorClose !== "INFORMATIONAL" ? " / close " + r.behaviorClose : "");
    (by[key] ??= []).push(id);
    if (r.behavior === "FAILED" || r.behaviorClose === "FAILED") bad++;
  }
  console.log(agent + ": " + Object.keys(cases).length + " cases");
  for (const [k, ids] of Object.entries(by)) console.log("  " + k.padEnd(28) + ids.length + (k.startsWith("OK") ? "" : "  " + ids.slice(0, 12).join(" ")));
}
process.exit(bad ? 1 : 0);
' "$PWD/$OUT/reports/index.json"
