#!/usr/bin/env bash
# WebSocket protocol conformance of the native transport (own framing): the Autobahn testsuite
# fuzzing client (docker image crossbario/autobahn-testsuite) against the ws-echo example, over
# ws:// and wss://, including the permessage-deflate cases (12.*, 13.*; the echo server
# compresses every reply). Long runs through Docker Desktop's host.docker.internal sometimes
# drop a connect, after which wstest skips the rest of that run, so the cases run in groups,
# one server at a time, and a group whose run hit that is run again (up to 3 attempts). Reports:
# target/autobahn/reports-<server>-<group>/index.html, merged into target/autobahn/merged.json.
# Exits non-zero if any case FAILED or a group did not complete.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=target/autobahn
PORT=${PORT:-19001}
TLS_PORT=${TLS_PORT:-19443}
IMAGE=${IMAGE:-crossbario/autobahn-testsuite}
PLATFORM=${PLATFORM:-linux/amd64}
ATTEMPTS=${ATTEMPTS:-3}
CASE_GROUPS=('"1.*","2.*","3.*","4.*","5.*","6.*","7.*"' '"9.*","10.*"' '"12.*"' '"13.*"')
SERVERS=("wt-native|ws://host.docker.internal:$PORT" "wt-native-tls|wss://host.docker.internal:$TLS_PORT")

mkdir -p "$OUT"
rm -rf "$OUT"/reports* "$OUT"/wstest*.log
cargo build --release -q -p wt-server --example ws-echo
cargo build --release -q -p wt-loadgen
./target/release/wt-loadgen gen-cert "$OUT"

./target/release/examples/ws-echo "$PORT" > "$OUT/echo.log" 2>&1 &
ECHO=$!
./target/release/examples/ws-echo "$TLS_PORT" "$OUT/cert.pem" "$OUT/key.pem" > "$OUT/echo-tls.log" 2>&1 &
ECHO_TLS=$!
trap 'kill $ECHO $ECHO_TLS 2>/dev/null || true' EXIT
sleep 1

incomplete=""
for server in "${SERVERS[@]}"; do
  agent=${server%%|*}
  url=${server#*|}
  for g in "${!CASE_GROUPS[@]}"; do
    for attempt in $(seq "$ATTEMPTS"); do
      name="$agent-$g"
      cat > "$OUT/fuzzingclient-$name.json" <<EOF
{ "outdir": "/out/reports-$name", "servers": [{ "agent": "$agent", "url": "$url" }],
  "cases": [${CASE_GROUPS[$g]}], "exclude-cases": [], "exclude-agent-cases": {} }
EOF
      rm -rf "$OUT/reports-$name"
      # --add-host: host.docker.internal on Linux too (Docker Desktop defines it already).
      docker run --rm --platform "$PLATFORM" --add-host=host.docker.internal:host-gateway \
        -v "$PWD/$OUT:/out" "$IMAGE" \
        wstest -m fuzzingclient -s "/out/fuzzingclient-$name.json" > "$OUT/wstest-$name.log" 2>&1 \
        || { tail -20 "$OUT/wstest-$name.log"; exit 1; }
      grep -q "failed (User timeout" "$OUT/wstest-$name.log" || break
      echo "$agent group $g, attempt $attempt: a connection timed out; running the group again" >&2
      [ "$attempt" = "$ATTEMPTS" ] && incomplete="$incomplete $name"
    done
  done
done

node -e '
const fs = require("fs"), dir = process.argv[1];
const merged = {};
for (const d of fs.readdirSync(dir).filter((d) => d.startsWith("reports-")).sort()) {
  const file = `${dir}/${d}/index.json`;
  if (!fs.existsSync(file)) continue;
  for (const [agent, cases] of Object.entries(JSON.parse(fs.readFileSync(file)))) {
    merged[agent] = { ...(merged[agent] ?? {}), ...cases };
  }
}
fs.writeFileSync(dir + "/merged.json", JSON.stringify(merged));
let bad = 0;
for (const [agent, cases] of Object.entries(merged)) {
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
' "$PWD/$OUT"
if [ -n "$incomplete" ]; then
  echo "incomplete after $ATTEMPTS attempts:$incomplete" >&2
  exit 1
fi
