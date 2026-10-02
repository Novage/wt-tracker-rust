#!/usr/bin/env bash
# Load test against aquatic_ws (github.com/greatest-ape/aquatic, Linux / io_uring only): builds a
# Linux image with aquatic_ws (pinned AQUATIC_REV) and this workspace's server and load
# generator (loadtest/aquatic/Dockerfile), then runs the loadtest/run.sh profiles against
# aquatic-1 (1 socket + 1 swarm worker), aquatic-n, rust-1 and rust-n inside one container
# (server and client share its CPUs and loopback). Needs Docker; io_uring needs
# seccomp=unconfined and unlimited memlock. Writes bench/results/aquatic.json and regenerates
# spec §11.
set -euo pipefail
cd "$(dirname "$0")/.."

AQUATIC_REV=${AQUATIC_REV:-a2ddc4b323c5aaf844ce32b655b0ffc8c4836cde}
IMAGE=wt-aquatic-loadtest
OUT=target/loadtest-aquatic

docker build -q -f loadtest/aquatic/Dockerfile --build-arg AQUATIC_REV="$AQUATIC_REV" -t "$IMAGE" . > /dev/null
mkdir -p "$OUT" bench/results
docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1:-1 --ulimit nofile=65536:65536 \
  -e LIGHT_CONNS -e HEAVY_CONNS -e MEDIA_CONNS -e SWARMS -e DURATION -e RAMP \
  -v "$PWD/$OUT:/out" "$IMAGE"

node -e '
const fs = require("fs"), [dir, rev] = process.argv.slice(1);
const runs = fs.readdirSync(dir).filter((f) => f.startsWith("run-")).sort().map((f) => {
  const r = JSON.parse(fs.readFileSync(dir + "/" + f));
  const [profile, name] = r.label.split(" ");
  try { r.placement = JSON.parse(fs.readFileSync(`${dir}/stats-${profile}-${name}.json`)).placement; } catch {}
  return r;
});
const [cores, kernel] = fs.readFileSync(dir + "/env.txt", "utf8").split("\n");
const env = { where: "Docker Desktop Linux VM", cores: Number(cores), kernel, aquatic_rev: rev };
fs.writeFileSync("bench/results/aquatic.json", JSON.stringify({ env, runs }, null, 2));
' "$OUT" "$AQUATIC_REV"
node bench/compare.ts > /dev/null
echo "aquatic results: bench/results/aquatic.json (spec §11 regenerated)" >&2
