// One shard of the JS scaling run: builds its tracker, reports ready, then on "go" times the
// re-announce passes. Mirrors the per-thread body of `scaling` in crates/wt-bench/src/main.rs.

import { parentPort, workerData } from "node:worker_threads";
import {
  MANY_SWARMS_SWARMS,
  MP_CONNS,
  MP_PEERS,
  makeConns,
  makeIds,
  mpMemberships,
  newTracker,
  runAnnounceList,
  type Ids,
} from "./scenarios.ts";

const { shard, shards, passes } = workerData as { shard: number; shards: number; passes: number };

const ids: Ids = {
  peers: makeIds("p", MP_PEERS),
  swarms: makeIds("h", MANY_SWARMS_SWARMS),
  conns: makeConns(MP_CONNS),
};
const list = mpMemberships(shard, shards);
const tracker = newTracker();
runAnnounceList(tracker, ids, list, "started");

parentPort!.once("message", () => {
  const start = process.hrtime.bigint();
  for (let i = 0; i < passes; i++) runAnnounceList(tracker, ids, list, undefined);
  const elapsedNs = Number(process.hrtime.bigint() - start);
  tracker.dispose();
  parentPort!.postMessage({ ops: (list.length / 3) * passes, elapsedNs });
});
parentPort!.postMessage({ ready: true });
