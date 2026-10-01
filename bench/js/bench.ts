// JS twin of crates/wt-bench: the same scenarios against wt-tracker's FastTracker.
// Run: node --expose-gc bench/js/bench.ts > bench/results/js.json

import os from "node:os";
import { StringDecoder } from "node:string_decoder";
import { Worker } from "node:worker_threads";
import {
  ANSWERS,
  MANY_SWARMS_PEERS,
  MANY_SWARMS_SWARMS,
  MP_CONNS,
  MP_MEMBERSHIPS,
  MP_PEERS,
  ONE_SWARM_PEERS,
  PROTO_FRAMES,
  PROTO_MSGS,
  announce,
  announceFrame,
  answerFrame,
  answerTarget,
  protoMemberships,
  counter,
  makeConns,
  makeIds,
  mpMemberships,
  newTracker,
  resetCounter,
  runAnnounceList,
  takeSweep,
  type Ids,
  type Tracker,
} from "./scenarios.ts";

const WARMUP = 2;
const RUNS = 5;
const SCALING_THREADS = [1, 2, 4, 8];
const SCALING_PASSES = 3;
const SCALING_TRIALS = 3;

const gc = (globalThis as { gc?: () => void }).gc ?? (() => {});
const nowNs = () => process.hrtime.bigint();

type Result = Record<string, unknown>;

function bench(name: string, ops: number, run: () => bigint): Result {
  process.stderr.write(name.padEnd(28));
  for (let i = 0; i < WARMUP; i++) run();
  const times: number[] = [];
  let messages = {};
  for (let i = 0; i < RUNS; i++) {
    times.push(Number(run()));
    messages = { ...counter };
  }
  times.sort((a, b) => a - b);
  const median = times[Math.floor(RUNS / 2)];
  process.stderr.write(
    `${(median / ops).toFixed(1).padStart(10)} ns/op  (min ${(times[0] / ops).toFixed(1)})  ${(median / 1e6).toFixed(2).padStart(8)} ms/run\n`,
  );
  return {
    name,
    ops,
    median_ns_per_op: median / ops,
    min_ns_per_op: times[0] / ops,
    median_ms_per_run: median / 1e6,
    ops_per_sec: ops / (median / 1e9),
    messages,
  };
}

/** Runs `setup` untimed on a fresh tracker, then times `measure`. */
function timed(
  setup: (t: Tracker) => void,
  measure: (t: Tracker) => void,
): bigint {
  const tracker = newTracker();
  setup(tracker);
  gc();
  resetCounter();
  const start = nowNs();
  measure(tracker);
  const elapsed = nowNs() - start;
  tracker.dispose();
  return elapsed;
}

function passOn(tracker: Tracker, measure: (t: Tracker) => void): bigint {
  resetCounter();
  const start = nowNs();
  measure(tracker);
  return nowNs() - start;
}

const ids: Ids = {
  peers: makeIds("p", MANY_SWARMS_PEERS),
  swarms: makeIds("h", MANY_SWARMS_SWARMS),
  conns: makeConns(MANY_SWARMS_PEERS),
};
const mp = mpMemberships();

const joinOneSwarm = (t: Tracker) => {
  const infoHash = ids.swarms[0];
  for (let i = 0; i < ONE_SWARM_PEERS; i++) {
    announce(t, ids.conns[i], infoHash, ids.peers[i], "started");
  }
};
const reannounceOneSwarm = (t: Tracker) => {
  const infoHash = ids.swarms[0];
  for (let i = 0; i < ONE_SWARM_PEERS; i++) {
    announce(t, ids.conns[i], infoHash, ids.peers[i], undefined);
  }
};
const joinManySwarms = (t: Tracker) => {
  for (let i = 0; i < MANY_SWARMS_PEERS; i++) {
    announce(t, ids.conns[i], ids.swarms[i % MANY_SWARMS_SWARMS], ids.peers[i], "started");
  }
};
const mpJoin = (t: Tracker) => runAnnounceList(t, ids, mp, "started");
const reannounce = (t: Tracker) => runAnnounceList(t, ids, mp, undefined);

const answerMessage: Record<string, unknown> = {
  action: "announce",
  info_hash: "",
  peer_id: "",
  to_peer_id: "",
  offer_id: "o0",
  answer: { type: "answer", sdp: "x" },
};
const answers = (t: Tracker) => {
  const from = ids.conns[0];
  for (let i = 0; i < ANSWERS; i++) {
    // processAnswer clears to_peer_id, so set it every time.
    answerMessage.to_peer_id = ids.peers[(i * 2_654_435_761) % MP_PEERS];
    t.processMessage(answerMessage, from);
  }
};

const stopMessage: Record<string, unknown> = {
  action: "announce",
  event: "stopped",
  info_hash: "",
  peer_id: "",
};
const stopAll = (t: Tracker) => {
  for (let i = 0; i < mp.length; i += 3) {
    stopMessage.info_hash = ids.swarms[mp[i + 2]];
    stopMessage.peer_id = ids.peers[mp[i + 1]];
    t.processMessage(stopMessage, ids.conns[mp[i]]);
  }
};
const disconnectAll = (t: Tracker) => {
  for (let c = 0; c < MP_CONNS; c++) t.disconnect(ids.conns[c]);
};

// FastTracker reads performance.now(); the expire scenario uses a fake clock (ms).
const realNow = performance.now.bind(performance);
function withClock<T>(fn: (setNow: (ms: number) => void) => T): T {
  let fake = 0;
  performance.now = () => fake;
  try {
    return fn((ms) => (fake = ms));
  } finally {
    performance.now = realNow;
  }
}

function expireSweep(): bigint {
  return withClock((setNow) => {
    const tracker = newTracker();
    const sweep = takeSweep();
    setNow(0);
    mpJoin(tracker);
    setNow(30_000);
    for (let i = 0; i < mp.length; i += 3) {
      if (mp[i + 1] % 2 === 0) {
        announce(tracker, ids.conns[mp[i]], ids.swarms[mp[i + 2]], ids.peers[mp[i + 1]], undefined, false);
      }
    }
    setNow(45_000);
    gc();
    resetCounter();
    const start = nowNs();
    sweep();
    const elapsed = nowNs() - start;
    tracker.dispose();
    return elapsed;
  });
}

function memory(name: string, build: (t: Tracker) => void): Result {
  gc();
  gc();
  const before = process.memoryUsage().heapUsed;
  const tracker = newTracker();
  build(tracker);
  gc();
  gc();
  const bytes = process.memoryUsage().heapUsed - before;
  let memberships = 0;
  for (const swarm of tracker.swarms.values()) memberships += swarm.peers.length;
  const result = {
    name,
    bytes,
    memberships,
    peers: tracker.peers.size,
    swarms: tracker.swarms.size,
    bytes_per_membership: bytes / memberships,
  };
  process.stderr.write(
    `${name.padEnd(20)} ${(bytes / 2 ** 20).toFixed(1).padStart(8)} MiB  ${(bytes / memberships).toFixed(1).padStart(6)} B/membership\n`,
  );
  tracker.dispose();
  return result;
}

async function scalingRun(mode: "weak" | "strong", threads: number): Promise<number> {
  const workers = Array.from(
    { length: threads },
    (_, t) =>
      new Worker(new URL("./scaling-worker.ts", import.meta.url), {
        workerData: { shard: mode === "strong" ? t : 0, shards: mode === "strong" ? threads : 1, passes: SCALING_PASSES },
      }),
  );
  const next = (w: Worker) =>
    new Promise<{ ops: number; elapsedNs: number }>((resolve, reject) => {
      w.once("message", resolve);
      w.once("error", reject);
    });
  await Promise.all(workers.map(next)); // built and ready
  const done = workers.map(next);
  for (const w of workers) w.postMessage("go");
  const results = await Promise.all(done);
  await Promise.all(workers.map((w) => w.terminate()));
  const ops = results.reduce((sum, r) => sum + r.ops, 0);
  const slowest = Math.max(...results.map((r) => r.elapsedNs));
  return ops / (slowest / 1e9);
}

async function scaling(mode: "weak" | "strong"): Promise<Result[]> {
  const results = [];
  for (const threads of SCALING_THREADS) {
    const trials = [];
    for (let i = 0; i < SCALING_TRIALS; i++) trials.push(await scalingRun(mode, threads));
    trials.sort((a, b) => a - b);
    const opsPerSec = trials[Math.floor(SCALING_TRIALS / 2)];
    process.stderr.write(`scaling ${mode.padEnd(6)} ${threads} threads: ${(opsPerSec / 1e6).toFixed(2).padStart(8)} M announces/s\n`);
    results.push({ mode, threads, ops_per_sec: opsPerSec });
  }
  return results;
}

// Memory builds create id strings on the fly, like JSON.parse does in the real server, so the
// strings the tracker retains are counted (the pre-generated id arrays are not).
const pid = (i: number) => "p" + String(i).padStart(19, "0");
const hid = (i: number) => "h" + String(i).padStart(19, "0");
const memoryResults = [
  memory("multi_peer_join", (t) => {
    for (let i = 0; i < mp.length; i += 3) {
      announce(t, ids.conns[mp[i]], hid(mp[i + 2]), pid(mp[i + 1]), "started");
    }
  }),
  memory("join_many_swarms", (t) => {
    for (let i = 0; i < MANY_SWARMS_PEERS; i++) {
      announce(t, ids.conns[i], hid(i % MANY_SWARMS_SWARMS), pid(i), "started");
    }
  }),
];

const scenarios: Result[] = [];
scenarios.push(bench("join_one_swarm", ONE_SWARM_PEERS, () => timed(() => {}, joinOneSwarm)));
scenarios.push(bench("join_many_swarms", MANY_SWARMS_PEERS, () => timed(() => {}, joinManySwarms)));
scenarios.push(bench("multi_peer_join", MP_MEMBERSHIPS, () => timed(() => {}, mpJoin)));
{
  const tracker = newTracker();
  mpJoin(tracker);
  gc();
  scenarios.push(bench("reannounce", MP_MEMBERSHIPS, () => passOn(tracker, reannounce)));
  scenarios.push(bench("answer", ANSWERS, () => passOn(tracker, answers)));
  tracker.dispose();
}
{
  const tracker = newTracker();
  joinOneSwarm(tracker);
  gc();
  scenarios.push(bench("reannounce_one_swarm", ONE_SWARM_PEERS, () => passOn(tracker, reannounceOneSwarm)));
  tracker.dispose();
}
scenarios.push(bench("stop_all", MP_MEMBERSHIPS, () => timed(mpJoin, stopAll)));
scenarios.push(bench("disconnect_all", MP_CONNS, () => timed(mpJoin, disconnectAll)));
scenarios.push(bench("expire_sweep", MP_MEMBERSHIPS, expireSweep));

// ---- protocol scenarios ----
{
  const members = protoMemberships();
  const announces: [number, Buffer][] = [];
  const answers: [number, Buffer][] = [];
  for (let n = 0; n < PROTO_FRAMES; n++) {
    const [c, p, s] = [members[n * 3], members[n * 3 + 1], members[n * 3 + 2]];
    announces.push([c, Buffer.from(announceFrame(ids.swarms[s], ids.peers[p], n))]);
    answers.push([c, Buffer.from(answerFrame(ids.swarms[s], ids.peers[p], ids.peers[answerTarget(n)], n))]);
  }
  // As uws-tracker.onMessage: StringDecoder over the received bytes, then JSON.parse.
  const decoder = new StringDecoder();
  const parse = (frame: Buffer) => JSON.parse(decoder.end(frame)) as Record<string, unknown>;

  scenarios.push(
    bench("proto_parse_announce", PROTO_MSGS, () => {
      resetCounter();
      const start = nowNs();
      for (let i = 0; i < PROTO_MSGS; i++) {
        const frame = announces[i % PROTO_FRAMES][1];
        parse(frame);
        counter.bytes += frame.length;
      }
      return nowNs() - start;
    }),
  );

  // Encoding only: the same reused message objects as FastTracker, then JSON.stringify.
  const parsed = announces.map(([, f]) => parse(f));
  const reply = { action: "announce", interval: 0, info_hash: "", complete: 0, incomplete: 0 };
  const offerMsg = { action: "announce", info_hash: "", offer_id: undefined as unknown, peer_id: "", offer: { type: "offer", sdp: undefined as unknown } };
  scenarios.push(
    bench("proto_encode_announce", PROTO_MSGS, () => {
      resetCounter();
      const start = nowNs();
      for (let i = 0; i < PROTO_MSGS; i++) {
        const m = parsed[i % PROTO_FRAMES];
        reply.interval = 20;
        reply.info_hash = m.info_hash as string;
        reply.complete = 1;
        reply.incomplete = 59;
        counter.bytes += JSON.stringify(reply).length;
        counter.replies++;
        for (const item of m.offers as { offer: { sdp: unknown }; offer_id: unknown }[]) {
          offerMsg.info_hash = m.info_hash as string;
          offerMsg.offer_id = item.offer_id;
          offerMsg.peer_id = m.peer_id as string;
          offerMsg.offer.sdp = item.offer.sdp;
          counter.bytes += JSON.stringify(offerMsg).length;
          counter.offers++;
        }
      }
      return nowNs() - start;
    }),
  );

  const pipeline = (name: string, frames: [number, Buffer][]) => {
    const tracker = newTracker(true);
    mpJoin(tracker);
    gc();
    scenarios.push(
      bench(name, PROTO_MSGS, () => {
        resetCounter();
        const start = nowNs();
        for (let i = 0; i < PROTO_MSGS; i++) {
          const [c, frame] = frames[i % PROTO_FRAMES];
          tracker.processMessage(parse(frame), ids.conns[c]);
        }
        return nowNs() - start;
      }),
    );
    tracker.dispose();
  };
  pipeline("pipeline_reannounce", announces);
  pipeline("pipeline_answer", answers);
}

const scalingResults = [...(await scaling("strong")), ...(await scaling("weak"))];

process.stdout.write(
  JSON.stringify(
    {
      runtime: "js",
      env: {
        node: process.version,
        cpu: os.cpus()[0]?.model,
        parallelism: os.availableParallelism(),
      },
      scenarios,
      memory: memoryResults,
      scaling: scalingResults,
    },
    null,
    2,
  ) + "\n",
);
