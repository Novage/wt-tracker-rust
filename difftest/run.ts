// Differential test: runs identical random operation traces through the JS FastTracker
// (../wt-tracker) and the Rust Shard (crates/wt-difftest), then checks after every op that
//   1. both produced the same output and state, and
//   2. each output is correct per docs/SPEC.md (so identical bugs cannot pass).
//
// Offer receivers are random on both sides and swarm order may legitimately differ, so offers
// are compared by count and offer ids, and each receiver is checked to be a valid other member.
//
// Usage: node difftest/run.ts [traces=200] [ops=500] [first-seed=1]
//   WT_TRACKER_DIR=<path to wt-tracker> (default ../wt-tracker)

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const root = path.join(import.meta.dirname, "..");
const trackerDir = path.resolve(process.env.WT_TRACKER_DIR ?? path.join(root, "../wt-tracker"));
const [TRACES, OPS, FIRST_SEED] = [200, 500, 1].map((d, i) => Number(process.argv[2 + i] ?? d));
const SELECTIONS = ["sample", "window", "round_robin"];
const MAX_OFFERS = 20;
const INTERVAL = 20;

// ---- deterministic PRNG for trace generation and JS Math.random ----

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---- trace generation ----

type Op = Record<string, unknown> & { op: string };

const PEER_IDS = ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "ÿp\u0001", "пір"];
const INFO_HASHES = ["h0", "h1", "h2", "éh"];

function generateTrace(seed: number, opsCount: number): { seed: number; ops: Op[] } {
  const rnd = mulberry32(seed * 7919 + 17);
  const int = (n: number) => Math.floor(rnd() * n);
  const pick = <T>(xs: readonly T[]): T => xs[int(xs.length)];
  const weighted = <T>(choices: [number, T][]): T => {
    let r = rnd() * choices.reduce((s, [w]) => s + w, 0);
    for (const [w, v] of choices) if ((r -= w) < 0) return v;
    return choices[choices.length - 1][1];
  };

  const conns = 1 + int(6);
  const peers = PEER_IDS.slice(0, 1 + int(PEER_IDS.length));
  const hashes = INFO_HASHES.slice(0, 1 + int(INFO_HASHES.length));
  // Each peer_id usually stays on its home connection; several peer_ids share a connection.
  const home = new Map(peers.map((p, i) => [p, i % conns]));

  const ops: Op[] = [];
  for (let i = 0; i < opsCount; i++) {
    const peer = pick(peers);
    const kind = weighted<string>([
      [55, "announce"], [10, "answer"], [10, "stop"], [5, "scrape"],
      [4, "disconnect"], [10, "advance"], [6, "expire"],
    ]);
    if (kind === "announce") {
      if (rnd() < 0.08) home.set(peer, int(conns)); // peer_id moves to another connection
      ops.push({
        op: "announce",
        conn: home.get(peer)!,
        info_hash: pick(hashes),
        peer_id: peer,
        event: weighted<string | null>([[30, "started"], [50, null], [20, "completed"]]),
        left: weighted<number | null>([[80, null], [10, 0], [10, 7]]),
        numwant: weighted<number | null>([[10, null], [5, 0], [10, 1], [15, 3], [40, 10], [20, 50]]),
        offers: weighted<number | null>([[10, null], [90, int(13)]]),
      });
    } else if (kind === "answer") {
      ops.push({
        op: "answer",
        conn: home.get(peer)!,
        info_hash: pick(hashes),
        peer_id: peer,
        to_peer_id: rnd() < 0.9 ? pick(peers) : "nobody",
        id: i,
      });
    } else if (kind === "stop") {
      ops.push({ op: "stop", conn: home.get(peer)!, info_hash: rnd() < 0.9 ? pick(hashes) : "nope", peer_id: peer });
    } else if (kind === "scrape") {
      const info_hash = weighted<unknown>([
        [30, null],
        [40, rnd() < 0.8 ? pick(hashes) : "nope"],
        [30, [pick(hashes), pick(hashes), "nope"]],
      ]);
      ops.push({ op: "scrape", conn: int(conns), info_hash });
    } else if (kind === "disconnect") {
      ops.push({ op: "disconnect", conn: int(conns) });
    } else if (kind === "advance") {
      ops.push({ op: "advance", secs: 1 + int(30) });
    } else {
      ops.push({ op: "expire" });
    }
  }
  return { seed, ops };
}

// ---- results ----

type State = { swarms: Record<string, { peers: string[]; complete: number }>; peers: Record<string, number> };
type Result = {
  error: boolean;
  replies: unknown[][];
  offers: [number, string, string, number][];
  answers: unknown[][];
  removed: [string, number][];
  scrape: Record<string, unknown> | null;
  state: State;
  invariants?: string | null;
};

const cmp = (a: unknown, b: unknown) => {
  const [x, y] = [JSON.stringify(a), JSON.stringify(b)];
  return x < y ? -1 : x > y ? 1 : 0;
};
/** Order-independent canonical form: sorted object keys and sorted string arrays. */
function canonical(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") {
    return Object.fromEntries(
      Object.keys(v).sort().map((k) => [k, canonical((v as Record<string, unknown>)[k])]),
    );
  }
  return v;
}
function canonicalState(s: State): unknown {
  const swarms: Record<string, unknown> = {};
  for (const [h, sw] of Object.entries(s.swarms)) swarms[h] = { peers: [...sw.peers].sort(), complete: sw.complete };
  return canonical({ swarms, peers: s.peers });
}

// ---- JS runner ----

let lastSweep: (() => void) | undefined;
globalThis.setInterval = ((fn: () => void) => {
  lastSweep = fn;
  return 0;
}) as unknown as typeof setInterval;
let fakeNowMs = 0;
performance.now = () => fakeNowMs;

const { FastTracker } = (await import(path.join(trackerDir, "src/fast-tracker.ts"))) as typeof import(
  "../../wt-tracker/src/fast-tracker.ts"
);
const { TrackerError } = (await import(path.join(trackerDir, "src/tracker.ts"))) as typeof import(
  "../../wt-tracker/src/tracker.ts"
);

type Conn = { id: number };

function runJs(trace: { seed: number; ops: Op[] }): Result[] {
  Math.random = mulberry32(trace.seed);
  fakeNowMs = 0;
  let out: Result;
  const tracker = new FastTracker<Conn>({}, (json, conn) => {
    if (json.action === "scrape") out.scrape = structuredClone(json.files as Record<string, unknown>);
    else if (json.offer !== undefined) out.offers.push([conn.id, json.peer_id as string, json.info_hash as string, json.offer_id as number]);
    else if (json.answer !== undefined) out.answers.push([conn.id, json.offer_id]);
    else if (json.interval !== undefined) out.replies.push([conn.id, json.info_hash, json.interval, json.complete, json.incomplete]);
    else throw new Error("unexpected message " + JSON.stringify(json));
  });
  const sweep = lastSweep!;
  tracker.onRemovePeer = (peerId, conn) => out.removed.push([peerId, conn.id]);

  const conns = new Map<number, Conn>();
  const conn = (id: number) => {
    let c = conns.get(id);
    if (!c) conns.set(id, (c = { id }));
    return c;
  };
  const offerList = (n: number) => Array.from({ length: n }, (_, i) => ({ offer: { type: "offer", sdp: "x" }, offer_id: i }));

  const results: Result[] = [];
  for (const op of trace.ops) {
    out = { error: false, replies: [], offers: [], answers: [], removed: [], scrape: null, state: { swarms: {}, peers: {} } };
    try {
      if (op.op === "announce") {
        const msg: Record<string, unknown> = { action: "announce", info_hash: op.info_hash, peer_id: op.peer_id };
        if (op.event !== null) msg.event = op.event;
        if (op.left !== null) msg.left = op.left;
        if (op.numwant !== null) msg.numwant = op.numwant;
        if (op.offers !== null) msg.offers = offerList(op.offers as number);
        tracker.processMessage(msg, conn(op.conn as number));
      } else if (op.op === "answer") {
        tracker.processMessage(
          { action: "announce", info_hash: op.info_hash, peer_id: op.peer_id, to_peer_id: op.to_peer_id, answer: { type: "answer", sdp: "x" }, offer_id: op.id },
          conn(op.conn as number),
        );
      } else if (op.op === "stop") {
        tracker.processMessage({ action: "announce", event: "stopped", info_hash: op.info_hash, peer_id: op.peer_id }, conn(op.conn as number));
      } else if (op.op === "scrape") {
        const msg: Record<string, unknown> = { action: "scrape" };
        if (op.info_hash !== null) msg.info_hash = op.info_hash;
        tracker.processMessage(msg, conn(op.conn as number));
      } else if (op.op === "disconnect") {
        tracker.disconnect(conn(op.conn as number));
      } else if (op.op === "advance") {
        fakeNowMs += (op.secs as number) * 1000;
      } else if (op.op === "expire") {
        sweep();
      }
    } catch (e) {
      if (!(e instanceof TrackerError)) throw e;
      out.error = true;
    }
    for (const [h, sw] of tracker.swarms) out.state.swarms[h] = { peers: sw.peers.map((p) => p.peerId), complete: sw.completedCount };
    for (const [id, p] of tracker.peers) out.state.peers[id] = p.connection.id;
    results.push(out);
  }
  tracker.dispose();
  return results;
}

// ---- spec checks (applied to each side independently) ----

const counts = { ops: 0, offers: 0, fullFanOut: 0, partialFanOut: 0, connChanges: 0, errors: 0, removed: 0, expired: 0 };

function specCheck(op: Op, r: Result, prev: State, side: string): string | undefined {
  const s = r.state;
  const fail = (m: string) => `${side}: ${m}`;
  if (r.invariants) return fail(`invariants: ${r.invariants}`);
  if (side === "rust") {
    counts.errors += Number(r.error);
    counts.removed += r.removed.length;
    if (op.op === "expire") counts.expired += r.removed.length;
  }

  // Removed peers = peers gone from the state, or moved to another connection.
  const expectedRemoved = Object.entries(prev.peers)
    .filter(([id, c]) => s.peers[id] !== c)
    .map(([id, c]) => [id, c]);
  if (cmp([...r.removed].sort(cmp), expectedRemoved.sort(cmp)) !== 0)
    return fail(`removed ${JSON.stringify(r.removed)}, expected ${JSON.stringify(expectedRemoved)}`);

  const quiet = (...allowed: string[]) => {
    for (const k of ["replies", "offers", "answers"] as const) if (!allowed.includes(k) && r[k].length) return fail(`unexpected ${k}`);
    if (!allowed.includes("scrape") && r.scrape !== null) return fail("unexpected scrape");
    if (!allowed.includes("error") && r.error) return fail("unexpected error");
  };

  if (op.op === "announce") {
    const q = quiet("replies", "offers");
    if (q) return q;
    const [ih, pid, conn] = [op.info_hash as string, op.peer_id as string, op.conn as number];
    const sw = s.swarms[ih];
    if (!sw?.peers.includes(pid) || s.peers[pid] !== conn) return fail("announcer not in swarm on its connection");
    const reply = [conn, ih, INTERVAL, sw.complete, sw.peers.length - sw.complete];
    if (cmp(r.replies, [reply]) !== 0) return fail(`reply ${JSON.stringify(r.replies)}, expected ${JSON.stringify([reply])}`);

    const others = sw.peers.length - 1;
    const n = op.offers === null || op.numwant === null || others < 1
      ? 0
      : Math.max(0, Math.min(others, op.offers as number, MAX_OFFERS, op.numwant as number));
    if (r.offers.length !== n) return fail(`${r.offers.length} offers, expected ${n}`);
    const ids = r.offers.map((o) => o[3]).sort((a, b) => a - b);
    if (cmp(ids, Array.from({ length: n }, (_, i) => i)) !== 0) return fail(`offer ids ${ids}`);
    if (r.offers.some((o) => o[1] !== pid || o[2] !== ih)) return fail("offer from/info_hash");
    // Receivers: a sub-multiset of the other members' connections (all of them if n == others).
    const pool = sw.peers.filter((p) => p !== pid).map((p) => s.peers[p]);
    for (const [to] of r.offers) {
      const i = pool.indexOf(to);
      if (i < 0) return fail(`offer to ${to}, which is not another member's connection`);
      pool.splice(i, 1);
    }
    if (n === others && pool.length) return fail("full fan-out missed members");
    if (side === "rust") {
      counts.offers += n;
      if (n > 0) n === others ? counts.fullFanOut++ : counts.partialFanOut++;
      if (prev.peers[pid] !== undefined && prev.peers[pid] !== conn) counts.connChanges++;
    }
  } else if (op.op === "answer") {
    const to = prev.peers[op.to_peer_id as string];
    if (to === undefined) return quiet("error") ?? (r.error ? undefined : fail("answer to unknown peer did not fail"));
    const q = quiet("answers");
    if (q) return q;
    if (cmp(r.answers, [[to, op.id]]) !== 0) return fail(`answers ${JSON.stringify(r.answers)}`);
  } else if (op.op === "scrape") {
    const q = quiet("scrape");
    if (q) return q;
    const entry = (h: string) => {
      const sw = s.swarms[h];
      const c = sw?.complete ?? 0;
      return { complete: c, incomplete: (sw?.peers.length ?? 0) - c, downloaded: c };
    };
    const hashes = op.info_hash === null ? Object.keys(s.swarms) : ([] as string[]).concat(op.info_hash as string);
    const expected = Object.fromEntries(hashes.map((h) => [h, entry(h)]));
    if (cmp(canonical(r.scrape), canonical(expected)) !== 0) return fail(`scrape ${JSON.stringify(r.scrape)}, expected ${JSON.stringify(expected)}`);
  } else {
    const q = quiet();
    if (q) return q;
  }
}

function compare(js: Result, rust: Result): string | undefined {
  if (js.error !== rust.error) return `error: js ${js.error}, rust ${rust.error}`;
  for (const k of ["replies", "answers"] as const) {
    if (cmp(js[k], rust[k]) !== 0) return `${k}: js ${JSON.stringify(js[k])}, rust ${JSON.stringify(rust[k])}`;
  }
  if (cmp([...js.removed].sort(cmp), [...rust.removed].sort(cmp)) !== 0)
    return `removed: js ${JSON.stringify(js.removed)}, rust ${JSON.stringify(rust.removed)}`;
  if (cmp(canonical(js.scrape), canonical(rust.scrape)) !== 0) return `scrape: js ${JSON.stringify(js.scrape)}, rust ${JSON.stringify(rust.scrape)}`;
  if (cmp(canonicalState(js.state), canonicalState(rust.state)) !== 0)
    return `state: js ${JSON.stringify(canonicalState(js.state))}, rust ${JSON.stringify(canonicalState(rust.state))}`;
  const offerKey = (r: Result) => r.offers.map((o) => [o[1], o[2], o[3]]).sort(cmp);
  if (cmp(offerKey(js), offerKey(rust)) !== 0) return `offers: js ${JSON.stringify(js.offers)}, rust ${JSON.stringify(rust.offers)}`;
}

// ---- main ----

const build = spawnSync("cargo", ["build", "--release", "-q", "-p", "wt-difftest"], { cwd: root, stdio: "inherit" });
if (build.status !== 0) process.exit(1);

const traces = Array.from({ length: TRACES }, (_, i) => generateTrace(FIRST_SEED + i, OPS));
const jsResults = traces.map(runJs);
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wt-difftest-"));
const failuresDir = path.join(import.meta.dirname, "failures");
let failures = 0;

for (const selection of SELECTIONS) {
  const input = path.join(tmp, `${selection}.json`);
  fs.writeFileSync(input, JSON.stringify({ selection, traces }));
  const run = spawnSync(path.join(root, "target/release/wt-difftest"), [input], { maxBuffer: 2 ** 31, encoding: "utf8" });
  if (run.status !== 0) {
    process.stderr.write(run.stderr);
    process.exit(1);
  }
  const rustResults = JSON.parse(run.stdout) as Result[][];
  let selectionFailures = 0;

  for (let t = 0; t < traces.length; t++) {
    let prevJs: State = { swarms: {}, peers: {} };
    let prevRust: State = { swarms: {}, peers: {} };
    for (let i = 0; i < traces[t].ops.length; i++) {
      const op = traces[t].ops[i];
      const [js, rust] = [jsResults[t][i], rustResults[t][i]];
      if (selection === SELECTIONS[0]) counts.ops++;
      const problem =
        specCheck(op, js, prevJs, "js") ?? specCheck(op, rust, prevRust, selection === SELECTIONS[0] ? "rust" : "rust-" + selection) ?? compare(js, rust);
      if (problem) {
        selectionFailures++;
        if (failures + selectionFailures <= 5) {
          fs.mkdirSync(failuresDir, { recursive: true });
          const file = path.join(failuresDir, `seed-${traces[t].seed}.json`);
          fs.writeFileSync(file, JSON.stringify(traces[t], null, 1));
          console.error(`MISMATCH [${selection}] seed ${traces[t].seed} op #${i} ${JSON.stringify(op)}\n  ${problem}\n  trace saved to ${path.relative(root, file)}`);
        }
        break; // later ops of this trace would only repeat the divergence
      }
      [prevJs, prevRust] = [js.state, rust.state];
    }
  }
  failures += selectionFailures;
  console.log(`${selection.padEnd(12)} ${selectionFailures === 0 ? "OK" : `${selectionFailures} traces FAILED`}`);
}
fs.rmSync(tmp, { recursive: true, force: true });

console.log(
  `\n${TRACES} traces × ${OPS} ops (seeds ${FIRST_SEED}..${FIRST_SEED + TRACES - 1}), per strategy:\n` +
    Object.entries(counts).map(([k, v]) => `  ${k}: ${v}`).join("\n"),
);
process.exit(failures ? 1 : 0);
