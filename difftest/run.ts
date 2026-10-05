// Wire-level differential test: identical random traces of raw JSON frames go through
//   JS:   JSON.parse → FastTracker.processMessage → JSON.stringify   (as ../wt-tracker's
//         uws-tracker; a parse error or TrackerError closes the connection → disconnect)
//   Rust: wt_proto::handle (+ Shard) → Encoder; an error closes → Shard::disconnect
// for every offer-selection strategy × parser backend. Deliberate differences of docs/SPEC.md
// §8 that change behaviour are applied on the JS side before FastTracker sees the frame, so both
// sides stay comparable: an answer is delivered only within its swarm from a peer of the
// sending connection (else dropped), and one without a string info_hash is an error (§5.3).
// After every op it checks that
//   1. both sides sent the same bytes and reached the same state, and
//   2. each side is correct per docs/SPEC.md on its own (so identical bugs cannot pass).
//
// Offer receivers are random on both sides and swarm order may legitimately differ, so offer
// messages are compared as a multiset of texts and each receiver is checked to be a valid
// other member. Frames marked non-canonical (extra whitespace, escapes JSON.stringify would not
// produce, duplicate keys) and scrape-all replies are compared after JSON.parse.
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
const BACKENDS = ["serde_json", "sonic"];
const MAX_OFFERS = 20;
const INTERVAL = 20;

// ---- deterministic PRNG ----

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

// ---- frames ----

const ch = String.fromCharCode;
const PEER_IDS = ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", ch(0xff) + "p" + ch(1), "пір"];
const INFO_HASHES = ["h0", "h1", "h2", ch(0xe9) + "h" + ch(0x1f)];
/// Pieces of SDP text: CRLF, quotes, backslash, non-ASCII, line separator, emoji, control char.
const SDP_PIECES = ["v=0\r\n", "a=candidate:1 1 udp 2122260223 10.0.0.1 5000 typ host\r\n", 'a=x "q"', "\\", ch(0xe9), ch(0x2028), ch(0xd83d, 0xde00), ch(1), "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"];

/** Semantic description of a frame, used by the spec checks. */
type Sem =
  | { kind: "announce"; info_hash: string; peer_id: string; numwant: number | null; offers: Item[] | null }
  | { kind: "answer"; info_hash?: string; peer_id: string; to_peer_id: string; frame: string }
  | { kind: "stop" }
  | { kind: "scrape"; info_hash: unknown }
  | { kind: "error" };
type Item = { offer_id?: unknown; sdp?: unknown };
type Op = { op: string; conn?: number; frame?: string; secs?: number; sem?: Sem; noncanonical?: boolean };

const json = (v: unknown) => JSON.stringify(v);
const escapeAll = (s: string) => '"' + [...s].map((c) => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0")).join("") + '"';

function generateTrace(seed: number, opsCount: number): { seed: number; ops: Op[] } {
  const rnd = mulberry32(seed * 7919 + 17);
  const int = (n: number) => Math.floor(rnd() * n);
  const pick = <T>(xs: readonly T[]): T => xs[int(xs.length)];
  const chance = (p: number) => rnd() < p;
  const weighted = <T>(choices: [number, T][]): T => {
    let r = rnd() * choices.reduce((s, [w]) => s + w, 0);
    for (const [w, v] of choices) if ((r -= w) < 0) return v;
    return choices[choices.length - 1][1];
  };
  const shuffle = <T>(xs: T[]) => {
    for (let i = xs.length - 1; i > 0; i--) {
      const j = int(i + 1);
      [xs[i], xs[j]] = [xs[j], xs[i]];
    }
    return xs;
  };
  const sdp = () => Array.from({ length: 1 + int(4) }, () => pick(SDP_PIECES)).join("");

  /** Joins members into an object text; optionally non-canonical whitespace. */
  const object = (members: [string, string][], spaced: boolean) =>
    spaced
      ? "{ " + members.map(([k, v]) => `${json(k)} :\t${v}`).join(" ,\n ") + " }"
      : "{" + members.map(([k, v]) => `${json(k)}:${v}`).join(",") + "}";

  const conns = 1 + int(6);
  const peers = PEER_IDS.slice(0, 1 + int(PEER_IDS.length));
  const hashes = INFO_HASHES.slice(0, 1 + int(INFO_HASHES.length));
  const home = new Map(peers.map((p, i) => [p, i % conns]));
  const ops: Op[] = [];

  const push = (conn: number, members: [string, string][], sem: Sem, opts: { forwarded?: boolean } = {}) => {
    // ~6% non-canonical frames. Text variants of fields that are never echoed back are free
    // (do not change output bytes) unless the whole frame is forwarded (answers).
    let noncanonical = false;
    let spaced = false;
    if (chance(0.06)) {
      noncanonical = true;
      const variant = int(3);
      if (variant === 0) spaced = true;
      if (variant === 1) {
        // Duplicate key, last wins. Not to_peer_id: a duplicate there is a documented deviation.
        const dup = members.filter(([k]) => k !== "to_peer_id").at(-1)!;
        members.unshift([dup[0], "1"]);
      }
      if (variant === 2) members = members.map(([k, v]) => (k === "action" || k === "event" ? [k, escapeAll(JSON.parse(v))] : [k, v]));
    }
    const frame = object(members, spaced);
    if (sem.kind === "answer") sem.frame = frame;
    ops.push({ op: "frame", conn, frame, sem, noncanonical: noncanonical && (opts.forwarded || members.some(([k]) => k === "offers")) });
  };

  for (let i = 0; i < opsCount; i++) {
    const peer = pick(peers);
    const kind = weighted<string>([
      [50, "announce"], [10, "answer"], [8, "stop"], [5, "scrape"], [5, "error"],
      [4, "disconnect"], [10, "advance"], [6, "expire"],
    ]);
    if (kind === "announce") {
      if (chance(0.08)) home.set(peer, int(conns)); // the peer_id moves to another connection
      const info_hash = pick(hashes);
      const members: [string, string][] = [["action", '"announce"'], ["info_hash", json(info_hash)], ["peer_id", json(peer)]];
      const event = weighted<string | null>([[30, "started"], [50, null], [20, "completed"]]);
      if (event) members.push(["event", json(event)]);
      const left = weighted<string | null>([[70, null], [8, "0"], [4, "-0"], [4, "0.0"], [6, '"0"'], [8, "7"]]);
      if (left) members.push(["left", left]);
      const [numwantText, numwant] = weighted<[string | null, number | null]>([
        [8, [null, null]], [3, ["null", null]], [3, ["2.5", null]], [3, ['"10"', null]], [4, ["0", 0]],
        [8, ["1", 1]], [12, ["3", 3]], [30, ["10", 10]], [12, ["50", 50]], [3, ["-3", 0]],
        [3, ["1e1", 10]], [3, ["10.0", 10]], [2, ["1e300", 1e300]],
      ]);
      if (numwantText) members.push(["numwant", numwantText]);
      let offers: Item[] | null = null;
      if (chance(0.9)) {
        offers = Array.from({ length: int(13) }, (_, k) => {
          const item: Item = {};
          const id = weighted<unknown>([[60, "o" + k], [10, k], [10, null], [20, undefined]]);
          if (id !== undefined) item.offer_id = id;
          if (chance(0.9)) item.sdp = sdp();
          return item;
        });
        const itemText = (item: Item) => {
          const offer: [string, string][] = [["type", '"offer"']];
          if ("sdp" in item) offer.push(["sdp", json(item.sdp)]);
          if (chance(0.1)) offer.push(["extra", "[1,{}]"]);
          const offerText = "sdp" in item || chance(0.7) ? object(offer, false) : "[]";
          const m: [string, string][] = [["offer", offerText]];
          if ("offer_id" in item) m.push(["offer_id", json(item.offer_id)]);
          if (chance(0.1)) m.push(["extra", '"x"']);
          return object(shuffle(m), false);
        };
        members.push(["offers", "[" + offers.map(itemText).join(",") + "]"]);
      }
      if (chance(0.3)) members.push(["uploaded", "0"], ["downloaded", "0"], ["x", '{"y":[1,"z"]}']);
      push(home.get(peer)!, shuffle(members), { kind: "announce", info_hash, peer_id: peer, numwant, offers });
    } else if (kind === "answer") {
      const to = chance(0.9) ? pick(peers) : "nobody";
      const members: [string, string][] = [
        ["action", '"announce"'], ["peer_id", json(peer)], ["to_peer_id", json(to)],
        ["answer", weighted<string>([[80, json({ type: "answer", sdp: sdp() })], [10, "null"], [10, "5"]])],
      ];
      const info_hash = chance(0.8) ? pick(hashes) : undefined;
      if (info_hash !== undefined) members.push(["info_hash", json(info_hash)]);
      if (chance(0.7)) members.push(["offer_id", json("o" + int(12))]);
      if (chance(0.2)) members.push(["extra", '[1,{"a":"b"}]']);
      // Mostly from the sender's home connection; sometimes from another one (dropped, §5.3).
      const from = chance(0.9) ? home.get(peer)! : int(conns);
      push(from, shuffle(members), { kind: "answer", info_hash, peer_id: peer, to_peer_id: to, frame: "" }, { forwarded: true });
    } else if (kind === "stop") {
      const info_hash = weighted<string>([[85, json(pick(hashes))], [10, '"nope"'], [5, "5"]]);
      push(home.get(peer)!, shuffle([["action", '"announce"'], ["event", '"stopped"'], ["info_hash", info_hash], ["peer_id", json(peer)]]), { kind: "stop" });
    } else if (kind === "scrape") {
      const info_hash = weighted<unknown>([[30, undefined], [35, chance(0.8) ? pick(hashes) : "nope"], [25, [pick(hashes), 5, pick(hashes), "nope", null]], [10, 5]]);
      const members: [string, string][] = [["action", '"scrape"']];
      if (info_hash !== undefined) members.push(["info_hash", json(info_hash)]);
      push(int(conns), members, { kind: "scrape", info_hash });
    } else if (kind === "error") {
      // Rejected by both sides before any state change ("zz" is a fresh peer_id).
      const frame = pick([
        '{"action":"announce","info_hash":"h0","peer_id":"zz"',
        "[]", '"x"', "5", "{}", '{"action":"nope"}', '{"action":5}',
        '{"action":"announce","event":"paused","info_hash":"h0","peer_id":"zz"}',
        '{"action":"announce","event":null,"info_hash":"h0","peer_id":"zz"}',
        '{"action":"announce","info_hash":5,"peer_id":"zz"}',
        '{"action":"announce","info_hash":"h0","peer_id":5}',
        '{"action":"announce","info_hash":"h0"}',
        '{"action":"announce","peer_id":"zz","to_peer_id":5,"answer":{}}',
        '{"action":"announce","peer_id":5,"to_peer_id":"p0","answer":{}}',
        '{"action":"announce","event":"stopped","info_hash":"h0","peer_id":5}',
      ]);
      ops.push({ op: "frame", conn: int(conns), frame, sem: { kind: "error" } });
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
type Result = { error: boolean; messages: [number, string][]; removed: [string, number][]; state: State; invariants?: string | null };

const cmp = (a: unknown, b: unknown) => {
  const [x, y] = [JSON.stringify(a), JSON.stringify(b)];
  return x < y ? -1 : x > y ? 1 : 0;
};
const sorted = <T>(xs: T[]) => [...xs].sort(cmp);
function canonical(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonical((v as Record<string, unknown>)[k])]));
  }
  return v;
}
const canonicalText = (text: string) => JSON.stringify(canonical(JSON.parse(text)));
function canonicalState(s: State): unknown {
  const swarms: Record<string, unknown> = {};
  for (const [h, sw] of Object.entries(s.swarms)) swarms[h] = { peers: [...sw.peers].sort(), complete: sw.complete };
  return canonical({ swarms, peers: s.peers });
}

type Kind = "reply" | "offer" | "answer" | "scrape";
function kindOf(text: string): Kind {
  const m = JSON.parse(text);
  if (m.action === "scrape") return "scrape";
  if (m.offer !== undefined) return "offer";
  if (m.interval !== undefined) return "reply";
  return "answer";
}
const byKind = (r: Result, kind: Kind) => r.messages.filter(([, t]) => kindOf(t) === kind);

// ---- JS side (emulates uws-tracker) ----

let lastSweep: (() => void) | undefined;
globalThis.setInterval = ((fn: () => void) => {
  lastSweep = fn;
  return 0;
}) as unknown as typeof setInterval;
let fakeNowMs = 0;
performance.now = () => fakeNowMs;

const { FastTracker } = (await import(path.join(trackerDir, "src/fast-tracker.ts"))) as typeof import("../../wt-tracker/src/fast-tracker.ts");
const { TrackerError } = (await import(path.join(trackerDir, "src/tracker.ts"))) as typeof import("../../wt-tracker/src/tracker.ts");

type Conn = { id: number };

function runJs(trace: { seed: number; ops: Op[] }): Result[] {
  Math.random = mulberry32(trace.seed);
  fakeNowMs = 0;
  let out: Result;
  const tracker = new FastTracker<Conn>({}, (msg, conn) => out.messages.push([conn.id, JSON.stringify(msg)]));
  const sweep = lastSweep!;
  tracker.onRemovePeer = (peerId, conn) => out.removed.push([peerId, conn.id]);
  const conns = new Map<number, Conn>();
  const conn = (id: number) => {
    let c = conns.get(id);
    if (!c) conns.set(id, (c = { id }));
    return c;
  };

  // Spec §5.3 (a deliberate difference from FastTracker): an answer needs a string info_hash
  // (else an error, closing the connection), and is delivered only if the sender is a peer of
  // the sending connection in that swarm and the target is in it too (else dropped).
  const answerAllowed = (m: Record<string, unknown>, c: Conn): boolean => {
    if (typeof m.info_hash !== "string") throw new TrackerError("answer: info_hash is not a string");
    const sw = tracker.swarms.get(m.info_hash);
    const from = tracker.peers.get(m.peer_id as string);
    const member = (id: unknown) => !!sw?.peers.some((p: { peerId: string }) => p.peerId === id);
    return from?.connection === c && member(m.peer_id) && member(m.to_peer_id);
  };

  const results: Result[] = [];
  for (const op of trace.ops) {
    out = { error: false, messages: [], removed: [], state: { swarms: {}, peers: {} } };
    if (op.op === "frame") {
      const c = conn(op.conn!);
      try {
        const message = JSON.parse(op.frame!);
        if (op.sem?.kind !== "answer" || answerAllowed(message, c)) tracker.processMessage(message, c);
      } catch (e) {
        if (!(e instanceof SyntaxError || e instanceof TrackerError)) throw e;
        out.error = true;
        tracker.disconnect(c); // ws.close() → onClose → disconnect
      }
    } else if (op.op === "disconnect") {
      tracker.disconnect(conn(op.conn!));
    } else if (op.op === "advance") {
      fakeNowMs += op.secs! * 1000;
    } else if (op.op === "expire") {
      sweep();
    }
    for (const [h, sw] of tracker.swarms) out.state.swarms[h] = { peers: sw.peers.map((p) => p.peerId), complete: sw.completedCount };
    for (const [id, p] of tracker.peers) out.state.peers[id] = p.connection.id;
    results.push(out);
  }
  tracker.dispose();
  return results;
}

// ---- spec checks (each side independently) ----

const counts = { ops: 0, frames: 0, noncanonical: 0, offers: 0, fullFanOut: 0, partialFanOut: 0, answers: 0, droppedAnswers: 0, connChanges: 0, errors: 0, removed: 0, expired: 0 };

function specCheck(op: Op, r: Result, prev: State, side: string): string | undefined {
  const s = r.state;
  const fail = (m: string) => `${side}: ${m}`;
  if (r.invariants) return fail(`invariants: ${r.invariants}`);
  const count = side === "rust";
  if (count) {
    counts.errors += Number(r.error);
    counts.removed += r.removed.length;
    if (op.op === "expire") counts.expired += r.removed.length;
  }

  // Removed peers = peers gone from the state, or moved to another connection.
  const expectedRemoved = Object.entries(prev.peers).filter(([id, c]) => s.peers[id] !== c);
  if (cmp(sorted(r.removed), sorted(expectedRemoved)) !== 0) return fail(`removed ${json(r.removed)}, expected ${json(expectedRemoved)}`);

  const sem = op.sem;
  const only = (...kinds: Kind[]) => {
    const extra = r.messages.find(([, t]) => !kinds.includes(kindOf(t)));
    return extra ? fail(`unexpected message ${extra[1]}`) : undefined;
  };

  if (op.op !== "frame") return only();
  const expectError = sem!.kind === "error" || (sem!.kind === "answer" && sem!.info_hash === undefined);
  if (expectError !== r.error) return fail(`error ${r.error} for ${sem!.kind}`);

  switch (sem!.kind) {
    case "error":
      return only();
    case "stop":
      return only();
    case "announce": {
      const { info_hash: ih, peer_id: pid } = sem;
      const sw = s.swarms[ih];
      if (!sw?.peers.includes(pid) || s.peers[pid] !== op.conn) return fail("announcer not in swarm on its connection");
      const replies = byKind(r, "reply");
      const reply = [op.conn, json({ action: "announce", interval: INTERVAL, info_hash: ih, complete: sw.complete, incomplete: sw.peers.length - sw.complete })];
      if (cmp(replies, [reply]) !== 0) return fail(`reply ${json(replies)}, expected ${json([reply])}`);
      const others = sw.peers.length - 1;
      const n = sem.offers === null || sem.numwant === null || others < 1 ? 0 : Math.min(others, sem.offers.length, MAX_OFFERS, sem.numwant);
      const offers = byKind(r, "offer");
      if (offers.length !== n) return fail(`${offers.length} offers, expected ${n}`);
      // Payloads: exactly the first n offer items, rebuilt as the JS tracker does.
      const expected = (sem.offers ?? []).slice(0, n).map((it) => canonicalText(json({ action: "announce", info_hash: ih, offer_id: it.offer_id, peer_id: pid, offer: { type: "offer", sdp: it.sdp } })));
      if (cmp(sorted(offers.map(([, t]) => canonicalText(t))), sorted(expected)) !== 0) return fail("offer payloads");
      // Receivers: a sub-multiset of the other members' connections (all of them if n == others).
      const pool = sw.peers.filter((p) => p !== pid).map((p) => s.peers[p]);
      for (const [to] of offers) {
        const i = pool.indexOf(to);
        if (i < 0) return fail(`offer to ${to}, which is not another member's connection`);
        pool.splice(i, 1);
      }
      if (n === others && pool.length) return fail("full fan-out missed members");
      if (count) {
        counts.offers += n;
        if (n > 0) n === others ? counts.fullFanOut++ : counts.partialFanOut++;
        if (prev.peers[pid] !== undefined && prev.peers[pid] !== op.conn) counts.connChanges++;
      }
      return only("reply", "offer");
    }
    case "answer": {
      if (r.error) return only();
      // Spec §5.3: delivered only from a peer of the sending connection to a peer, both in the
      // answer's swarm; otherwise dropped.
      const sw = sem.info_hash === undefined ? undefined : prev.swarms[sem.info_hash];
      const allowed = prev.peers[sem.peer_id] === op.conn && !!sw?.peers.includes(sem.peer_id) && !!sw?.peers.includes(sem.to_peer_id);
      const answers = byKind(r, "answer");
      if (!allowed) {
        if (count) counts.droppedAnswers++;
        return answers.length ? fail(`answer delivered against §5.3: ${json(answers)}`) : only();
      }
      const to = prev.peers[sem.to_peer_id];
      const body = JSON.parse(sem.frame);
      body.to_peer_id = undefined;
      if (answers.length !== 1 || answers[0][0] !== to || canonicalText(answers[0][1]) !== canonicalText(json(body))) return fail(`answers ${json(answers)}`);
      if (count) counts.answers++;
      return only("answer");
    }
    case "scrape": {
      const scrapes = byKind(r, "scrape");
      const entry = (h: string) => {
        const sw = s.swarms[h];
        const c = sw?.complete ?? 0;
        return { complete: c, incomplete: (sw?.peers.length ?? 0) - c, downloaded: c };
      };
      const ih = sem.info_hash;
      const hashes = ih === undefined ? Object.keys(s.swarms) : typeof ih === "string" ? [ih] : Array.isArray(ih) ? (ih.filter((h) => typeof h === "string") as string[]) : [];
      const expected = canonicalText(json({ action: "scrape", files: Object.fromEntries(hashes.map((h) => [h, entry(h)])) }));
      if (scrapes.length !== 1 || scrapes[0][0] !== op.conn || canonicalText(scrapes[0][1]) !== expected) return fail(`scrape ${json(scrapes)}, expected ${expected}`);
      return only("scrape");
    }
  }
}

/** Same bytes (or same JSON for non-canonical frames / scrape-all) on both sides. */
function compare(op: Op, js: Result, rust: Result): string | undefined {
  if (js.error !== rust.error) return `error: js ${js.error}, rust ${rust.error}`;
  if (cmp(sorted(js.removed), sorted(rust.removed)) !== 0) return `removed: js ${json(js.removed)}, rust ${json(rust.removed)}`;
  if (cmp(canonicalState(js.state), canonicalState(rust.state)) !== 0) return `state: js ${json(canonicalState(js.state))}, rust ${json(canonicalState(rust.state))}`;
  const exact = !op.noncanonical && !(op.sem?.kind === "scrape" && op.sem.info_hash === undefined);
  const norm = (t: string) => (exact ? t : canonicalText(t));
  const key = (r: Result) =>
    sorted(r.messages.map(([to, t]) => (kindOf(t) === "offer" ? [-1, norm(t)] : [to, norm(t)])));
  const [a, b] = [key(js), key(rust)];
  if (cmp(a, b) !== 0) {
    const onlyJs = a.filter((m) => !b.some((x) => cmp(x, m) === 0));
    const onlyRust = b.filter((m) => !a.some((x) => cmp(x, m) === 0));
    return `messages differ${exact ? " (bytes)" : ""}:\n    js only:   ${json(onlyJs)}\n    rust only: ${json(onlyRust)}`;
  }
}

// ---- main ----

const build = spawnSync("cargo", ["build", "--release", "-q", "-p", "wt-difftest"], { cwd: root, stdio: "inherit" });
if (build.status !== 0) process.exit(1);

const traces = Array.from({ length: TRACES }, (_, i) => generateTrace(FIRST_SEED + i, OPS));
const jsResults = traces.map(runJs);
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wt-difftest-"));
const failuresDir = path.join(import.meta.dirname, "failures");
const input = path.join(tmp, "traces.json");
fs.writeFileSync(input, json({ traces: traces.map((t) => ({ seed: t.seed, ops: t.ops.map(({ sem: _, noncanonical: __, ...o }) => o) })) }));
let failures = 0;

for (const backend of BACKENDS) {
  for (const selection of SELECTIONS) {
    const config = path.join(tmp, `${backend}-${selection}.json`);
    fs.writeFileSync(config, fs.readFileSync(input, "utf8").replace(/^\{/, `{"selection":"${selection}","backend":"${backend}",`));
    const run = spawnSync(path.join(root, "target/release/wt-difftest"), [config], { maxBuffer: 2 ** 31, encoding: "utf8" });
    if (run.status !== 0) {
      process.stderr.write(run.stderr);
      process.exit(1);
    }
    const rustResults = JSON.parse(run.stdout) as Result[][];
    const first = backend === BACKENDS[0] && selection === SELECTIONS[0];
    let runFailures = 0;

    for (let t = 0; t < traces.length; t++) {
      let [prevJs, prevRust]: State[] = [{ swarms: {}, peers: {} }, { swarms: {}, peers: {} }];
      for (let i = 0; i < traces[t].ops.length; i++) {
        const op = traces[t].ops[i];
        const [js, rust] = [jsResults[t][i], rustResults[t][i]];
        if (first) {
          counts.ops++;
          if (op.op === "frame") counts.frames++;
          if (op.noncanonical) counts.noncanonical++;
        }
        const problem = specCheck(op, js, prevJs, "js") ?? specCheck(op, rust, prevRust, first ? "rust" : `rust-${backend}-${selection}`) ?? compare(op, js, rust);
        if (problem) {
          runFailures++;
          if (failures + runFailures <= 5) {
            fs.mkdirSync(failuresDir, { recursive: true });
            const file = path.join(failuresDir, `seed-${traces[t].seed}.json`);
            fs.writeFileSync(file, JSON.stringify(traces[t], null, 1));
            console.error(`MISMATCH [${backend}/${selection}] seed ${traces[t].seed} op #${i} ${json(op)}\n  ${problem}\n  trace saved to ${path.relative(root, file)}`);
          }
          break; // later ops of this trace would only repeat the divergence
        }
        [prevJs, prevRust] = [js.state, rust.state];
      }
    }
    failures += runFailures;
    console.log(`${backend.padEnd(11)} ${selection.padEnd(12)} ${runFailures === 0 ? "OK" : `${runFailures} traces FAILED`}`);
  }
}
fs.rmSync(tmp, { recursive: true, force: true });

console.log(
  `\n${TRACES} traces × ${OPS} ops (seeds ${FIRST_SEED}..${FIRST_SEED + TRACES - 1}), per backend × strategy:\n` +
    Object.entries(counts).map(([k, v]) => `  ${k}: ${v}`).join("\n"),
);
process.exit(failures ? 1 : 0);
