// Scenario shapes shared by bench.ts and scaling-worker.ts. Mirrors crates/wt-bench/src/lib.rs.

import path from "node:path";

// The JS tracker under test; override with WT_TRACKER_DIR.
const trackerDir = path.resolve(
  process.env.WT_TRACKER_DIR ??
    path.join(import.meta.dirname, "../../../wt-tracker"),
);

// FastTracker starts a real setInterval sweep in its constructor. Capture it instead so a
// sweep never fires during a measurement, and the expire scenario can trigger it on demand.
let lastSweep: (() => void) | undefined;
globalThis.setInterval = ((fn: () => void) => {
  lastSweep = fn;
  return 0;
}) as unknown as typeof setInterval;

export function takeSweep(): () => void {
  const sweep = lastSweep;
  if (!sweep) throw new Error("no sweep captured");
  lastSweep = undefined;
  return sweep;
}

const { FastTracker } = (await import(
  path.join(trackerDir, "src/fast-tracker.ts")
)) as typeof import("../../../wt-tracker/src/fast-tracker.ts");

export const OFFERS_PER_ANNOUNCE = 10;
export const NUMWANT = 10;

export const ONE_SWARM_PEERS = 100_000;
export const MANY_SWARMS_PEERS = 1_000_000;
export const MANY_SWARMS_SWARMS = 100_000;
export const MP_CONNS = 100_000;
export const MP_PEERS_PER_CONN = 3;
export const MP_SWARMS_PER_PEER = 2;
export const MP_SWARMS = 10_000;
export const MP_PEERS = MP_CONNS * MP_PEERS_PER_CONN;
export const MP_MEMBERSHIPS = MP_PEERS * MP_SWARMS_PER_PEER;
export const ANSWERS = 1_000_000;

export type Conn = Record<string, unknown>;

export function makeIds(prefix: string, count: number): string[] {
  const ids = new Array<string>(count);
  for (let i = 0; i < count; i++) {
    ids[i] = prefix + String(i).padStart(19, "0");
  }
  return ids;
}

export function makeConns(count: number): Conn[] {
  return Array.from({ length: count }, () => ({}));
}

export const counter = { replies: 0, offers: 0, answers: 0, removed: 0 };

export function resetCounter() {
  counter.replies = 0;
  counter.offers = 0;
  counter.answers = 0;
  counter.removed = 0;
}

function sendMessage(json: Record<string, unknown>) {
  if (json.offer !== undefined) counter.offers++;
  else if (json.answer !== undefined) counter.answers++;
  else if (json.interval !== undefined) counter.replies++;
}

export function newTracker() {
  const tracker = new FastTracker<Conn>({}, sendMessage);
  tracker.onRemovePeer = () => {
    counter.removed++;
  };
  return tracker;
}

export type Tracker = ReturnType<typeof newTracker>;

const offers = Array.from({ length: OFFERS_PER_ANNOUNCE }, (_, i) => ({
  offer: { type: "offer", sdp: "x" },
  offer_id: "o" + i,
}));

const announceMessage: Record<string, unknown> = {
  action: "announce",
  event: "started",
  info_hash: "",
  peer_id: "",
  offers,
  numwant: NUMWANT,
};

export function announce(
  tracker: Tracker,
  conn: Conn,
  infoHash: string,
  peerId: string,
  event: "started" | undefined,
  withOffers = true,
) {
  announceMessage.event = event;
  announceMessage.info_hash = infoHash;
  announceMessage.peer_id = peerId;
  announceMessage.offers = withOffers ? offers : undefined;
  tracker.processMessage(announceMessage, conn);
}

export function mpSwarm(p: number, j: number) {
  return (p * 7 + j * 5003) % MP_SWARMS;
}

/** Flat [conn, peer, swarm, ...] triples of the multi-peer scenario, optionally one shard's. */
export function mpMemberships(shard = 0, shards = 1): Int32Array {
  const list: number[] = [];
  for (let c = 0; c < MP_CONNS; c++) {
    for (let k = 0; k < MP_PEERS_PER_CONN; k++) {
      const p = c * MP_PEERS_PER_CONN + k;
      for (let j = 0; j < MP_SWARMS_PER_PEER; j++) {
        const s = mpSwarm(p, j);
        if (s % shards === shard) list.push(c, p, s);
      }
    }
  }
  return Int32Array.from(list);
}

export type Ids = { peers: string[]; swarms: string[]; conns: Conn[] };

export function runAnnounceList(
  tracker: Tracker,
  ids: Ids,
  list: Int32Array,
  event: "started" | undefined,
  withOffers = true,
) {
  for (let i = 0; i < list.length; i += 3) {
    announce(
      tracker,
      ids.conns[list[i]],
      ids.swarms[list[i + 2]],
      ids.peers[list[i + 1]],
      event,
      withOffers,
    );
  }
}
