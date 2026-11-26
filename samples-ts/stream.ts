import { subscribe, CommitmentLevel, LaserstreamClient, type SubscribeRequest, type SubscribeUpdate } from "helius-laserstream";
import { Pool } from "pg";
import { decodeTransfers, commitSlot, type Transfer } from "./transfers";
import { log } from "./log";

const MINTS = [
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
];
const STREAM_ID = "laserstream-main";
const REPLAY_MARGIN = 50n; // re-read a few slots on restart; idempotent inserts make the overlap free
const REPORT_EVERY_MS = 10_000;

const config = { apiKey: process.env.HELIUS_API_KEY!, endpoint: process.env.LASERSTREAM_ENDPOINT! };
const db = new Pool({ connectionString: process.env.DATABASE_URL });

// Resume from our own checkpoint: the SDK's reconnect position lives in memory and dies with the process.
async function resumeSlot(): Promise<bigint> {
  const laserstream = new LaserstreamClient(config);
  const { rows } = await db.query("SELECT slot FROM stream_checkpoint WHERE id = $1", [STREAM_ID]);
  // First run: start at the tip; backfill covers the rest of the window.
  if (!rows[0]) {
    const tip = BigInt((await laserstream.getSlot(CommitmentLevel.FINALIZED)).slot);
    log("info", "stream_resume", { mode: "tip", from_slot: tip });
    return tip;
  }

  const { firstAvailable } = await laserstream.subscribeReplayInfo();
  if (firstAvailable == null) {
    throw new Error("Replay unavailable; keep the checkpoint and retry without advancing it");
  }
  const oldest = BigInt(firstAvailable);
  const checkpoint = BigInt(rows[0].slot);
  const from = checkpoint > REPLAY_MARGIN ? checkpoint - REPLAY_MARGIN : 0n;
  if (from >= oldest) {
    log("info", "stream_resume", { mode: "checkpoint", checkpoint, from_slot: from });
    return from;
  }

  // Outage outlived the replay window: queue the gap for backfill, resume from the oldest replayable slot.
  await db.query(
    "INSERT INTO backfill_ranges (start_slot, end_slot) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    [from.toString(), (oldest - 1n).toString()],
  );
  log("warn", "stream_resume", { mode: "gap_queued", checkpoint, from_slot: oldest, gap_start: from, gap_end: oldest - 1n });
  return oldest;
}

// Slots buffered until the stream has moved past them and their block time is known.
const pending = new Map<bigint, { blockTime?: number; transfers: Transfer[] }>();
let maxSeen = 0n;
let writes = Promise.resolve(); // one flush at a time, so slots commit in order
// Progress since the last report; the last committed slot and its block time give ingestion lag.
const progress = { slots: 0, transfers: 0, last: undefined as { slot: bigint; blockTime: number } | undefined };

function report() {
  const { last } = progress;
  log("info", "stream_progress", {
    slot: last?.slot ?? null, lag_s: last ? Math.floor(Date.now() / 1000) - last.blockTime : null,
    slots: progress.slots, transfers: progress.transfers, pending: pending.size,
  });
  progress.slots = progress.transfers = 0;
}

function onData(update: SubscribeUpdate) {
  const slot = BigInt(update.transaction?.slot ?? update.blockMeta?.slot ?? -1);
  if (slot < 0n) return; // pings, pongs
  const entry = pending.get(slot) ?? { transfers: [] };
  pending.set(slot, entry);
  if (slot > maxSeen) maxSeen = slot;

  // matchMints delivers whole transactions, so a USDC→SOL swap also carries other mints' transfers; keep ours.
  if (update.transaction) entry.transfers.push(...decodeTransfers(update.transaction).filter((t) => MINTS.includes(t.mint)));
  if (update.blockMeta) entry.blockTime = Number(update.blockMeta.blockTime?.timestamp);
  writes = writes.then(flushReady);
}

async function flushReady() {
  for (const slot of [...pending.keys()].sort((a, b) => (a < b ? -1 : 1))) {
    const entry = pending.get(slot)!;
    if (slot >= maxSeen || entry.blockTime === undefined) break; // never move the checkpoint past an unfinished slot
    await commitSlot(db, STREAM_ID, slot, entry.blockTime, entry.transfers) // transfers + checkpoint, one transaction
      .catch((e) => { throw new Error(`commit slot ${slot}: ${e.message}`); });
    pending.delete(slot);
    progress.slots++;
    progress.transfers += entry.transfers.length;
    progress.last = { slot, blockTime: entry.blockTime };
  }
}

function exit(error: unknown): never {
  log("error", "stream_exit", { error: String(error) });
  process.exit(1); // restart resumes from the checkpoint
}

async function main() {
  const fromSlot = await resumeSlot();
  const request: SubscribeRequest = {
    commitment: CommitmentLevel.FINALIZED, // only finalized slots are guaranteed to be on canonical chain
    fromSlot: Number(fromSlot),             // our checkpoint, see "Handling failure"
    transactions: {
      transfers: {
        accountInclude: MINTS,
        accountExclude: [],
        accountRequired: [],
        matchMints: true, // also match on pre/post token balance mints: plain Transfer never lists the mint
        vote: false,
        failed: false,
      },
    },
    blocksMeta: { everySlot: {} }, // every slot, with block time, including slots with no matching transfers
  };

  log("info", "stream_connect", { from_slot: fromSlot });
  setInterval(report, REPORT_EVERY_MS);
  // SDK reconnects itself; this fires on fatal errors such as OUT_OF_RANGE.
  await subscribe({ ...config, replay: true }, request, onData, exit);
}

process.on("unhandledRejection", exit); // a failed commit rejects the writes chain
main().catch(exit);
