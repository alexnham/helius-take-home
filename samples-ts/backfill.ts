import { readFileSync } from "node:fs";
import { join } from "node:path";
import bs58 from "bs58";
import type { SubscribeUpdateTransaction } from "helius-laserstream";
import { Pool } from "pg";
import { decodeTransfers, insertTransfers } from "./transfers";
import { log } from "./log";

const MINTS = [
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
];
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`;
const WORKERS = Number(process.env.BACKFILL_WORKERS ?? 32);
const MAX_RPS = Number(process.env.BACKFILL_MAX_RPS ?? 200); // Helius Business plan RPC limit
const MAX_ATTEMPTS = 5;
const REPORT_EVERY_MS = 10_000;
const db = new Pool({ connectionString: process.env.DATABASE_URL, max: WORKERS });

const CLAIM_RANGE = readFileSync(join(__dirname, "../common/claim_range.sql"), "utf8");

// A worker that died mid-range stops touching updated_at; hand its range back.
const RECLAIM_STALE = `
  UPDATE backfill_ranges SET status = 'pending'
  WHERE status = 'running' AND updated_at < now() - interval '10 minutes'`;

// Totals for the periodic progress report.
const stats = { rangesDone: 0, rangesFailed: 0, rangesRunning: 0, slots: 0, blocks: 0, transfers: 0, rpcRetries: 0, bytes: 0 };

function report(slotsBefore: number, elapsedMs: number) {
  log("info", "backfill_progress", {
    ranges_done: stats.rangesDone, ranges_failed: stats.rangesFailed, ranges_running: stats.rangesRunning,
    slots_per_s: Math.round(((stats.slots - slotsBefore) * 1000) / elapsedMs),
    blocks: stats.blocks, transfers: stats.transfers, rpc_retries: stats.rpcRetries, bytes: stats.bytes,
  });
  return stats.slots;
}

// Space request starts evenly so all workers together stay under the plan's rate limit.
let nextStart = 0;
async function throttle() {
  const now = Date.now();
  const wait = Math.max(0, nextStart - now);
  nextStart = Math.max(now, nextStart) + 1000 / MAX_RPS;
  if (wait) await new Promise((r) => setTimeout(r, wait));
}

// Errors name the method and slot, never RPC_URL: it carries the API key.
async function rpc<T>(method: string, params: unknown[]): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    await throttle();
    const res = await fetch(RPC_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    }).catch((e) => { throw new Error(`${method} ${params[0]}: ${e.cause?.message ?? e.message}`); });
    if ((res.status === 429 || res.status >= 500) && attempt < 6) {
      stats.rpcRetries++;
      log("warn", "rpc_retry", { method, slot: params[0], status: res.status, attempt });
      await new Promise((r) => setTimeout(r, 2 ** attempt * 250 * (0.5 + Math.random()))); // backoff with jitter
      continue;
    }
    const text = await res.text();
    stats.bytes += text.length;
    const body = JSON.parse(text);
    if (body.error) throw new Error(`${method} ${params[0]}: ${body.error.message}`);
    return body.result;
  }
}

async function backfillRange(start: bigint, end: bigint) {
  let transfersWritten = 0;
  // getBlocks lists only produced slots, so skipped slots are never requested.
  const slots = await rpc<number[]>("getBlocks", [Number(start), Number(end), { commitment: "finalized" }]);
  for (const slot of slots) {
    const block = await rpc<RpcBlock>("getBlock", [slot, {
      commitment: "finalized",
      encoding: "json",
      transactionDetails: "full",
      maxSupportedTransactionVersion: 1, // v1 (SIMD-0385) transactions are common; 0 makes the RPC reject the block
      rewards: false,
    }]);
    const transfers = block.transactions
      .filter((tx) => succeeded(tx) && touchesMints(tx)) // same rules as failed: false and matchMints on the live stream
      .flatMap((tx) => decodeTransfers(toUpdate(tx, slot)))
      .filter((t) => MINTS.includes(t.mint));

    const client = await db.connect();
    try {
      await client.query("BEGIN");
      await insertTransfers(client, BigInt(slot), block.blockTime, transfers);
      await client.query("UPDATE backfill_ranges SET updated_at = now() WHERE start_slot = $1", [start.toString()]); // heartbeat
      await client.query("COMMIT");
      stats.blocks++;
      stats.transfers += transfers.length;
      transfersWritten += transfers.length;
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }
  return { blocks: slots.length, transfers: transfersWritten };
}

async function worker() {
  for (;;) {
    const { rows } = await db.query(CLAIM_RANGE);
    if (!rows[0]) return;
    const start = BigInt(rows[0].start_slot);
    const end = BigInt(rows[0].end_slot);
    log("info", "range_claimed", { start, end });
    const began = Date.now();
    stats.rangesRunning++;
    try {
      const { blocks, transfers } = await backfillRange(start, end);
      // Done only after every block is written. A crash before this just redoes the range: inserts are idempotent.
      await db.query("UPDATE backfill_ranges SET status = 'done', updated_at = now() WHERE start_slot = $1", [start.toString()]);
      stats.rangesDone++;
      stats.slots += Number(end - start) + 1;
      log("info", "range_done", { start, end, duration_ms: Date.now() - began, blocks, transfers });
    } catch (e) {
      stats.rangesFailed++;
      log("error", "range_failed", { start, end, error: String(e) });
      await db.query(
        `UPDATE backfill_ranges SET status = CASE WHEN attempts >= $2 THEN 'failed' ELSE 'pending' END, updated_at = now()
         WHERE start_slot = $1`,
        [start.toString(), MAX_ATTEMPTS],
      );
    } finally {
      stats.rangesRunning--;
    }
  }
}

// shortcut: takes explicit slots; production finds the 30-day start by binary-searching getBlockTime.
async function plan(from: bigint, to: bigint, size: bigint) {
  for (let start = from; start <= to; start += size) {
    const end = start + size - 1n < to ? start + size - 1n : to;
    await db.query("INSERT INTO backfill_ranges (start_slot, end_slot) VALUES ($1, $2) ON CONFLICT DO NOTHING",
      [start.toString(), end.toString()]);
  }
}

async function main() {
  const [command, from, to, size = "1000"] = process.argv.slice(2);
  if (command === "plan") await plan(BigInt(from), BigInt(to), BigInt(size));
  else if (command === "work") {
    await db.query(RECLAIM_STALE);
    const started = Date.now();
    let slots = 0, last = started;
    const timer = setInterval(() => { slots = report(slots, Date.now() - last); last = Date.now(); }, REPORT_EVERY_MS);
    await Promise.all(Array.from({ length: WORKERS }, worker));
    clearInterval(timer);
    report(0, Date.now() - started); // final totals, with the whole run's average rate
  } else throw new Error("usage: backfill.ts plan <fromSlot> <toSlot> [rangeSize] | backfill.ts work");
  await db.end();
}

// --- RPC JSON → the LaserStream shape, so both paths share one decoder ---

type RpcIx = { programIdIndex: number; accounts: number[]; data: string };
type RpcTx = {
  transaction: { signatures: string[]; message: { accountKeys: string[]; instructions: RpcIx[] } };
  meta: {
    err: unknown;
    loadedAddresses?: { writable: string[]; readonly: string[] };
    preTokenBalances?: { accountIndex: number; mint: string; owner?: string }[];
    postTokenBalances?: { accountIndex: number; mint: string; owner?: string }[];
    innerInstructions?: { index: number; instructions: RpcIx[] }[];
  } | null;
};
type RpcBlock = { blockTime: number; transactions: RpcTx[] };

// Malformed RPC data fails the range (retried, then marked failed), never a silent skip.
function bad(tx: RpcTx, what: string): never {
  throw new Error(`${tx.transaction.signatures[0]}: bad ${what}`);
}

// Failed transactions move no tokens, and RPC omits fields for some (null innerInstructions): skip them unparsed.
export function succeeded(tx: RpcTx) {
  return (tx.meta ?? bad(tx, "meta")).err == null;
}

export function touchesMints(tx: RpcTx) {
  const pre = tx.meta?.preTokenBalances ?? bad(tx, "preTokenBalances");
  const post = tx.meta?.postTokenBalances ?? bad(tx, "postTokenBalances");
  return [...pre, ...post].some((b) => MINTS.includes(b.mint));
}

// shortcut: round-trips base58 so live and backfill share decodeTransfers; request base64 if this gets hot.
export function toUpdate(tx: RpcTx, slot: number): SubscribeUpdateTransaction {
  const ix = (i: RpcIx) => {
    // Uint8Array.from would wrap 300 to 44 and point the transfer at the wrong account.
    if (!i.accounts.every((a) => Number.isInteger(a) && a >= 0 && a <= 255)) bad(tx, "account index");
    return { programIdIndex: i.programIdIndex, accounts: Uint8Array.from(i.accounts), data: bs58.decode(i.data) };
  };
  const meta = tx.meta ?? bad(tx, "meta");
  // Missing loadedAddresses is normal for legacy transactions; missing innerInstructions would hide inner transfers.
  const inner = meta.innerInstructions ?? bad(tx, "innerInstructions");
  return {
    slot: String(slot),
    transaction: {
      signature: bs58.decode(tx.transaction.signatures[0]),
      transaction: { message: { accountKeys: tx.transaction.message.accountKeys.map((k) => bs58.decode(k)), instructions: tx.transaction.message.instructions.map(ix) } },
      meta: {
        err: meta.err ?? undefined,
        loadedWritableAddresses: (meta.loadedAddresses?.writable ?? []).map((k) => bs58.decode(k)),
        loadedReadonlyAddresses: (meta.loadedAddresses?.readonly ?? []).map((k) => bs58.decode(k)),
        preTokenBalances: meta.preTokenBalances,
        postTokenBalances: meta.postTokenBalances,
        innerInstructions: inner.map((g) => ({ index: g.index, instructions: g.instructions.map(ix) })),
      },
    },
  } as unknown as SubscribeUpdateTransaction;
}

if (require.main === module) main(); // importable by tests without starting workers
