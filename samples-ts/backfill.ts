import { readFileSync } from "node:fs";
import { join } from "node:path";
import bs58 from "bs58";
import type { SubscribeUpdateTransaction } from "helius-laserstream";
import { Pool } from "pg";
import { decodeTransfers, insertTransfers } from "./transfers";

const MINTS = [
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
];
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`;
const WORKERS = Number(process.env.BACKFILL_WORKERS ?? 32);
const MAX_RPS = Number(process.env.BACKFILL_MAX_RPS ?? 200); // Helius Business plan RPC limit
const MAX_ATTEMPTS = 5;
const db = new Pool({ connectionString: process.env.DATABASE_URL, max: WORKERS });

const CLAIM_RANGE = readFileSync(join(__dirname, "../common/claim_range.sql"), "utf8");

// A worker that died mid-range stops touching updated_at; hand its range back.
const RECLAIM_STALE = `
  UPDATE backfill_ranges SET status = 'pending'
  WHERE status = 'running' AND updated_at < now() - interval '10 minutes'`;

// Space request starts evenly so all workers together stay under the plan's rate limit.
let nextStart = 0;
async function throttle() {
  const now = Date.now();
  const wait = Math.max(0, nextStart - now);
  nextStart = Math.max(now, nextStart) + 1000 / MAX_RPS;
  if (wait) await new Promise((r) => setTimeout(r, wait));
}

async function rpc<T>(method: string, params: unknown[]): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    await throttle();
    const res = await fetch(RPC_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    });
    if ((res.status === 429 || res.status >= 500) && attempt < 6) {
      await new Promise((r) => setTimeout(r, 2 ** attempt * 250 * (0.5 + Math.random()))); // backoff with jitter
      continue;
    }
    const body = await res.json();
    if (body.error) throw new Error(`${method}: ${body.error.message}`);
    return body.result;
  }
}

async function backfillRange(start: bigint, end: bigint) {
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
      .filter((tx) => touchesMints(tx)) // same rule as matchMints on the live stream
      .flatMap((tx) => decodeTransfers(toUpdate(tx, slot)))
      .filter((t) => MINTS.includes(t.mint));

    const client = await db.connect();
    try {
      await client.query("BEGIN");
      await insertTransfers(client, BigInt(slot), block.blockTime, transfers);
      await client.query("UPDATE backfill_ranges SET updated_at = now() WHERE start_slot = $1", [start.toString()]); // heartbeat
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }
}

async function worker() {
  for (;;) {
    const { rows } = await db.query(CLAIM_RANGE);
    if (!rows[0]) return;
    const start = BigInt(rows[0].start_slot);
    try {
      await backfillRange(start, BigInt(rows[0].end_slot));
      // Done only after every block is written. A crash before this just redoes the range: inserts are idempotent.
      await db.query("UPDATE backfill_ranges SET status = 'done', updated_at = now() WHERE start_slot = $1", [start.toString()]);
    } catch (e) {
      console.error(`range ${start} failed:`, e);
      await db.query(
        `UPDATE backfill_ranges SET status = CASE WHEN attempts >= $2 THEN 'failed' ELSE 'pending' END, updated_at = now()
         WHERE start_slot = $1`,
        [start.toString(), MAX_ATTEMPTS],
      );
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
    await Promise.all(Array.from({ length: WORKERS }, worker));
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

function touchesMints(tx: RpcTx) {
  return [...(tx.meta?.preTokenBalances ?? []), ...(tx.meta?.postTokenBalances ?? [])].some((b) => MINTS.includes(b.mint));
}

// shortcut: round-trips base58 so live and backfill share decodeTransfers; request base64 if this gets hot.
function toUpdate(tx: RpcTx, slot: number): SubscribeUpdateTransaction {
  const ix = (i: RpcIx) => ({ programIdIndex: i.programIdIndex, accounts: Uint8Array.from(i.accounts), data: bs58.decode(i.data) });
  const meta = tx.meta!;
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
        innerInstructions: (meta.innerInstructions ?? []).map((g) => ({ index: g.index, instructions: g.instructions.map(ix) })),
      },
    },
  } as unknown as SubscribeUpdateTransaction;
}

main();
