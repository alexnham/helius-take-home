import assert from "node:assert/strict";
import test from "node:test";
import bs58 from "bs58";
import type { SubscribeUpdateTransaction } from "helius-laserstream";
import { decodeTransfers } from "./transfers";

const key = (n: number) => new Uint8Array(32).fill(n);
const u64 = (n: bigint) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(n); return b; };
const TOKEN = bs58.decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

// keys: 0 signer, 1 src, 2 dst, 3 token program, 4 mint; 5 comes from a lookup table
const update = {
  slot: "100",
  transaction: {
    signature: key(9),
    transaction: {
      message: {
        accountKeys: [key(0), key(1), key(2), TOKEN, bs58.decode(USDC)],
        instructions: [
          { programIdIndex: 3, accounts: Uint8Array.from([1, 2, 0]), data: Buffer.concat([Buffer.from([3]), u64(5_000_000n)]) },
          { programIdIndex: 0, accounts: Uint8Array.from([]), data: Uint8Array.from([1]) }, // not a token program
        ],
      },
    },
    meta: {
      loadedWritableAddresses: [key(5)],
      loadedReadonlyAddresses: [],
      preTokenBalances: [
        { accountIndex: 1, mint: USDC, owner: "ownerA" },
        { accountIndex: 5, mint: USDC, owner: "ownerB" },
      ],
      postTokenBalances: [],
      innerInstructions: [{
        index: 1,
        instructions: [
          // TransferChecked to the lookup-table account, amount > 2^53
          { programIdIndex: 3, accounts: Uint8Array.from([2, 4, 5, 0]), data: Buffer.concat([Buffer.from([12]), u64(2n ** 60n), Buffer.from([6])]) },
        ],
      }],
    },
  },
} as unknown as SubscribeUpdateTransaction;

test("decodes outer Transfer and inner TransferChecked, resolving lookup-table accounts", () => {
  const [outer, inner] = decodeTransfers(update);
  assert.deepEqual(
    { ...outer, signature: undefined },
    { signature: undefined, ixIndex: 0, innerIxIndex: -1, mint: USDC, source: bs58.encode(key(1)), destination: bs58.encode(key(2)),
      sourceOwner: "ownerA", destinationOwner: null, amount: 5_000_000n },
  );
  assert.equal(inner.ixIndex, 1);
  assert.equal(inner.innerIxIndex, 0);
  assert.equal(inner.destination, bs58.encode(key(5)));
  assert.equal(inner.destinationOwner, "ownerB");
  assert.equal(inner.amount, 2n ** 60n);
  assert.equal(decodeTransfers(update).length, 2);
});

test("skips failed transactions", () => {
  const failed = structuredClone(update);
  failed.transaction!.meta!.err = { err: Uint8Array.from([1]) };
  assert.deepEqual(decodeTransfers(failed), []);
});
