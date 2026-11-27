import assert from "node:assert/strict";
import test from "node:test";
import { succeeded, toUpdate, touchesMints } from "./backfill";

const USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const key = "11111111111111111111111111111111";

const rpcTx = (accounts: number[]) => ({
  transaction: { signatures: ["1"], message: { accountKeys: [key, key, key, key], instructions: [{ programIdIndex: 3, accounts, data: "3Bxs4h24hBtQy9rw" }] } },
  meta: { err: null, innerInstructions: [], preTokenBalances: [{ accountIndex: 1, mint: USDC }], postTokenBalances: [] },
});

test("converts a well-formed RPC transaction", () => {
  const tx = rpcTx([1, 2, 0]);
  assert.ok(touchesMints(tx));
  assert.deepEqual([...(toUpdate(tx, 7).transaction?.transaction?.message?.instructions?.[0]?.accounts ?? [])], [1, 2, 0]);
});

test("rejects malformed RPC data instead of corrupting or dropping it", () => {
  assert.throws(() => toUpdate(rpcTx([300, 2, 0]), 7), /account index/); // would wrap to 44
  const noInner = rpcTx([1, 2, 0]) as { meta: Record<string, unknown> };
  delete noInner.meta.innerInstructions;
  assert.throws(() => toUpdate(noInner as never, 7), /innerInstructions/);
  assert.throws(() => touchesMints({ ...rpcTx([1]), meta: null }), /preTokenBalances/);
});

test("skips failed transactions before strict parsing", () => {
  // Seen on mainnet: MaxLoadedAccountsDataSizeExceeded comes back with innerInstructions: null.
  const failed = { ...rpcTx([1, 2, 0]), meta: { ...rpcTx([1]).meta, err: "MaxLoadedAccountsDataSizeExceeded", innerInstructions: undefined } };
  assert.equal(succeeded(failed), false);
  assert.ok(succeeded(rpcTx([1, 2, 0])));
  assert.throws(() => succeeded({ ...rpcTx([1]), meta: null }), /meta/);
});
