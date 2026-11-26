import bs58 from "bs58";
import type { SubscribeUpdateTransaction } from "helius-laserstream";
import type { Pool } from "pg";

const TOKEN_PROGRAMS = new Set([
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // SPL Token
  "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // Token-2022: same Transfer/TransferChecked layout
]);
const TRANSFER = 3;
const TRANSFER_CHECKED = 12;

export type Transfer = {
  signature: string;
  ixIndex: number;
  innerIxIndex: number; // -1 for outer instructions
  mint: string;
  source: string;
  destination: string;
  sourceOwner: string | null;
  destinationOwner: string | null;
  amount: bigint;
};

type TxInfo = NonNullable<SubscribeUpdateTransaction["transaction"]>;
type Meta = NonNullable<TxInfo["meta"]>;
type Message = NonNullable<NonNullable<TxInfo["transaction"]>["message"]>;
type Instruction = { programIdIndex?: number | null; accounts?: Uint8Array | null; data?: Uint8Array | null };

// shortcut: Token-2022 TransferCheckedWithFee and confidential transfers are skipped; add them before indexing Token-2022 mints.
export function decodeTransfers(update: SubscribeUpdateTransaction): Transfer[] {
  const { signature, transaction, meta } = update.transaction!;
  if (meta!.err) return [];

  const keys = accountKeys(transaction!.message!, meta!);
  const tokenAccounts = tokenAccountInfo(keys, meta!);
  const transfers: Transfer[] = [];

  for (const { ix, ixIndex, innerIxIndex } of allInstructions(transaction!.message!, meta!)) {
    if (!TOKEN_PROGRAMS.has(keys[ix.programIdIndex!])) continue;
    const transfer = parseTransfer(ix, keys);
    if (!transfer) continue;

    const source = tokenAccounts.get(transfer.source);
    const destination = tokenAccounts.get(transfer.destination);
    // Plain Transfer doesn't name the mint; take it from the token balances of the accounts involved.
    const mint = transfer.mint ?? source?.mint ?? destination?.mint;
    if (!mint) continue;

    transfers.push({
      signature: bs58.encode(signature!),
      ixIndex,
      innerIxIndex,
      mint,
      source: transfer.source,
      destination: transfer.destination,
      sourceOwner: source?.owner ?? null,
      destinationOwner: destination?.owner ?? null,
      amount: transfer.amount,
    });
  }
  return transfers;
}

// Instructions refer to accounts by position in this list: static keys, then lookup-table addresses.
function accountKeys(message: Message, meta: Meta): string[] {
  return [...message.accountKeys!, ...(meta.loadedWritableAddresses ?? []), ...(meta.loadedReadonlyAddresses ?? [])]
    .map((key) => bs58.encode(key));
}

// Mint and owner of every token account the transaction touched, by address.
function tokenAccountInfo(keys: string[], meta: Meta) {
  const info = new Map<string, { mint: string; owner: string | null }>();
  for (const balance of [...(meta.preTokenBalances ?? []), ...(meta.postTokenBalances ?? [])]) {
    info.set(keys[balance.accountIndex!], { mint: balance.mint!, owner: balance.owner || null });
  }
  return info;
}

// Outer instructions, then inner ones (CPIs from swaps, aggregators, ...) tagged with the outer index that made them.
function* allInstructions(message: Message, meta: Meta) {
  for (const [i, ix] of message.instructions!.entries()) {
    yield { ix: ix as Instruction, ixIndex: i, innerIxIndex: -1 };
  }
  for (const group of meta.innerInstructions ?? []) {
    for (const [j, ix] of group.instructions!.entries()) {
      yield { ix: ix as Instruction, ixIndex: group.index!, innerIxIndex: j };
    }
  }
}

function parseTransfer(ix: Instruction, keys: string[]) {
  const data = Buffer.from(ix.data ?? []);
  const accounts = [...(ix.accounts ?? [])].map((i) => keys[i]);

  // Transfer: data [3, u64 amount], accounts [source, destination, authority]
  if (data[0] === TRANSFER && data.length === 9) {
    return { source: accounts[0], destination: accounts[1], mint: undefined, amount: data.readBigUInt64LE(1) };
  }
  // TransferChecked: data [12, u64 amount, u8 decimals], accounts [source, mint, destination, authority]
  if (data[0] === TRANSFER_CHECKED && data.length === 10) {
    return { source: accounts[0], destination: accounts[2], mint: accounts[1], amount: data.readBigUInt64LE(1) };
  }
  return null;
}

// Write a slot's transfers and advance the checkpoint in one transaction:
// a crash can never record progress for rows that weren't stored.
export async function commitSlot(db: Pool, streamId: string, slot: bigint, blockTime: number, transfers: Transfer[]) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    for (const t of transfers) {
      await client.query(
        `INSERT INTO transfers (signature, ix_index, inner_ix_index, slot, block_time, mint, source, destination,
                                source_owner, destination_owner, amount)
         VALUES ($1, $2, $3, $4, to_timestamp($5), $6, $7, $8, $9, $10, $11)
         ON CONFLICT DO NOTHING`, // replay re-delivers slots; the primary key makes that harmless
        [t.signature, t.ixIndex, t.innerIxIndex, slot.toString(), blockTime, t.mint, t.source, t.destination,
         t.sourceOwner, t.destinationOwner, t.amount.toString()],
      );
    }
    await client.query(
      `INSERT INTO stream_checkpoint (id, slot) VALUES ($1, $2)
       ON CONFLICT (id) DO UPDATE SET slot = GREATEST(stream_checkpoint.slot, EXCLUDED.slot), updated_at = now()`,
      [streamId, slot.toString()],
    );
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}
