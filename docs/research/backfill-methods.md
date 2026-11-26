# Backfill methods for 30 days of USDC/USDT token transfers

Date: 2026-11-26

**Question.** What is the best way, as of November 2026, to backfill about 30 days of chain-wide SPL Token and Token-2022 transfer data for USDC (`EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`) and USDT (`Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB`) on Solana mainnet? The backfill must cover the part of the window that LaserStream gRPC's roughly 48-hour replay cannot reach, plus any gap older than that replay window.

**The requirement that decides it.** A classic `Transfer` instruction does not list the mint. The only way to tie it to USDC or USDT is through `preTokenBalances`/`postTokenBalances` in the transaction metadata. Helius states this directly for its own streaming product: "A classic SPL `Transfer` instruction never references the mint ... so a plain account filter misses the most common operation on any token" ([Helius mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering)). This means any method that looks up transactions by address and treats the mint as that address is incomplete for this use case.

## Bottom line

1. **Bulk 30-day load: Old Faithful CAR archives streamed with Jetstreamer, maintained by Vaultic.** Use v0.10.0, or Carbon's `jetstreamer-datasource` from git main. Run it close to Amsterdam and do the filtering in your own code. Keep a transaction when its pre/post token-balance mints contain USDC or USDT, not when its account keys contain the mint. Then decode outer instructions, inner instructions, and p-token `Batch` members.
   - Data volume: the last 30 published epochs (1067–1096) total about **10.4 TB** of CAR files ([CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md)).
   - Cost: no Helius credits.
   - Triton recommends this path for bulk pulls ([Triton historical data](https://docs.triton.one/solana/historical-data)).
2. **Recent range that is not archived yet: LaserStream replay with `fromSlot` and `matchMints: true`.** Read `SubscribeReplayInfo.first_available` first. Replay honors `matchMints` ([historical replay](https://www.helius.dev/docs/laserstream/historical-replay)).
   - The un-archived range is the current epoch (about 25.8 h) plus publication lag. I measured that lag at about 5–6 h on 2026-11-25 and 2026-11-26.
   - That totals about 31–32 h, which fits inside the roughly 48 h replay window. This only holds while publication stays under about 22 h.
3. **Gaps older than replay:**
   - If the gap falls inside an archived epoch, use a Jetstreamer slot-range run.
   - If it falls in an epoch not yet archived, which happens only when publication is late, use Helius `getBlocks` + `getBlock` with `maxSupportedTransactionVersion: 1`. This costs 1 credit per block ([credits](https://www.helius.dev/docs/billing/credits.md)), which is about 12M credits for a full 30 days. That is cheap in credits but slow at the rate we measured (about 7 days at 20 slots/s).
   - `getTransactionsForAddress`, `getTransfersByAddress`, `getSignaturesForAddress` and Parsed Events Scan cannot cover a whole mint chain-wide (see §1–3).

**Changes from the earlier notes:**
- **Freshness.** Measured publication lag is about 5–6 h, so archive plus replay now usually meet without any RPC fill. The earlier notes assumed a routine gap. The OF1 page states a looser goal, "within 4 epochs", so the RPC fill is still needed as a fallback.
- **Parsed Events Scan.** The earlier note called scanning the mint address "plausible" for plain Transfers. The docs show that Scan walks only the mint address's own history, so it does not cover them (§2).
- **Carbon.** The published Carbon Jetstreamer crate is too old to use (§6).
- **p-token.** p-token's `Batch` instruction is a new decoder requirement (§10).
- **Slot count.** 30 days at 209 ms is about 12.40M slots, not 12.96M (12.96M assumes 200 ms). Epoch 1096 measured 214.9 ms/slot (92,854 s for 432,000 slots), which gives about **12.06M slots, or about 28 epochs**.

## Comparison

| Method | Plain `Transfer` coverage | Throughput / cost | Ops effort | Freshness | Verdict |
|---|---|---|---|---|---|
| Helius `getTransactionsForAddress` on the mint | **No**: only transactions that reference the address; `tokenAccounts` follows ownership, not mint | 10 credits per 100 full transactions; 1,000 per page; no batching; Developer+ | Low | Up to the tip | Use only for cross-checks and per-wallet history |
| Helius `getTransfersByAddress` | **No**: owner wallets only | 10 credits/request, 100 rows/page; Developer+ | Low | Up to the tip | Not for mint-wide work |
| Parsed Events Scan (mint address) | **No** (walks the mint's own address history) | 10 + 1 credit per 1,000 transactions scanned | Low | Up to the tip | Use only for cross-checks |
| `getSignaturesForAddress` + `getTransaction` | **No**: indexed by message account keys | 1 + 1 credit per transaction | Medium | Up to the tip | Do not use |
| Helius `getBlocks` + `getBlock` (v1) | **Yes**: full blocks with metadata | 1 credit/block, about 12M credits per 30 days; about 4 MB/block, about 48 TB JSON; Business 200 rps; measured 20 slots/s means about 7 days | Medium | Up to the tip | **Gap filler**, or an all-Helius fallback |
| Old Faithful + Jetstreamer (Vaultic) | **Yes**, if you filter on token-balance mints yourself | About 10.4 TB per 30 epochs; free public mirror; claims of 2.7M TPS on 64 cores and 30 Gbps | Medium–high (Rust, Agave dependencies, bandwidth) | Completed epochs only; about 5–6 h lag observed | **Bulk load** |
| Carbon `jetstreamer-datasource` | Yes, but only with custom filtering: its built-in filter matches account keys only | Same as Jetstreamer | Medium | Same | Use from git main, not crates.io 1.0.0 |
| Carbon `rpc-block-crawler-datasource` | Yes (full blocks) | Same as `getBlock` | Low–medium | Up to the tip | It can silently drop blocks; wrap it with retries |
| LaserStream replay + `matchMints` | **Yes** | 2 credits per 0.1 MB; Business: 10 connections | Low (same code as live) | About the last 48 h only | **Recent window** |
| Dune / Allium / Flipside / BigQuery / Artemis | Dune and Allium: yes, at instruction level with inner index | Paid; exports metered | Low to set up, but a format mismatch | Dune: about 24 h standard; others UNVERIFIED | Use for reconciliation counts only |
| SQD Portal (Solana) | Yes: filter on token-balance `preMint`/`postMint` and join instructions | UNVERIFIED pricing and rate limits | Medium | Real time; `start_block` 0 | A credible alternative; not evaluated |
| Firehose / Substreams | Yes (full blocks) | UNVERIFIED | High | Up to the tip | Too much for 30 days |

## 1. Helius `getTransactionsForAddress` (gTFA)

**(a) Coverage.** It can be pointed at a mint address, and Helius's own example queries a mint to find its creation transaction ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)). But the filters define what it returns:
- `none` (default) returns "only transactions that reference the provided address" ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).
- `balanceChanged` and `all` add transactions involving token accounts "owned by the provided address" ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)). A mint does not own its holders' token accounts, so neither option helps.
- Ownership comes from the `owner` field in token-balance metadata, which only exists from slot 111,491,819 ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).
- The `tokenTransfer.mint` filter narrows transfers in which *the queried address participated*. It does not widen the result set ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).

**Conclusion:** gTFA on the USDC mint returns the activity that names the mint (TransferChecked, MintTo, Burn, swaps that pass the mint), not every transfer. That is an owner-or-reference view, not a mint-wide one. Whether Helius's address index also keys on token-balance mints is **UNVERIFIED** (see Open questions). Nothing in the docs says it does, and Helius's LaserStream doc treats "mint not in account keys" as the reason a plain filter misses transfers ([mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering)).

**(b) Throughput and cost:**
- Full transactions cost 10 credits per 100 returned (10-credit minimum); signatures-only costs 10 credits flat ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).
- `limit` goes up to 1,000 in both modes.
- No batch requests: one address per request ([rate limits](https://www.helius.dev/docs/billing/rate-limits.md)).
- Developer plan or higher ([credits](https://www.helius.dev/docs/billing/credits.md)).
- Business plan: 200 RPC req/s, 100M credits/month, $499/month ([plans](https://www.helius.dev/docs/billing/plans)).

**(c) Ops effort.** Low. Filter by `slot`, `blockTime`, `signature` and `status`; use `sortOrder: asc`; page with a `"slot:position"` token ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).

**(d) Freshness.** Mainnet retention is "Unlimited" ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)). It supports `finalized`/`confirmed` and `maxSupportedTransactionVersion: 1` for v1 transactions ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).

**(e) Gotchas:**
- Without `maxSupportedTransactionVersion`, it returns only legacy transactions ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)).
- System, Vote, Memo and Compute Budget fall back to a slot scan of at most 100 slots, and many sysvars return empty ([gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress)). The Token programs are not on that list, but whether querying `Tokenkeg...` across the whole chain is practical is UNVERIFIED and likely uneconomic.
- Making gTFA complete for a mint would need a gTFA call for every USDC/USDT token account. That is impractical.

## 2. Helius `getTransfersByAddress` and Parsed Events Scan

**`getTransfersByAddress` exists.** It returns parsed transfer rows with instruction, inner-instruction and transaction indexes. It covers Token/Token-2022 `Transfer`/`TransferChecked`, `TransferCheckedWithFee` with fee fields, mints, burns and `SetAuthority` owner changes ([getTransfersByAddress](https://www.helius.dev/docs/rpc/gettransfersbyaddress)).
- **Coverage: owner only.** "Pass the wallet owner address, not an associated token account" ([getTransfersByAddress](https://www.helius.dev/docs/rpc/gettransfersbyaddress)). Its `mint` parameter filters that wallet's transfers. It is not usable for mint-wide history.
- **Cost and limits:**
  - 10 credits/request, `limit` 1–100, Developer+ ([getTransfersByAddress](https://www.helius.dev/docs/rpc/gettransfersbyaddress)).
  - No batching ([rate limits](https://www.helius.dev/docs/billing/rate-limits.md)).
  - V1 excludes failed transactions and `harvestWithheldTokensToMint` ([getTransfersByAddress](https://www.helius.dev/docs/rpc/gettransfersbyaddress)).
- **Use:** a good reconciliation oracle for a sample of wallets.

**Parsed Events Scan** (`POST /v1/parsed-events/scan`) "walks one address's history" ([Scan reference](https://www.helius.dev/docs/api-reference/parsed-events/scan)).
- A `match.mints` entry tests the pre/post token-balance mints of transactions *in that history*. `transaction` scope widens which balances count, not which transactions are walked ([Scan reference](https://www.helius.dev/docs/api-reference/parsed-events/scan)).
- The Pump.fun example says a mint's history holds that token's activity because "each buy or sell instruction lists the mint as an account" ([Scan reference](https://www.helius.dev/docs/api-reference/parsed-events/scan)). That is consistent with history meaning "transactions that reference the address".
- **This changes the user's 2026-11-15 note:** plain `Transfer`s are very likely absent, though this is UNVERIFIED by test.
- **Cost:** 10 credits + 1 credit per history page of up to 1,000 transactions; at most 10,000 scanned per request; available on all plans ([Parsed Events](https://www.helius.dev/docs/parsed-events)).

## 3. `getSignaturesForAddress` + `getTransaction`

- **(a) Coverage: no.** Agave's transaction-status service writes the address-signature index from `message.account_keys()` (static plus loaded keys) ([Agave `transaction_status_service.rs`](https://github.com/anza-xyz/agave/blob/7fc7ecd8c066248a3c0759b1d2d30b79da31d3d0/rpc/src/transaction_status_service.rs#L359-L373), [`blockstore.rs`](https://github.com/anza-xyz/agave/blob/7fc7ecd8c066248a3c0759b1d2d30b79da31d3d0/ledger/src/blockstore.rs#L4506-L4552)). A plain `Transfer` never has the mint in its keys. Helius also says the method "does not include associated token accounts" ([historical data overview](https://www.helius.dev/docs/rpc/historical-data)).
- **(b) Cost:** 1 credit per call each ([credits](https://www.helius.dev/docs/billing/credits.md)). `limit` max is 1,000; no batching for `getSignaturesForAddress` ([gSFA guide](https://www.helius.dev/docs/rpc/guides/getsignaturesforaddress)). `getTransaction` batches up to 100 ([rate limits](https://www.helius.dev/docs/billing/rate-limits.md)).
- **(c–e):** You page newest to oldest and make an extra call per transaction ([indexing guide](https://www.helius.dev/docs/rpc/how-to-index-solana-data)). Use `maxSupportedTransactionVersion: 1` or v1 transactions fail with `-32015` ([getBlock guide](https://www.helius.dev/docs/rpc/guides/getblock)). **Verdict:** strictly worse than gTFA for this job.

## 4. RPC `getBlocks` / `getBlock` on Helius

**(a) Coverage: complete.** You get every transaction with its full metadata (inner instructions, token balances, loaded addresses), so mint matching on token balances works.
- The protobuf `TransactionStatusMeta` carries `inner_instructions`, `pre/post_token_balances` and `loaded_writable/readonly_addresses` ([Agave `confirmed_block.proto`](https://github.com/anza-xyz/agave/blob/master/storage-proto/proto/confirmed_block.proto)).
- `maxSupportedTransactionVersion` must be 1. Otherwise a block containing a higher version fails with `-32015` ([getBlock guide](https://www.helius.dev/docs/rpc/guides/getblock)). This matches our measurement that about half of transactions are v1.

**(b) Throughput and cost:**
- `getBlock` and `getBlocks` each cost 1 credit ([credits](https://www.helius.dev/docs/billing/credits.md)).
- `getBlocks` covers at most 500,000 slots per call ([getBlocks guide](https://www.helius.dev/docs/rpc/guides/getblocks)).
- Archival batches are limited to 10 ([getBlock guide](https://www.helius.dev/docs/rpc/guides/getblock)).
- A typical block is about 4 MB ([credits](https://www.helius.dev/docs/billing/credits.md)). Our sample block was 3.1–3.6 MB of JSON.
- **Total for 30 days:** about 12.06M blocks (minus skipped slots) is about **12M credits**, or about 12% of Business's 100M monthly allowance (overage is $5/M) ([plans](https://www.helius.dev/docs/billing/plans)). That is about **48 TB of JSON** transferred.
- **Time:**
  - Business allows 200 RPC req/s ([rate limits](https://www.helius.dev/docs/billing/rate-limits.md)), so the ceiling is about 16.8 h. That would need about 800 MB/s of ingest and decode.
  - Our measured 20 slots/s with 4 workers gives **about 7 days**.
  - Professional allows 500 req/s and +100 rps for $100/month ([rate limits](https://www.helius.dev/docs/billing/rate-limits.md)).
  - There is no `getBlock`-specific rate limit listed.

**(c) Ops effort.** Medium: slot planning, skipped-slot handling, retries, and decoding multi-MB JSON.

**(d) Freshness.** Up to the finalized tip. Helius says its archive spans genesis to present ([historical data overview](https://www.helius.dev/docs/rpc/historical-data)).

**(e) Helius's own guidance.** `getBlock` is "inherently wasteful" unless many transactions per block are relevant or "address-based filtering cannot capture your target data" ([indexing guide](https://www.helius.dev/docs/rpc/how-to-index-solana-data)). This use case is exactly the second condition.

**No separate Helius bulk or archive product appears in the docs index** ([Helius llms.txt](https://www.helius.dev/docs/llms.txt)).

## 5. Old Faithful CAR archives + Jetstreamer

**Who maintains what:**
- Anza archived `anza-xyz/jetstreamer`; its last push was 2026-10-15. Its README says "Development will continue with Vaultic" ([Anza repo](https://github.com/anza-xyz/jetstreamer)).
- `vaultic-win/jetstreamer` is active, with its latest release **v0.10.0 on 2026-11-11** ([Vaultic repo](https://github.com/vaultic-win/jetstreamer), [releases](https://github.com/vaultic-win/jetstreamer/releases)).
- crates.io still shows `jetstreamer` 0.8.1 (2026-10-09), pointing at the Anza repo ([crates.io](https://crates.io/crates/jetstreamer)).
- Triton One maintains OF1, the full CAR copy, "provided with a grant from Solana Foundation", at `files.old-faithful.net`, stored in Amsterdam ([OF1](https://docs.old-faithful.net/running-old-faithful/sourcing-data/of1), [What is Old Faithful](https://docs.old-faithful.net/introduction/what-is-old-faithful)).

**(a) Coverage: complete for this use case.**
- Each archived transaction stores raw transaction data plus a metadata frame ([ledger schema](https://github.com/rpcpool/yellowstone-faithful/blob/main/ledger.ipldsch)).
- The metadata is the status-meta protobuf above, with inner instructions, token balances and loaded addresses. Each epoch has a "tx meta check" ([CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md)).
- Transactions are stored "already-executed", and there are **no account updates** ([Vaultic README](https://github.com/vaultic-win/jetstreamer)).
- Jetstreamer parses legacy, v0 and v1 transactions from crate 0.8.1 onward ([Triton best practices](https://docs.triton.one/solana/historical-data/best-practices)).
- Our earlier bounded test decoded 203 v1 transactions out of 472 in one slot (user notes, `archive.md`).

**(b) Throughput and volume:**
- Jetstreamer claims more than 2.7M TPS "given the right hardware" (64 cores, 30 Gbps+) ([README](https://github.com/vaultic-win/jetstreamer)).
- By default it runs in a "polite" mode against the shared public mirror; a `--turbo` flag removes the pacing ([README](https://github.com/vaultic-win/jetstreamer)).
- Recent epoch CARs are 263–432 GB each. Epochs 1067–1096 total about **10.4 TB** ([CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md)).
- That is about 23 h at a sustained 1 Gbps, or about 2.3 h at 10 Gbps (derived).
- Filtering happens in your process; the whole epoch is streamed. No Helius credits are used. Mirror bandwidth is free but shared. Sustained public-mirror throughput is UNVERIFIED.

**(c) Ops effort.** Medium–high:
- Rust 1.98.1+ and a large Agave dependency tree ([README](https://github.com/vaultic-win/jetstreamer)).
- Slot offsets come from per-epoch `epoch-{N}-slot-ranges.raw` files of about 5 MB.
- S3 mirrors are supported ([README](https://github.com/vaultic-win/jetstreamer)).
- `--reverse` processes epochs newest-first ([README](https://github.com/vaultic-win/jetstreamer)).

**(d) Freshness:**
- One CAR per completed epoch. "Expect it to be online within 10-20h after epoch end" ([What is Old Faithful](https://docs.old-faithful.net/introduction/what-is-old-faithful)), but the OF1 page says "within 4 epochs" ([OF1](https://docs.old-faithful.net/running-old-faithful/sourcing-data/of1)).
- What I observed on 2026-11-26:
  - Epoch 1097 is ongoing ([CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md), updated 2026-11-26 04:22 UTC).
  - Epoch 1096 ended at 2026-11-25 21:53:12 UTC (`getBlockTime(473904000)` on public RPC).
  - `epoch-1096.car` (420,503,815,503 bytes) has `Last-Modified` 03:15:17 UTC, so it arrived **about 5 h 22 m** after epoch end.
  - Epoch 1095 ended 2026-11-24 20:05:38 UTC and its CAR has `Last-Modified` 2026-11-25 02:15:10, so **about 6 h 10 m**.
- These are two single-observation HTTP HEADs, not an SLA.
- At most, the un-archived tail is one epoch (about 25.8 h) plus publication lag.

**(e) Gotchas:**
- `files.old-faithful.net` returns 403 to Jetstreamer user agents v0.4–v0.6. Use 0.7.0 or later ([Triton best practices](https://docs.triton.one/solana/historical-data/best-practices)).
- Epoch 0–156 data is bincode, not protobuf, and some early epochs have no metadata ([sharp edges](https://docs.old-faithful.net/usage/validation/sharp-edges)). This does not affect a 30-day window.
- Our earlier test saw longer, untruncated logs in the archive than in RPC (130 RPC lines with "Log truncated" versus 194 in the archive; user notes, `archive.md`).

## 6. Carbon datasources

Carbon's main branch is at 2.0.0 with active pushes (2026-11-25) ([Carbon repo](https://github.com/sevenlabs-hq/carbon)). Its datasources include `jetstreamer-datasource`, `rpc-block-crawler-datasource`, `rpc-transaction-crawler-datasource`, and three Helius-specific ones: `helius-laserstream-datasource`, `helius-gtfa-datasource` and `helius-gpa-v2-datasource` ([datasources dir](https://github.com/sevenlabs-hq/carbon/tree/main/datasources)).

- **`carbon-jetstreamer-datasource`:**
  - The crates.io release is only **1.0.0 (2026-06-04)**, which depends on `jetstreamer-firehose ^0.2.0` ([crates.io](https://crates.io/crates/carbon-jetstreamer-datasource)). That version predates v1 parsing (0.8.1+).
  - Git main (2.0.0, unpublished) vendors Vaultic's firehose at commit `59d4ced` ([README](https://github.com/sevenlabs-hq/carbon/tree/main/datasources/jetstreamer-datasource)).
  - **Gotcha:** its `TransactionFilter` matches only static plus loaded account keys ([lib.rs](https://github.com/sevenlabs-hq/carbon/blob/722f9a0f2ae2e63e5d8b4d54421f04c0b9d00467/datasources/jetstreamer-datasource/src/lib.rs#L335-L370)). Putting the USDC mint in `account_include` would miss plain Transfers. Instead, include both token program IDs and filter by token-balance mint in the processor.
- **`carbon-rpc-block-crawler-datasource` 2.0.0 (2026-09-05):**
  - Defaults: 10 concurrent requests, 100 ms interval, `confirmed` commitment ([lib.rs](https://github.com/sevenlabs-hq/carbon/blob/722f9a0f2ae2e63e5d8b4d54421f04c0b9d00467/datasources/rpc-block-crawler-datasource/src/lib.rs)).
  - **Gotcha:** errors `-32004`, `-32007` and `-32009` are counted as skipped, and any other error is only logged. The block is dropped with no retry, so you get silent gaps. Set `finalized` and `max_supported_transaction_version: 1` in `block_config`, and wrap it with retries.
- **`carbon-helius-gtfa-datasource` 2.0.0:**
  - It has 67 downloads (low maturity) ([crates.io](https://crates.io/crates/carbon-helius-gtfa-datasource)).
  - It caps `limit` at 100, exposes no `tokenAccounts` filter, and sends `maxSupportedTransactionVersion: 1` ([lib.rs](https://github.com/sevenlabs-hq/carbon/blob/722f9a0f2ae2e63e5d8b4d54421f04c0b9d00467/datasources/helius-gtfa-datasource/src/lib.rs)).
  - It inherits gTFA's mint limitation.
- **`carbon-helius-laserstream-datasource` 2.0.0:** about 2.9k downloads ([crates.io](https://crates.io/crates/carbon-helius-laserstream-datasource)). Whether it exposes `matchMints` was not checked (UNVERIFIED).

## 7. LaserStream historical replay

- **Window:** "up to ~48 hours of history (~864,000 slots at ... ~200ms)" ([historical replay](https://www.helius.dev/docs/laserstream/historical-replay)). At the measured 214.9 ms/slot, 48 h is about 804k slots (derived).
- **Commitment:** data older than about 20 minutes comes from a finalized-only store.
- **`matchMints`:** "Replay honors the `matchMints` transaction filter flag, so a mint-filtered replay returns the same set of transactions the live stream would have delivered" ([historical replay](https://www.helius.dev/docs/laserstream/historical-replay)).
  - `matchMints` matches account lists against account keys **or** the mint set from pre/post token balances, for both SPL Token and Token-2022 ([mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering)).
  - It needs `helius-laserstream` 0.8.5 or later ([mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering)).
- **Availability check:** `SubscribeReplayInfo` returns `first_available`. If the field is absent, replay is unavailable. The value can move before you subscribe ([SubscribeReplayInfo](https://www.helius.dev/docs/api-reference/laserstream/grpc/subscribereplayinfo)).
- **Plan and cost:**
  - Mainnet needs Business or Professional ([historical replay](https://www.helius.dev/docs/laserstream/historical-replay)).
  - Business allows 10 active connections ([rate limits](https://www.helius.dev/docs/billing/rate-limits.md)), so a separate replay connection can run beside the live one.
  - Billing is 2 credits per 0.1 MB uncompressed ([credits](https://www.helius.dev/docs/billing/credits.md)). The 48 h USDC/USDT volume is UNVERIFIED.
- **Can replay alone cover part of the backfill?** Yes, the last ~48 h (about 6.7% of 30 days). Replay streams forward to the present, so it cannot fetch an arbitrary older slice ([historical replay](https://www.helius.dev/docs/laserstream/historical-replay)).
- **Completion caveat:** A quiet filtered stream does not prove a slot had no matches (user notes, `research/2026-11-13-laserstream-replay-contract.md`).

## 8. Third-party datasets

- **Dune:**
  - `tokens_solana.transfers` covers SPL, Token-2022 and native SOL, with `outer_instruction_index`/`inner_instruction_index`. It warns that "non-standard transfer methods ... might not be included" ([Dune token transfers](https://docs.dune.com/data-catalog/curated/token-transfers/solana/solana-token-transfers)).
  - `stablecoins_solana.transfers` has one row per instruction ([Dune stablecoins](https://docs.dune.com/data-catalog/curated/stablecoins/transfers/stablecoins-solana-transfers)).
  - Datashare freshness: "24 hours for most tables", hourly on Enterprise ([Datashare FAQ](https://docs.dune.com/datashare/datashare-faqs)).
  - Exports cost 2–10 credits/MB depending on plan; the free tier has no exports ([export](https://docs.dune.com/learning/how-tos/export-data-out)).
- **Allium:**
  - `solana.assets.transfers` covers SPL, Token-2022 and SOL ([Allium transfers](https://docs.allium.so/historical-data/supported-blockchains/solana/solana/assets/transfers)).
  - Since 22–23 October 2026 it includes p-token and prefund instructions, with a `batch_instruction_index` column for `Batch` members ([Allium changelog](https://docs.allium.so/changelog/solana-data-corrections-2026)).
  - Freshness and pricing are UNVERIFIED (gated docs).
- **Flipside:** `solana.core.fact_transfers`/`ez_transfers` exist ([Flipside](https://docs.flipsidecrypto.xyz/blockchain-data/solana/core/ez-transfers)). The docs host failed TLS, so the inner-instruction handling and freshness are UNVERIFIED.
- **Google BigQuery:** Solana appears on the supported list ([Google](https://docs.cloud.google.com/blockchain-analytics/docs/supported-datasets)). The schema page has no Solana section, so table names and freshness are UNVERIFIED.
- **Artemis:** offers a Snowflake share and an API of "core datasets"; the public docs give no instruction-level Solana data ([Artemis docs](https://www.artemis.ai/docs)). UNVERIFIED.
- **Triton:**
  - Superbank, AGPL, offers JSON-RPC history including its own `getTransactionsForAddress`; for bulk pulls Triton points to Jetstreamer and Old Faithful ([Triton historical data](https://docs.triton.one/solana/historical-data)).
  - Fumarole is a persistent gRPC stream with 4-day rolling replay ([Fumarole](https://docs.triton.one/solana/real-time-streaming/fumarole-persistent-streams)). It is a non-Helius alternative to the 48 h replay.
- **Practicality for a 30-day load:** As backfill sources, these datasets have row formats and identities that differ from your decoder, and their freshness is a day or more. They are worth using for per-day count and amount reconciliation, not as the source of truth.

## 9. Other credible options

- **SQD Portal:**
  - The public Solana dataset (`real_time: true`, `start_block: 0`, from a live `/metadata` call) supports `tokenBalances` filters on `preMint`/`postMint`, with joins to the parent transaction and all its instructions ([SQD stream API](https://docs.sqd.dev/en/api/solana/stream.md)).
  - That gives server-side mint-wide matching, including plain Transfers.
  - Pricing, rate limits and throughput are UNVERIFIED.
- **StreamingFast Firehose for Solana:** active, v1.7.0 released 2026-11-24 ([firehose-solana](https://github.com/streamingfast/firehose-solana/releases)). There is a Substreams SPL token package ([substreams.dev](https://substreams.dev/packages/solana-spl-token/latest)). Endpoint and pricing are UNVERIFIED. It is a heavy stack for 30 days.
- **Old Faithful gRPC `StreamTransactions`:** server-side filtering, but you run the Old Faithful server yourself ([What is Old Faithful](https://docs.old-faithful.net/introduction/what-is-old-faithful)).
- **Helius indexing guide:** recommends gTFA for address-scoped backfill, `getBlock` when address filtering cannot capture the data, and LaserStream for live data ([indexing guide](https://www.helius.dev/docs/rpc/how-to-index-solana-data)).

## 10. Decoder requirement that applies to every source

p-token defines `Batch = 255`, a single Token-program instruction whose data packs several sub-instructions, each with its own discriminator and accounts ([p-token `instruction.rs`](https://github.com/solana-program/token/blob/main/pinocchio/interface/src/instruction.rs)). It also adds `UnwrapLamports = 45` ([same file](https://github.com/solana-program/token/blob/main/pinocchio/interface/src/instruction.rs)). Allium reports 49.2M p-token and prefund rows since 13 May 2026 ([Allium changelog](https://docs.allium.so/changelog/solana-data-corrections-2026), secondary).

Plain `Transfer`s inside a `Batch` keep the same instruction and inner-instruction index. A stable transfer ID therefore needs a batch-member index as well.

## Open questions / unverified

- **Does Helius's gTFA (and Scan) address index include token-balance mints?** This is the one test that would settle §1–2:
  1. Take a captured plain-`Transfer` USDC signature from LaserStream.
  2. Call gTFA on the USDC mint with `filters.slot` set to that slot, `transactionDetails: "signatures"` (10 credits).
  3. If the signature is absent, gTFA does not cover plain Transfers.

  It is not run here (paid API).
- Sustained Jetstreamer throughput from the public mirror at default ("polite") versus `--turbo` settings, and from a non-Amsterdam host.
- Whether the observed 5–6 h CAR publication lag holds over time. The two docs state 10–20 h and "within 4 epochs". `Last-Modified` may also precede index availability.
- USDC+USDT matching transaction volume per day. This drives replay credits (2 credits/0.1 MB) and gTFA cost.
- Freshness, pricing and inner-instruction handling for Allium, Flipside, BigQuery and Artemis. SQD Portal limits and pricing.
- Whether Carbon's LaserStream datasource passes `matchMints`.
- The exact Helius archive behavior on skipped slots and `-32004` and `-32009` for `getBlock`.

## Sources

- Helius: [gTFA guide](https://www.helius.dev/docs/rpc/gettransactionsforaddress), [gTFA API ref](https://www.helius.dev/docs/api-reference/rpc/http/gettransactionsforaddress), [getTransfersByAddress](https://www.helius.dev/docs/rpc/gettransfersbyaddress), [Parsed Events](https://www.helius.dev/docs/parsed-events), [Scan reference](https://www.helius.dev/docs/api-reference/parsed-events/scan), [historical data overview](https://www.helius.dev/docs/rpc/historical-data), [indexing guide](https://www.helius.dev/docs/rpc/how-to-index-solana-data), [getBlock guide](https://www.helius.dev/docs/rpc/guides/getblock), [getBlocks guide](https://www.helius.dev/docs/rpc/guides/getblocks), [gSFA guide](https://www.helius.dev/docs/rpc/guides/getsignaturesforaddress), [rate limits](https://www.helius.dev/docs/billing/rate-limits.md), [credits](https://www.helius.dev/docs/billing/credits.md), [plans](https://www.helius.dev/docs/billing/plans), [historical replay](https://www.helius.dev/docs/laserstream/historical-replay), [SubscribeReplayInfo](https://www.helius.dev/docs/api-reference/laserstream/grpc/subscribereplayinfo), [mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering), [llms.txt](https://www.helius.dev/docs/llms.txt)
- Agave: [transaction_status_service.rs @7fc7ecd](https://github.com/anza-xyz/agave/blob/7fc7ecd8c066248a3c0759b1d2d30b79da31d3d0/rpc/src/transaction_status_service.rs), [blockstore.rs @7fc7ecd](https://github.com/anza-xyz/agave/blob/7fc7ecd8c066248a3c0759b1d2d30b79da31d3d0/ledger/src/blockstore.rs), [confirmed_block.proto](https://github.com/anza-xyz/agave/blob/master/storage-proto/proto/confirmed_block.proto)
- Old Faithful: [What is Old Faithful](https://docs.old-faithful.net/introduction/what-is-old-faithful), [OF1](https://docs.old-faithful.net/running-old-faithful/sourcing-data/of1), [OF1 files](https://docs.old-faithful.net/references/of1-files), [sharp edges](https://docs.old-faithful.net/usage/validation/sharp-edges), [CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md), [ledger schema](https://github.com/rpcpool/yellowstone-faithful/blob/main/ledger.ipldsch); HTTP HEAD on `https://files.old-faithful.net/1096/epoch-1096.car` and `/1095/epoch-1095.car` (2026-11-26 06:22 UTC); public mainnet `getEpochInfo`/`getBlockTime`
- Jetstreamer: [Anza (archived)](https://github.com/anza-xyz/jetstreamer), [Vaultic](https://github.com/vaultic-win/jetstreamer), [Vaultic releases](https://github.com/vaultic-win/jetstreamer/releases), [crates.io jetstreamer](https://crates.io/crates/jetstreamer)
- Carbon: [repo](https://github.com/sevenlabs-hq/carbon), [jetstreamer-datasource](https://github.com/sevenlabs-hq/carbon/tree/main/datasources/jetstreamer-datasource), [rpc-block-crawler lib.rs](https://github.com/sevenlabs-hq/carbon/blob/722f9a0f2ae2e63e5d8b4d54421f04c0b9d00467/datasources/rpc-block-crawler-datasource/src/lib.rs), [helius-gtfa lib.rs](https://github.com/sevenlabs-hq/carbon/blob/722f9a0f2ae2e63e5d8b4d54421f04c0b9d00467/datasources/helius-gtfa-datasource/src/lib.rs), crates.io pages for [carbon-jetstreamer-datasource](https://crates.io/crates/carbon-jetstreamer-datasource), [carbon-helius-gtfa-datasource](https://crates.io/crates/carbon-helius-gtfa-datasource), [carbon-helius-laserstream-datasource](https://crates.io/crates/carbon-helius-laserstream-datasource)
- Triton: [historical data](https://docs.triton.one/solana/historical-data), [best practices](https://docs.triton.one/solana/historical-data/best-practices), [Fumarole](https://docs.triton.one/solana/real-time-streaming/fumarole-persistent-streams)
- p-token: [instruction.rs](https://github.com/solana-program/token/blob/main/pinocchio/interface/src/instruction.rs)
- Third parties: [Dune token transfers](https://docs.dune.com/data-catalog/curated/token-transfers/solana/solana-token-transfers), [Dune stablecoins](https://docs.dune.com/data-catalog/curated/stablecoins/transfers/stablecoins-solana-transfers), [Dune Datashare FAQ](https://docs.dune.com/datashare/datashare-faqs), [Dune export](https://docs.dune.com/learning/how-tos/export-data-out), [Allium transfers](https://docs.allium.so/historical-data/supported-blockchains/solana/solana/assets/transfers), [Allium changelog](https://docs.allium.so/changelog/solana-data-corrections-2026), [Flipside ez_transfers](https://docs.flipsidecrypto.xyz/blockchain-data/solana/core/ez-transfers), [Google supported datasets](https://docs.cloud.google.com/blockchain-analytics/docs/supported-datasets), [Artemis docs](https://www.artemis.ai/docs), [SQD stream API](https://docs.sqd.dev/en/api/solana/stream.md), [firehose-solana](https://github.com/streamingfast/firehose-solana/releases), [Substreams SPL token package](https://substreams.dev/packages/solana-spl-token/latest)
- User notes re-checked: `/Users/alexnham/Documents/ChatGPT/indexer/docs/{historical-source,archive,source-boundary,30-day-backfill,backfill-replan}.md`, `docs/research/2026-11-13-laserstream-replay-contract.md`, `docs/research/2026-11-15-helius-parsed-events-scan.md`
