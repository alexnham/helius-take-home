# Backfill benchmark

Measured 2026-11-26. One day of USDC + USDT transfers: slots 473,480,000–473,882,599 (402,600 slots, about 24 h, inside fully archived epoch 1096).

30-day figures extrapolate each measured rate to 12,078,707 slots (measured with `getBlockTime`: 30.000 days at 214.6 ms per slot). All runs used 2,000-slot ranges from the same `backfill_ranges` table and wrote with `INSERT … ON CONFLICT DO NOTHING`.

## Rust (`samples-rust`)

| Method | Rate | 1 day | 30 days | Downloaded | CPU (of 8 vCPU) | Helius credits |
|---|---|---|---|---|---|---|
| Old Faithful archive via Jetstreamer (`src/bin/backfill_archive.rs`) | 330 slots/s | 20 min (full day measured) | ~10 h | 404 GB | 89% | 0 |
| Helius `getBlocks` + `getBlock` (`src/bin/backfill_rpc.rs`) | 193 slots/s | ~35 min (from a 40,000-slot run: 207 s) | ~17 h | 20.6 GB per 40k slots (~0.5 MB/slot gzipped) | 61% | 1 per block (~12M for 30 days) |

Settings:
- **Archive:** 4 processes × 8 Jetstreamer threads, reading `files.old-faithful.net` (public mirror, Amsterdam) with Jetstreamer v0.10.0 (Vaultic, rev `8f91a4a`). One transaction per range, multi-row inserts.
- **RPC:** 1 process; 8 ranges at once × 24 blocks in flight per range, capped at 200 requests/s. `encoding: json`, `maxSupportedTransactionVersion: 1`, gzip, HTTP/1.1. Transactions are parsed only if their raw text mentions USDC or USDT; decoding runs on blocking threads. One transaction per range, inserting up to 10,000 rows per statement.

Limits: RPC is capped by the Business plan's 200 requests/s, not by the machine (CPU 61%). The archive is limited by CPU and download speed, and has no request cap.

## TypeScript (`samples-ts`)

| Method | Rate | 1 day | 30 days | Downloaded | CPU (of 8 vCPU) | Helius credits |
|---|---|---|---|---|---|---|
| Helius `getBlocks` + `getBlock` (`backfill.ts`) | 89 slots/s | 75 min (full day measured) | ~38 h | 237 GB | 92% | 1 per block (~12M for 30 days) |

Settings: 8 processes × 12 workers, each process capped at 25 requests/s (200 in total). `encoding: json`, `maxSupportedTransactionVersion: 1`, Node's built-in `fetch` (gzip, HTTP/1.1). One transaction per block.

Limit: CPU. Node spends nearly all its time in `JSON.parse` on ~2–4 MB blocks, so it reaches less than half the plan's request limit. Jetstreamer is Rust-only, so there's no TypeScript archive path.

## Correctness

- **Full day:** the Rust archive run and the TypeScript RPC run each wrote 14,142,353 transfers, identical row for row (matching fingerprints).
- **40,000-slot slice:** the Rust RPC run wrote 1,270,795 transfers, identical to the TypeScript rows for the same slots.
- **Ranges:** every range completed. One archive range failed once ("slot missing from archive output") and succeeded on retry.

## Machine

| | |
|---|---|
| Provider / plan | Vultr Cloud Compute, `vc2-8c-32gb` |
| Region | Amsterdam (`ams`), same city as the Old Faithful mirror |
| CPU | Intel Xeon (Skylake, IBRS), 8 vCPU: 1 socket × 4 cores × 2 threads, 16 MiB L3 |
| Memory | 32 GB |
| Disk | 640 GB SSD (600 GB root filesystem) |
| Network | Not published by the provider; peak observed ~1.6 Gbit/s (an uncompressed RPC run averaged 202 MB/s) |
| OS | Ubuntu 24.04.5 LTS, kernel 6.8.0-149-generic |
| Toolchain | rustc 1.100.0 (release build), Node.js 24.22.3 |
| Database | PostgreSQL 17.12 in Docker on the same machine; `shared_buffers=4GB`, `max_connections=300`, `max_wal_size=16GB`; daily partitions, primary key plus three indexes (now split between `common/schema.sql` and `common/indexes.sql`; all were present during these runs) |

## Caveats

- **One machine and one day of data.** Rates vary with block size and network conditions.
- **Location favors the archive.** The machine sat next to the archive mirror in Amsterdam; elsewhere, the archive path would be slower.
- **The Rust RPC number comes from a 40,000-slot run**, not the full day.
- **Postgres ran on the same machine,** so writes competed with decoding for CPU.
