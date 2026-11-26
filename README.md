# transfer indexer

Live LaserStream ingestion plus RPC and Old Faithful backfill into PostgreSQL. All paths share one decoder and one idempotent insert.

```
common/        schema.sql, claim_range.sql, plan_ranges.sh
samples-ts/    stream.ts, backfill.ts (RPC), transfers.ts (decoder)
samples-rust/  stream, backfill_rpc, backfill_archive (Jetstreamer)
docs/          benchmarks.md, research/backfill-methods.md
```

## Setup

```sh
cp .env.example .env              # set HELIUS_API_KEY
cd samples-ts && npm ci && npm run db   # Postgres on 127.0.0.1:55433 with the schema
```

## Run

```sh
common/plan_ranges.sh             # last 30 days into backfill_ranges (DAYS, RANGE_SIZE, END_SLOT)

# TypeScript (from samples-ts/)
npm run stream
npm run backfill -- work

# Rust (from samples-rust/; first build takes a few minutes)
set -a; . ../.env; set +a
cargo run --release --bin stream
cargo run --release --bin backfill_rpc -- work
cargo run --release --bin backfill_archive
```

## Test

```sh
cd samples-ts && npm test
cd samples-rust && cargo test
```
