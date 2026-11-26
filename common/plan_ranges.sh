#!/usr/bin/env bash
# Fill backfill_ranges with the last DAYS of slots, split into RANGE_SIZE-slot ranges.
# Either backfill (samples-ts or samples-rust, RPC or archive) then claims them with claim_range.sql.
#
#   DAYS=30 RANGE_SIZE=2000 common/plan_ranges.sh
#
# Needs HELIUS_API_KEY and DATABASE_URL (read from .env at the repo root if present), plus curl, jq and psql.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && { set -a; . ./.env; set +a; }
DAYS=${DAYS:-30}
RANGE_SIZE=${RANGE_SIZE:-2000}
: "${HELIUS_API_KEY:?set HELIUS_API_KEY}" "${DATABASE_URL:?set DATABASE_URL}"

rpc() { # method, params JSON → result (the URL holds the key, so it's never echoed)
  curl -sf "https://mainnet.helius-rpc.com/?api-key=$HELIUS_API_KEY" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" | jq -ce '.result // error(.error.message)'
}
# First slot at or after $1 that produced a block (skipped slots have no block time).
produced_from() { rpc getBlocks "[$1, $(($1 + 100)), {\"commitment\":\"finalized\"}]" | jq -e '.[0]'; }
block_time() { rpc getBlockTime "[$1]"; }

end=${END_SLOT:-$(rpc getSlot '[{"commitment":"finalized"}]')}
cutoff=$(( $(block_time "$(produced_from $((end - 100)))") - DAYS * 86400 ))

# Binary search for the first block at or after the cutoff. Slot times drift, so don't assume a fixed slot length;
# the lower bound assumes slots average at least 100 ms, well under the ~215 ms measured.
lo=$(( end - DAYS * 864000 )) hi=$end
while (( hi - lo > 100 )); do
  mid=$(( (lo + hi) / 2 ))
  if (( $(block_time "$(produced_from "$mid")") < cutoff )); then lo=$mid; else hi=$mid; fi
done
start=$(produced_from "$hi")

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -c "
  INSERT INTO backfill_ranges (start_slot, end_slot)
  SELECT s, LEAST(s + $RANGE_SIZE - 1, $end) FROM generate_series($start::bigint, $end::bigint, $RANGE_SIZE) s
  ON CONFLICT DO NOTHING"   # re-running only adds ranges that aren't there yet

echo "planned slots $start..$end ($DAYS days, $(( end - start + 1 )) slots) in ranges of $RANGE_SIZE"
