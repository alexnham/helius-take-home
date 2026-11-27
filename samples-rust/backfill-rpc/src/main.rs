//! Backfill over Helius RPC getBlocks/getBlock. Same ranges table and decoder as the other paths.
//!   backfill_rpc plan <fromSlot> <toSlot> [rangeSize]
//!   backfill_rpc work
mod source;
use futures_util::{StreamExt, TryStreamExt, stream};
use common::{db, decode::{MINTS, Transfer, decode_transfers}, log::log};
use source::from_rpc;
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use std::{env, error::Error, sync::{Arc, atomic::{AtomicU64, Ordering::Relaxed}}, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{sync::Mutex, time::{Instant, sleep, sleep_until}};

type BoxError = Box<dyn Error + Send + Sync>;

const MAX_ATTEMPTS: i32 = 5;
const REPORT_EVERY: Duration = Duration::from_secs(10);

// Counters for the periodic progress report; the hot path only does relaxed adds.
struct Stats {
    ranges_done: AtomicU64,
    ranges_failed: AtomicU64,
    ranges_running: AtomicU64,
    slots: AtomicU64, // slots covered by finished ranges, skipped slots included
    blocks: AtomicU64,
    transfers: AtomicU64,
    rpc_retries: AtomicU64,
    bytes: AtomicU64, // response bodies after gzip decoding
}

static STATS: Stats = Stats {
    ranges_done: AtomicU64::new(0), ranges_failed: AtomicU64::new(0), ranges_running: AtomicU64::new(0),
    slots: AtomicU64::new(0), blocks: AtomicU64::new(0), transfers: AtomicU64::new(0),
    rpc_retries: AtomicU64::new(0), bytes: AtomicU64::new(0),
};

fn report(slots_before: u64, elapsed: Duration) -> u64 {
    let get = |c: &AtomicU64| c.load(Relaxed);
    let slots = get(&STATS.slots);
    log("info", "backfill_progress", json!({
        "ranges_done": get(&STATS.ranges_done), "ranges_failed": get(&STATS.ranges_failed),
        "ranges_running": get(&STATS.ranges_running),
        "slots_per_s": ((slots - slots_before) as f64 / elapsed.as_secs_f64()).round() as u64,
        "blocks": get(&STATS.blocks), "transfers": get(&STATS.transfers),
        "rpc_retries": get(&STATS.rpc_retries), "bytes": get(&STATS.bytes),
    }));
    slots
}

// reqwest's Display includes the request URL, and ours carries the API key.
fn without_url(e: reqwest::Error) -> String {
    let e = e.without_url();
    std::iter::successors(Some(&e as &dyn Error), |&e| e.source()).map(ToString::to_string).collect::<Vec<_>>().join(": ")
}

// A worker that died mid-range stops touching updated_at; hand its range back.
const RECLAIM_STALE: &str = "UPDATE backfill_ranges SET status = 'pending'
                             WHERE status = 'running' AND updated_at < now() - interval '10 minutes'";

struct Rpc {
    http: reqwest::Client,
    url: String,
    next_start: Mutex<Instant>,
    spacing: Duration,
}

impl Rpc {
    /// Raw response body, retrying 429/5xx. Callers parse only what they need.
    async fn post(&self, method: &str, params: Value) -> Result<Vec<u8>, BoxError> {
        for attempt in 0i32.. {
            // Space request starts evenly so all workers together stay under the plan's rate limit.
            let start = {
                let mut next = self.next_start.lock().await;
                let start = (*next).max(Instant::now());
                *next = start + self.spacing;
                start
            };
            sleep_until(start).await;

            let fail = |e| format!("{method} {}: {}", params[0], without_url(e));
            let res = self.http.post(&self.url)
                .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
                .send().await.map_err(fail)?;
            let status = res.status().as_u16();
            if (status == 429 || status >= 500) && attempt < 6 {
                STATS.rpc_retries.fetch_add(1, Relaxed);
                log("warn", "rpc_retry", json!({ "method": method, "slot": params[0], "status": status, "attempt": attempt }));
                let jitter = 0.5 + (SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos() % 1000) as f64 / 1000.0;
                sleep(Duration::from_millis(250).mul_f64(2f64.powi(attempt) * jitter)).await; // backoff with jitter
                continue;
            }
            let body = res.bytes().await.map_err(fail)?.to_vec();
            STATS.bytes.fetch_add(body.len() as u64, Relaxed);
            return Ok(body);
        }
        unreachable!()
    }
}

#[derive(Deserialize)]
struct Response<T> {
    result: Option<T>,
    error: Option<Box<RawValue>>,
}

impl<T> Response<T> {
    fn into_result(self, method: &str) -> Result<T, BoxError> {
        match (self.result, self.error) {
            (Some(result), None) => Ok(result),
            (_, error) => Err(format!("{method}: {}", error.map_or("no result".into(), |e| e.get().to_owned())).into()),
        }
    }
}

// Transactions stay unparsed text until we know they mention one of our mints.
#[derive(Deserialize)]
struct Block<'a> {
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
    #[serde(borrow)]
    transactions: Vec<&'a RawValue>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let db = db::connect().await?;
    match args.first().map(String::as_str) {
        Some("plan") => {
            let size = args.get(3).map_or(Ok(1000), |s| s.parse())?;
            db::plan_ranges(&db, args[1].parse()?, args[2].parse()?, size).await?;
        }
        Some("work") => {
            db.execute(RECLAIM_STALE, &[]).await?;
            let rpc = Arc::new(Rpc {
                // HTTP/2 multiplexes every request over one connection and caps throughput (78 vs 104 slots/s measured).
                http: reqwest::Client::builder().http1_only().pool_max_idle_per_host(256).build()?,
                url: format!("https://mainnet.helius-rpc.com/?api-key={}", env::var("HELIUS_API_KEY")?),
                next_start: Mutex::new(Instant::now()),
                spacing: Duration::from_secs(1) / env::var("BACKFILL_MAX_RPS").map_or(Ok(200), |r| r.parse())?, // Business plan limit
            });
            // Ranges in parallel x blocks in flight per range = requests in flight.
            let workers: usize = env::var("BACKFILL_WORKERS").map_or(Ok(8), |w| w.parse())?;
            let fetches: usize = env::var("BACKFILL_FETCHES_PER_RANGE").map_or(Ok(24), |f| f.parse())?;
            let started = Instant::now();
            tokio::spawn(async {
                let (mut slots, mut last) = (0, Instant::now());
                loop {
                    sleep(REPORT_EVERY).await;
                    slots = report(slots, last.elapsed());
                    last = Instant::now();
                }
            });
            let handles: Vec<_> = (0..workers).map(|_| tokio::spawn(worker(rpc.clone(), fetches))).collect();
            for handle in handles {
                handle.await?.map_err(|e| e.to_string())?;
            }
            report(0, started.elapsed()); // final totals, with the whole run's average rate
        }
        _ => return Err("usage: backfill_rpc plan <fromSlot> <toSlot> [rangeSize] | backfill_rpc work".into()),
    }
    Ok(())
}

async fn worker(rpc: Arc<Rpc>, fetches: usize) -> Result<(), BoxError> {
    let mut db = db::connect().await.map_err(|e| e.to_string())?;
    while let Some(row) = db.query_opt(db::CLAIM_RANGE, &[]).await? {
        let (start, end): (i64, i64) = (row.get(0), row.get(1));
        log("info", "range_claimed", json!({ "start": start, "end": end }));
        STATS.ranges_running.fetch_add(1, Relaxed);
        let began = Instant::now();
        let result = backfill_range(&rpc, &mut db, start, end, fetches).await;
        STATS.ranges_running.fetch_sub(1, Relaxed);
        match result {
            Ok((blocks, transfers)) => {
                STATS.ranges_done.fetch_add(1, Relaxed);
                STATS.slots.fetch_add((end - start + 1) as u64, Relaxed);
                log("info", "range_done", json!({
                    "start": start, "end": end, "duration_ms": began.elapsed().as_millis() as u64,
                    "blocks": blocks, "transfers": transfers,
                }));
            }
            Err(e) => {
                STATS.ranges_failed.fetch_add(1, Relaxed);
                log("error", "range_failed", json!({ "start": start, "end": end, "error": e.to_string() }));
                db::release_failed_range(&db, start, MAX_ATTEMPTS).await?;
            }
        }
    }
    Ok(())
}

/// Returns (blocks, transfers) written.
async fn backfill_range(rpc: &Arc<Rpc>, db: &mut tokio_postgres::Client, start: i64, end: i64, fetches: usize) -> Result<(usize, usize), BoxError> {
    // getBlocks lists only produced slots, so skipped slots are never requested.
    let body = rpc.post("getBlocks", json!([start, end, { "commitment": "finalized" }])).await?;
    let slots: Vec<u64> = serde_json::from_slice::<Response<Vec<u64>>>(&body)?.into_result("getBlocks")?;

    // Fetch many blocks at once; decode each on a blocking thread so parsing never stalls the downloads.
    let blocks: Vec<(u64, i64, Vec<Transfer>)> = stream::iter(slots)
        .map(|slot| {
            let rpc = rpc.clone();
            async move {
                let body = rpc.post("getBlock", json!([slot, {
                    "commitment": "finalized",
                    "encoding": "json",
                    "transactionDetails": "full",
                    "maxSupportedTransactionVersion": 1, // v1 (SIMD-0385) transactions are common; 0 makes the RPC reject the block
                    "rewards": false,
                }])).await?;
                let block = tokio::task::spawn_blocking(move || decode_block(slot, &body)).await?
                    .map_err(|e| BoxError::from(format!("slot {slot}: {e}")))?;
                STATS.blocks.fetch_add(1, Relaxed);
                Ok::<_, BoxError>(block)
            }
        })
        .buffer_unordered(fetches)
        .try_collect()
        .await?;

    // One transaction for the whole range: its transfers, then done. A crash before commit just redoes the range.
    let tx = db.transaction().await?;
    db::insert_batch(&tx, &blocks).await?;
    tx.execute("UPDATE backfill_ranges SET status = 'done', updated_at = now() WHERE start_slot = $1", &[&start]).await?;
    tx.commit().await?;
    let transfers: usize = blocks.iter().map(|b| b.2.len()).sum();
    STATS.transfers.fetch_add(transfers as u64, Relaxed);
    Ok((blocks.len(), transfers))
}

fn decode_block(slot: u64, body: &[u8]) -> Result<(u64, i64, Vec<Transfer>), BoxError> {
    let block = serde_json::from_slice::<Response<Block>>(body)?.into_result("getBlock")?;
    let block_time = block.block_time.ok_or("block without blockTime")?;
    let txs = block.transactions.iter()
        // Same rule as matchMints, as a text search: a transaction touching our mints names them in its token balances.
        .filter(|raw| MINTS.iter().any(|mint| raw.get().contains(mint)))
        .map(|raw| serde_json::from_str::<Value>(raw.get()))
        // Failed transactions move no tokens, and RPC omits fields for some (null innerInstructions): skip them unparsed.
        .filter(|tx| tx.as_ref().map_or(true, |tx| tx["meta"]["err"].is_null()))
        // A transaction we can't read fails the whole range (retried, then marked failed), never a silent skip.
        .map(|tx| -> Result<_, BoxError> { Ok(from_rpc(&tx?)?) })
        .collect::<Result<Vec<_>, _>>()?; // the caller adds the slot to the error
    Ok((slot, block_time, txs.iter().flat_map(decode_transfers).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn block(meta: Value) -> Vec<u8> {
        let tx = json!({
            "transaction": { "signatures": ["sig"], "message": { "accountKeys": ["a"], "instructions": [] } },
            "meta": meta,
        });
        json!({ "result": { "blockTime": 1, "transactions": [tx] }, "note": MINTS[0] }).to_string().into_bytes()
    }

    #[test]
    fn skips_failed_transactions_rpc_left_incomplete() {
        // Seen on mainnet: MaxLoadedAccountsDataSizeExceeded comes back with innerInstructions: null.
        let meta = json!({ "err": "MaxLoadedAccountsDataSizeExceeded", "innerInstructions": null,
                           "preTokenBalances": [{ "accountIndex": 0, "mint": MINTS[0] }], "postTokenBalances": [] });
        assert_eq!(decode_block(5, &block(meta)).unwrap().2, vec![]);
    }

    #[test]
    fn still_rejects_successful_transactions_missing_inner_instructions() {
        let meta = json!({ "err": null, "innerInstructions": null,
                           "preTokenBalances": [{ "accountIndex": 0, "mint": MINTS[0] }], "postTokenBalances": [] });
        assert!(decode_block(5, &block(meta)).unwrap_err().to_string().contains("innerInstructions"));
    }
}
