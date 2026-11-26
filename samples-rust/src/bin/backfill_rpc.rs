//! Backfill over Helius RPC getBlocks/getBlock. Same ranges table and decoder as the other paths.
//!   backfill_rpc plan <fromSlot> <toSlot> [rangeSize]
//!   backfill_rpc work
use futures_util::{StreamExt, TryStreamExt, stream};
use samples_rust::{db, decode::{MINTS, Transfer, decode_transfers}, sources::from_rpc};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use std::{env, error::Error, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{sync::Mutex, time::{Instant, sleep, sleep_until}};

type BoxError = Box<dyn Error + Send + Sync>;

const MAX_ATTEMPTS: i32 = 5;

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

            let res = self.http.post(&self.url)
                .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
                .send().await?;
            let status = res.status().as_u16();
            if (status == 429 || status >= 500) && attempt < 6 {
                let jitter = 0.5 + (SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos() % 1000) as f64 / 1000.0;
                sleep(Duration::from_millis(250).mul_f64(2f64.powi(attempt) * jitter)).await; // backoff with jitter
                continue;
            }
            return Ok(res.bytes().await?.to_vec());
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
            let handles: Vec<_> = (0..workers).map(|_| tokio::spawn(worker(rpc.clone(), fetches))).collect();
            for handle in handles {
                handle.await?.map_err(|e| e.to_string())?;
            }
        }
        _ => return Err("usage: backfill_rpc plan <fromSlot> <toSlot> [rangeSize] | backfill_rpc work".into()),
    }
    Ok(())
}

async fn worker(rpc: Arc<Rpc>, fetches: usize) -> Result<(), BoxError> {
    let mut db = db::connect().await.map_err(|e| e.to_string())?;
    while let Some(row) = db.query_opt(db::CLAIM_RANGE, &[]).await? {
        let (start, end): (i64, i64) = (row.get(0), row.get(1));
        if let Err(e) = backfill_range(&rpc, &mut db, start, end, fetches).await {
            eprintln!("range {start} failed: {e}");
            db::release_failed_range(&db, start, MAX_ATTEMPTS).await?;
        }
    }
    Ok(())
}

async fn backfill_range(rpc: &Arc<Rpc>, db: &mut tokio_postgres::Client, start: i64, end: i64, fetches: usize) -> Result<(), BoxError> {
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
                tokio::task::spawn_blocking(move || decode_block(slot, &body)).await?
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
    Ok(())
}

fn decode_block(slot: u64, body: &[u8]) -> Result<(u64, i64, Vec<Transfer>), BoxError> {
    let block = serde_json::from_slice::<Response<Block>>(body)?.into_result("getBlock")?;
    let block_time = block.block_time.ok_or("block without blockTime")?;
    let transfers = block.transactions.iter()
        // Same rule as matchMints, as a text search: a transaction touching our mints names them in its token balances.
        .filter(|raw| MINTS.iter().any(|mint| raw.get().contains(mint)))
        .filter_map(|raw| serde_json::from_str::<Value>(raw.get()).ok())
        .filter_map(|tx| from_rpc(&tx))
        .flat_map(|tx| decode_transfers(&tx))
        .collect();
    Ok((slot, block_time, transfers))
}
