//! Backfill over Helius RPC getBlocks/getBlock. Same ranges table and decoder as the other paths.
//!   backfill_rpc plan <fromSlot> <toSlot> [rangeSize]
//!   backfill_rpc work
use samples_rust::{db, decode::{MINTS, decode_transfers}, sources::from_rpc};
use serde_json::{Value, json};
use std::{env, error::Error, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{sync::Mutex, time::{Instant, sleep, sleep_until}};

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
    async fn call(&self, method: &str, params: Value) -> Result<Value, Box<dyn Error + Send + Sync>> {
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
            let mut body: Value = res.json().await?;
            if let Some(error) = body.get("error") {
                return Err(format!("{method}: {error}").into());
            }
            return Ok(body["result"].take());
        }
        unreachable!()
    }
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
                // HTTP/2 multiplexes every worker over one connection and caps throughput (78 vs 104 slots/s measured).
                http: reqwest::Client::builder().http1_only().pool_max_idle_per_host(256).build()?,
                url: format!("https://mainnet.helius-rpc.com/?api-key={}", env::var("HELIUS_API_KEY")?),
                next_start: Mutex::new(Instant::now()),
                spacing: Duration::from_secs(1) / env::var("BACKFILL_MAX_RPS").map_or(Ok(200), |r| r.parse())?, // Business plan limit
            });
            let workers: usize = env::var("BACKFILL_WORKERS").map_or(Ok(32), |w| w.parse())?;
            let handles: Vec<_> = (0..workers).map(|_| tokio::spawn(worker(rpc.clone()))).collect();
            for handle in handles {
                handle.await?.map_err(|e| e.to_string())?;
            }
        }
        _ => return Err("usage: backfill_rpc plan <fromSlot> <toSlot> [rangeSize] | backfill_rpc work".into()),
    }
    Ok(())
}

async fn worker(rpc: Arc<Rpc>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut db = db::connect().await.map_err(|e| e.to_string())?;
    while let Some(row) = db.query_opt(db::CLAIM_RANGE, &[]).await? {
        let (start, end): (i64, i64) = (row.get(0), row.get(1));
        match backfill_range(&rpc, &mut db, start, end).await {
            // Done only after every block is written. A crash before this just redoes the range: inserts are idempotent.
            Ok(()) => db.execute("UPDATE backfill_ranges SET status = 'done', updated_at = now() WHERE start_slot = $1", &[&start]).await.map(drop)?,
            Err(e) => {
                eprintln!("range {start} failed: {e}");
                db::release_failed_range(&db, start, MAX_ATTEMPTS).await?;
            }
        }
    }
    Ok(())
}

// Same rule as matchMints on the live stream, read straight from the JSON.
fn touches_mints(tx: &Value) -> bool {
    ["preTokenBalances", "postTokenBalances"].iter()
        .flat_map(|k| tx["meta"][k].as_array().into_iter().flatten())
        .any(|b| b["mint"].as_str().is_some_and(|m| MINTS.contains(&m)))
}

async fn backfill_range(rpc: &Rpc, db: &mut tokio_postgres::Client, start: i64, end: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
    // getBlocks lists only produced slots, so skipped slots are never requested.
    let slots = rpc.call("getBlocks", json!([start, end, { "commitment": "finalized" }])).await?;
    for slot in slots.as_array().ok_or("getBlocks: not an array")?.iter().filter_map(Value::as_u64) {
        let block = rpc.call("getBlock", json!([slot, {
            "commitment": "finalized",
            "encoding": "json",
            "transactionDetails": "full",
            "maxSupportedTransactionVersion": 1, // v1 (SIMD-0385) transactions are common; 0 makes the RPC reject the block
            "rewards": false,
        }])).await?;
        let block_time = block["blockTime"].as_i64().ok_or("block without blockTime")?;
        let transfers: Vec<_> = block["transactions"].as_array().into_iter().flatten()
            .filter(|tx| touches_mints(tx)) // check before the base58 work: most transactions aren't ours
            .filter_map(from_rpc)
            .flat_map(|tx| decode_transfers(&tx))
            .collect();

        let tx = db.transaction().await?;
        db::insert_transfers(&tx, slot, block_time, &transfers).await?;
        tx.execute("UPDATE backfill_ranges SET updated_at = now() WHERE start_slot = $1", &[&start]).await?; // heartbeat
        tx.commit().await?;
    }
    Ok(())
}
