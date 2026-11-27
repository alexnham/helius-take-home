//! Backfill from Old Faithful archives via Jetstreamer. Claims the same backfill_ranges rows as
//! backfill_rpc, so either path can fill any range.
mod source;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, OnEntryFn, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData, firehose,
};
use common::{db, decode::{MINTS, Transfer, decode_transfers}, log::log};
use source::from_archive;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    error::Error,
    sync::{Arc, Mutex},
    time::Instant,
};

const MAX_ATTEMPTS: i32 = 5;

#[derive(Default)]
struct Collected {
    transfers: BTreeMap<u64, Vec<Transfer>>,
    block_times: HashMap<u64, i64>,
    skipped: HashSet<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let threads: u64 = env::var("ARCHIVE_THREADS").map_or(Ok(4), |t| t.parse())?;
    let mut db = db::connect().await?;

    while let Some(row) = db.query_opt(db::CLAIM_RANGE, &[]).await? {
        let (start, end): (i64, i64) = (row.get(0), row.get(1));
        log("info", "range_claimed", json!({ "start": start, "end": end }));
        let began = Instant::now();
        let result = match stream_range(start as u64, end as u64, threads).await {
            Ok(collected) => write_range(&mut db, start, collected).await,
            Err(e) => Err(e),
        };
        match result {
            Ok((blocks, skipped, transfers)) => log("info", "range_done", json!({
                "start": start, "end": end, "duration_ms": began.elapsed().as_millis() as u64,
                "blocks": blocks, "skipped": skipped, "transfers": transfers,
            })),
            Err(e) => {
                log("error", "range_failed", json!({ "start": start, "end": end, "error": e.to_string() }));
                db::release_failed_range(&db, start, MAX_ATTEMPTS).await?;
            }
        }
    }
    Ok(())
}

/// Streams one inclusive slot range from the archive and keeps our mints' transfers in memory.
async fn stream_range(start: u64, end: u64, threads: u64) -> Result<Collected, Box<dyn Error>> {
    let state = Arc::new(Mutex::new(Collected::default()));

    let s = state.clone();
    let on_tx = move |_thread: usize, tx: TransactionData| {
        // Cheap check before converting: most transactions in a block don't touch our mints.
        let meta = &tx.transaction_status_meta;
        let balances = meta.pre_token_balances.iter().flatten().chain(meta.post_token_balances.iter().flatten());
        if balances.into_iter().any(|b| MINTS.contains(&b.mint.as_str())) {
            let transfers = decode_transfers(&from_archive(&tx));
            if !transfers.is_empty() {
                s.lock().unwrap().transfers.entry(tx.slot).or_default().extend(transfers);
            }
        }
        async { Ok(()) }.boxed()
    };
    let s = state.clone();
    let on_block = move |_thread: usize, block: BlockData| {
        let mut s = s.lock().unwrap();
        match block.block_time() {
            Some(time) => drop(s.block_times.insert(block.slot(), time)),
            None if block.was_skipped() => drop(s.skipped.insert(block.slot())),
            None => {} // a block without a time stays unaccounted, so the range fails below
        }
        async { Ok(()) }.boxed()
    };

    // Jetstreamer's range is half-open; ours is inclusive.
    firehose(
        threads, false, false, None, start..end + 1,
        Some(on_block), Some(on_tx),
        None::<OnEntryFn>, None::<OnRewardFn>, None::<OnErrorFn>, None::<OnStatsTrackingFn>, None,
    )
    .await
    .map_err(|(e, slot)| format!("firehose failed at slot {slot}: {e}"))?;

    let collected = Arc::try_unwrap(state).map_err(|_| "handlers still hold state")?.into_inner()?;
    // Done means every slot was seen as a block or a skip; anything else is a gap, not coverage.
    if let Some(missing) = (start..=end).find(|s| !collected.block_times.contains_key(s) && !collected.skipped.contains(s)) {
        return Err(format!("slot {missing} missing from archive output").into());
    }
    Ok(collected)
}

/// Inserts the range's transfers and marks it done in one transaction. Returns (blocks, skipped, transfers).
async fn write_range(db: &mut tokio_postgres::Client, start: i64, collected: Collected) -> Result<(usize, usize, usize), Box<dyn Error>> {
    let tx = db.transaction().await?;
    for (slot, transfers) in &collected.transfers {
        db::insert_transfers(&tx, *slot, collected.block_times[slot], transfers).await?;
    }
    tx.execute("UPDATE backfill_ranges SET status = 'done', updated_at = now() WHERE start_slot = $1", &[&start]).await?;
    tx.commit().await?;
    let count: usize = collected.transfers.values().map(Vec::len).sum();
    Ok((collected.block_times.len(), collected.skipped.len(), count))
}
