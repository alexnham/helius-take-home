//! Live ingestion from LaserStream gRPC, resuming from our own checkpoint.
use futures_util::StreamExt;
use helius_laserstream::{
    LaserstreamClient, LaserstreamConfig,
    grpc::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions,
           subscribe_update::UpdateOneof},
    subscribe,
};
use samples_rust::{db, decode::{MINTS, Transfer, decode_transfers}, sources::from_laserstream};
use std::{collections::BTreeMap, env, error::Error};

const STREAM_ID: &str = "laserstream-main";
const REPLAY_MARGIN: u64 = 50; // re-read a few slots on restart; idempotent inserts make the overlap free

#[derive(Default)]
struct Pending {
    block_time: Option<i64>,
    transfers: Vec<Transfer>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // reqwest and the LaserStream SDK enable different TLS backends; pick one explicitly.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = LaserstreamConfig::new(env::var("LASERSTREAM_ENDPOINT")?, env::var("HELIUS_API_KEY")?);
    let mut db = db::connect().await?;

    let request = SubscribeRequest {
        commitment: Some(CommitmentLevel::Finalized as i32), // only finalized slots are guaranteed to be on canonical chain
        from_slot: Some(resume_slot(&db, &config).await?),
        transactions: [("transfers".into(), SubscribeRequestFilterTransactions {
            account_include: MINTS.map(String::from).to_vec(),
            match_mints: true, // also match on pre/post token balance mints: plain Transfer never lists the mint
            vote: Some(false),
            failed: Some(false),
            ..Default::default()
        })].into(),
        // every slot, with block time, including slots with no matching transfers
        blocks_meta: [("every_slot".into(), SubscribeRequestFilterBlocksMeta::default())].into(),
        ..Default::default()
    };

    // Slots buffered until the stream has moved past them and their block time is known.
    let mut pending: BTreeMap<u64, Pending> = BTreeMap::new();
    let mut max_seen = 0;
    let (stream, _handle) = subscribe(config, request); // the SDK reconnects and replays on its own
    tokio::pin!(stream);
    while let Some(update) = stream.next().await {
        match update?.update_oneof {
            Some(UpdateOneof::Transaction(t)) => {
                let tx = t.transaction.as_ref().and_then(from_laserstream);
                pending.entry(t.slot).or_default().transfers.extend(tx.iter().flat_map(decode_transfers));
                max_seen = max_seen.max(t.slot);
            }
            Some(UpdateOneof::BlockMeta(m)) => {
                pending.entry(m.slot).or_default().block_time = m.block_time.map(|t| t.timestamp);
                max_seen = max_seen.max(m.slot);
            }
            _ => continue, // pings, pongs
        }
        // Flush, in slot order, every slot the stream has moved past and that has a block time.
        while let Some(entry) = pending.first_entry() {
            let slot = *entry.key();
            let Some(block_time) = entry.get().block_time.filter(|_| slot < max_seen) else { break }; // never skip ahead
            let slot_data = entry.remove();
            db::commit_slot(&mut db, STREAM_ID, slot, block_time, &slot_data.transfers).await?; // transfers + checkpoint
        }
    }
    Ok(())
}

// Resume from our own checkpoint: the SDK's reconnect position lives in memory and dies with the process.
async fn resume_slot(db: &tokio_postgres::Client, config: &LaserstreamConfig) -> Result<u64, Box<dyn Error>> {
    let laserstream = LaserstreamClient::connect(config.clone()).await?;
    let row = db.query_opt("SELECT slot FROM stream_checkpoint WHERE id = $1", &[&STREAM_ID]).await?;
    // First run: start at the tip; backfill covers the rest of the window.
    let Some(row) = row else { return Ok(laserstream.get_slot(Some(CommitmentLevel::Finalized)).await?.slot) };

    let oldest = laserstream.subscribe_replay_info().await?.first_available.unwrap_or(0);
    let from = (row.get::<_, i64>(0) as u64).saturating_sub(REPLAY_MARGIN);
    if from >= oldest {
        return Ok(from);
    }
    // Outage outlived the replay window: queue the gap for backfill, resume from the oldest replayable slot.
    db.execute(
        "INSERT INTO backfill_ranges (start_slot, end_slot) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &[&(from as i64), &(oldest as i64 - 1)],
    )
    .await?;
    Ok(oldest)
}
