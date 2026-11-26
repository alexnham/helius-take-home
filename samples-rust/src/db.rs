use crate::decode::Transfer;
use tokio_postgres::{Client, Error, NoTls, Transaction};

pub const CLAIM_RANGE: &str = include_str!("../claim_range.sql");

pub async fn connect() -> Result<Client, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(connection);
    Ok(client)
}

/// Shared by live and backfill: the same transfer gets the same key on every path.
/// One statement per slot: columns go in as arrays and unnest back into rows.
pub async fn insert_transfers(tx: &Transaction<'_>, slot: u64, block_time: i64, transfers: &[Transfer]) -> Result<(), Error> {
    if transfers.is_empty() {
        return Ok(());
    }
    let col = |f: fn(&Transfer) -> String| transfers.iter().map(f).collect::<Vec<_>>();
    let ints = |f: fn(&Transfer) -> i32| transfers.iter().map(f).collect::<Vec<_>>();
    let opt = |f: fn(&Transfer) -> Option<String>| transfers.iter().map(f).collect::<Vec<_>>();
    tx.execute(
        "INSERT INTO transfers (signature, ix_index, inner_ix_index, slot, block_time, mint, source, destination,
                                source_owner, destination_owner, amount)
         SELECT s, i, ii, $4, to_timestamp($5), m, src, dst, so, dow, a::numeric
         FROM unnest($1::text[], $2::int[], $3::int[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], $11::text[])
              AS t(s, i, ii, m, src, dst, so, dow, a)
         ON CONFLICT DO NOTHING", // replays and live/backfill overlap re-deliver slots; the primary key makes that harmless
        &[&col(|t| t.signature.clone()), &ints(|t| t.ix_index), &ints(|t| t.inner_ix_index), &(slot as i64),
          &(block_time as f64), &col(|t| t.mint.clone()), &col(|t| t.source.clone()), &col(|t| t.destination.clone()),
          &opt(|t| t.source_owner.clone()), &opt(|t| t.destination_owner.clone()), &col(|t| t.amount.to_string())],
    )
    .await
    .map(drop)
}

/// Write a slot's transfers and advance the checkpoint in one transaction:
/// a crash can never record progress for rows that weren't stored.
pub async fn commit_slot(db: &mut Client, stream_id: &str, slot: u64, block_time: i64, transfers: &[Transfer]) -> Result<(), Error> {
    let tx = db.transaction().await?;
    insert_transfers(&tx, slot, block_time, transfers).await?;
    tx.execute(
        "INSERT INTO stream_checkpoint (id, slot) VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET slot = GREATEST(stream_checkpoint.slot, EXCLUDED.slot), updated_at = now()",
        &[&stream_id, &(slot as i64)],
    )
    .await?;
    tx.commit().await
}

/// Back to pending for a retry, or failed after too many attempts.
pub async fn release_failed_range(db: &Client, start: i64, max_attempts: i32) -> Result<(), Error> {
    db.execute(
        "UPDATE backfill_ranges SET status = CASE WHEN attempts >= $2 THEN 'failed' ELSE 'pending' END, updated_at = now()
         WHERE start_slot = $1",
        &[&start, &max_attempts],
    )
    .await
    .map(drop)
}

pub async fn plan_ranges(db: &Client, from: i64, to: i64, size: i64) -> Result<(), Error> {
    for start in (from..=to).step_by(size as usize) {
        db.execute(
            "INSERT INTO backfill_ranges (start_slot, end_slot) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[&start, &(start + size - 1).min(to)],
        )
        .await?;
    }
    Ok(())
}
