CREATE TABLE IF NOT EXISTS transfers (
  signature         TEXT NOT NULL,
  ix_index          INT NOT NULL,
  inner_ix_index    INT NOT NULL,            -- -1 for outer instructions (NULLs would break the unique key)
  slot              BIGINT NOT NULL,
  block_time        TIMESTAMPTZ NOT NULL,
  mint              TEXT NOT NULL,
  source            TEXT NOT NULL,           -- token accounts, not wallets
  destination       TEXT NOT NULL,
  source_owner      TEXT,                    -- wallets, when token balance metadata has them
  destination_owner TEXT,
  amount            NUMERIC(20, 0) NOT NULL, -- raw u64 base units; exceeds JS number range
  -- Partitioned tables need the partition column in the key. Safe: block_time is fixed by the slot,
  -- so a transfer gets the same key on every ingestion path.
  PRIMARY KEY (signature, ix_index, inner_ix_index, block_time)
) PARTITION BY RANGE (block_time);

-- Catches rows for days without a partition instead of failing the insert; should stay empty.
CREATE TABLE IF NOT EXISTS transfers_default PARTITION OF transfers DEFAULT;

-- One partition per UTC day. Run daily to stay ahead of the stream.
-- shortcut: hand-rolled partition upkeep; use pg_partman or TimescaleDB in production.
CREATE OR REPLACE FUNCTION ensure_transfer_partitions(first_day DATE, last_day DATE) RETURNS void AS $$
DECLARE d DATE;
BEGIN
  FOR d IN SELECT generate_series(first_day, last_day, interval '1 day')::date LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I PARTITION OF transfers FOR VALUES FROM (%L) TO (%L)',
      'transfers_' || to_char(d, 'YYYYMMDD'), d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
  END LOOP;
END $$ LANGUAGE plpgsql;

-- Retention: dropping a day is instant, unlike DELETE. Queries still filter block_time >= now() - 30 days,
-- so the window is exact even if this runs late.
CREATE OR REPLACE FUNCTION drop_expired_transfer_partitions(keep_days INT DEFAULT 30) RETURNS void AS $$
DECLARE p TEXT;
BEGIN
  FOR p IN SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
           WHERE i.inhparent = 'transfers'::regclass AND c.relname ~ '^transfers_\d{8}$'
             AND to_date(substr(c.relname, 11), 'YYYYMMDD') < (now() AT TIME ZONE 'UTC')::date - keep_days LOOP
    EXECUTE format('DROP TABLE %I', p);
  END LOOP;
END $$ LANGUAGE plpgsql;

SELECT ensure_transfer_partitions((now() AT TIME ZONE 'UTC')::date - 31, (now() AT TIME ZONE 'UTC')::date + 7);

CREATE TABLE IF NOT EXISTS stream_checkpoint (
  id         TEXT PRIMARY KEY,
  slot       BIGINT NOT NULL,                -- last slot fully written
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS backfill_ranges (
  start_slot BIGINT PRIMARY KEY,
  end_slot   BIGINT NOT NULL,                -- inclusive
  status     TEXT NOT NULL DEFAULT 'pending', -- pending | running | done | failed
  attempts   INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
