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
  PRIMARY KEY (signature, ix_index, inner_ix_index)
);
CREATE INDEX IF NOT EXISTS transfers_mint_time ON transfers (mint, block_time);

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
