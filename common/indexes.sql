    -- Apply after the initial backfill. Keep the primary key in schema.sql for deduplication.
    -- Created on the parent, these indexes apply to existing and future partitions.
    CREATE INDEX IF NOT EXISTS transfers_mint_time ON transfers (mint, block_time);                    -- token activity over a time range
    CREATE INDEX IF NOT EXISTS transfers_source_owner_time ON transfers (source_owner, block_time);    -- wallet history: sent
    CREATE INDEX IF NOT EXISTS transfers_dest_owner_time ON transfers (destination_owner, block_time);  -- wallet history: received
