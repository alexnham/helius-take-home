-- Claim the newest pending backfill range for this worker.
-- FOR UPDATE SKIP LOCKED: concurrent workers skip rows another worker is claiming, so no range is taken twice.
UPDATE backfill_ranges
SET status = 'running', attempts = attempts + 1, updated_at = now()
WHERE start_slot = (
  SELECT start_slot FROM backfill_ranges
  WHERE status = 'pending'
  ORDER BY start_slot DESC          -- newest first: recent days are usually queried most
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING start_slot, end_slot;
