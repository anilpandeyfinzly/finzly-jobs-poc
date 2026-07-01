-- Cron-driven scheduling.
--
-- Jobs are selected for firing by their cron schedule, not by interval_seconds
-- (which is the status-report cadence — how often the processor reports status,
-- NOT when the job runs). Cron can't be evaluated in SQL, so we store the next
-- fire time (computed in Rust from cron) and the claim query selects rows where
-- next_fire_time <= now() ... FOR UPDATE SKIP LOCKED.

ALTER TABLE job_registration
    ADD COLUMN IF NOT EXISTS next_fire_time TIMESTAMPTZ;

-- Index supports the claim query's `next_fire_time <= now() ORDER BY next_fire_time`.
CREATE INDEX IF NOT EXISTS idx_job_registration_next_fire_time
    ON job_registration (next_fire_time);

-- Make already-registered recurring jobs eligible; the scheduler advances them
-- to their proper cron time on first claim.
UPDATE job_registration
    SET next_fire_time = now()
    WHERE cron IS NOT NULL AND next_fire_time IS NULL;
