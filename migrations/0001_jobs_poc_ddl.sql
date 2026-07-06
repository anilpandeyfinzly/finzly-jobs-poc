-- finzly-jobs-poc — Scheduler Service schema (timing-only).
--
-- The Scheduler owns just two things:
--   scheduled_trigger -- a cron schedule that fires a target (flow/job) per tenant
--   outbox            -- reliably publish the "due" event (no lost fire on crash)
--
-- Everything downstream (flow/job definitions, execution tracking, concurrency,
-- workers) belongs to the separate Orchestrator service and is NOT modelled here.
--
-- Timestamps are TIMESTAMP (no time zone); the trigger's own `timezone` column
-- drives cron evaluation. Fully schema-qualified so it applies regardless of
-- search_path. NOTE: the base schema (demo_galaxy_jobs) must exist before boot —
-- phoenix-postgres-sdk sets it as the connection search_path for the migration
-- ledger. Provision it out-of-band: CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;
CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;

-- --- scheduled_trigger: the master schedule table (common DB, multi-tenant). ---
-- References the target to fire by NAME (target_ref) — opaque to the Scheduler; the
-- Orchestrator resolves it. Cron can't be evaluated in SQL, so next_fire_time
-- (computed in Rust, in `timezone`) drives the claim query
-- `next_fire_time <= now() ... FOR UPDATE SKIP LOCKED`.
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.scheduled_trigger (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_name      VARCHAR(64)  NOT NULL,
    target_ref       VARCHAR(128) NOT NULL,                       -- flow/job name to fire
    target_type      VARCHAR(8)   NOT NULL DEFAULT 'FLOW',        -- FLOW | JOB
    cron_expression  VARCHAR(64)  NOT NULL,
    timezone         VARCHAR(64)  NOT NULL DEFAULT 'UTC',
    enabled          BOOLEAN      NOT NULL DEFAULT true,
    misfire_policy   VARCHAR(16)  NOT NULL DEFAULT 'SKIP',        -- SKIP | FIRE_ONCE
    skip_on_holiday  BOOLEAN      NOT NULL DEFAULT false,
    next_fire_time   TIMESTAMP,
    last_fire_time   TIMESTAMP,
    created_by       VARCHAR(128),
    created_at       TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT now(),
    -- One schedule per (tenant, target); lets registration upsert on re-upload.
    UNIQUE (tenant_name, target_ref)
);

CREATE INDEX IF NOT EXISTS idx_scheduled_trigger_next_fire_time
    ON demo_galaxy_jobs.scheduled_trigger (next_fire_time);

-- --- outbox: transactional outbox for the due event. ---
-- The claim loop inserts a row in the same tx that advances next_fire_time; a
-- publisher loop drains unpublished rows to Kafka, so a crash between claim and
-- publish never loses a fire (at-least-once; the Orchestrator dedupes on the
-- idempotency key in the payload).
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.outbox (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    topic         VARCHAR(255) NOT NULL,
    msg_key       VARCHAR(255) NOT NULL,
    tenant_name   VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT now(),
    published_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON demo_galaxy_jobs.outbox (created_at) WHERE published_at IS NULL;
