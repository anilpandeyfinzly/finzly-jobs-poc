-- finzly-jobs — Scheduler Service schema (control plane).
--
-- The Scheduler owns the CATALOG + SCHEDULES, serves the UI read APIs (project /
-- job / flow / schedule / history), polls due triggers, and publishes a
-- `finzly.jobs.trigger.due` event via the outbox. It does NOT execute anything.
--
--   Catalog (Scheduler writes):
--     project           -- owns a set of job definitions
--     job_definition    -- a reusable unit of work belonging to a project
--     flow_definition   -- a versioned flow (DAG of jobs) with SLA policy
--     scheduled_trigger -- cron schedule that fires a target (flow), per tenant
--   Reliability (Scheduler writes):
--     outbox            -- transactional outbox for the due event
--   Execution history (Orchestrator writes, Scheduler's history API reads):
--     flow_execution    -- one run of a flow
--     job_execution     -- one run of a job within a flow run
--
-- Concurrency/dispatch state (execution_lock, etc.) is Orchestrator-internal and
-- NOT modelled here. Timestamps are TIMESTAMP (no tz); the trigger's own `timezone`
-- drives cron evaluation. Fully schema-qualified. NOTE: the base schema must exist
-- before boot (phoenix-postgres-sdk sets it as the migration-ledger search_path):
-- CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;
CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;

-- ===== Catalog =====

CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.project (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(128) NOT NULL UNIQUE,
    created_by  VARCHAR(128),
    created_at  TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.job_definition (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID         NOT NULL REFERENCES demo_galaxy_jobs.project(id),
    name            VARCHAR(128) NOT NULL,
    description     VARCHAR(512),
    job_type        VARCHAR(64)  NOT NULL,               -- API | EVENT
    service_context VARCHAR(128) NOT NULL,
    parameters      JSONB,
    retry_policy    JSONB,
    created_at      TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);
CREATE INDEX IF NOT EXISTS idx_job_definition_project_id
    ON demo_galaxy_jobs.job_definition (project_id);

CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.flow_definition (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name         VARCHAR(128) NOT NULL,
    description  VARCHAR(512),
    is_enabled   BOOLEAN      NOT NULL DEFAULT true,
    version      INTEGER      NOT NULL DEFAULT 1,
    flow         JSONB        NOT NULL,                  -- DAG of jobs
    sla_policy   JSONB,
    created_by   VARCHAR(128),
    created_at   TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP    NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

-- Cron schedule (master, multi-tenant). References the target to fire by NAME
-- (target_ref) — the Orchestrator resolves it. next_fire_time (computed in Rust in
-- `timezone`) drives the claim query `next_fire_time <= now() ... FOR UPDATE SKIP LOCKED`.
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.scheduled_trigger (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_name      VARCHAR(64)  NOT NULL,
    target_ref       VARCHAR(128) NOT NULL,              -- flow/job name to fire
    target_type      VARCHAR(8)   NOT NULL DEFAULT 'FLOW',
    cron_expression  VARCHAR(64)  NOT NULL,
    timezone         VARCHAR(64)  NOT NULL DEFAULT 'UTC',
    enabled          BOOLEAN      NOT NULL DEFAULT true,
    misfire_policy   VARCHAR(16)  NOT NULL DEFAULT 'SKIP',
    skip_on_holiday  BOOLEAN      NOT NULL DEFAULT false,
    next_fire_time   TIMESTAMP,
    last_fire_time   TIMESTAMP,
    created_by       VARCHAR(128),
    created_at       TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT now(),
    UNIQUE (tenant_name, target_ref)
);
CREATE INDEX IF NOT EXISTS idx_scheduled_trigger_next_fire_time
    ON demo_galaxy_jobs.scheduled_trigger (next_fire_time);

-- ===== Reliability =====

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

-- ===== Execution history (Orchestrator writes; Scheduler history API reads) =====

CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.flow_execution (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_definition_id  UUID         REFERENCES demo_galaxy_jobs.flow_definition(id),
    flow_name           VARCHAR(128),
    flow_version        INTEGER,
    tenant_name         VARCHAR(64),
    idempotency_key     VARCHAR(255),
    status              VARCHAR(24)  NOT NULL DEFAULT 'SCHEDULED',
    started_at          TIMESTAMP,
    ended_at            TIMESTAMP,
    error_message       TEXT,
    created_at          TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP    NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_flow_execution_idempotency_key
    ON demo_galaxy_jobs.flow_execution (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_flow_execution_status
    ON demo_galaxy_jobs.flow_execution (status);

CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.job_execution (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_execution_id  UUID         NOT NULL REFERENCES demo_galaxy_jobs.flow_execution(id),
    job_name           VARCHAR(128) NOT NULL,
    status             VARCHAR(24)  NOT NULL DEFAULT 'SCHEDULED',
    output             VARCHAR(2048),
    attempt            INTEGER      NOT NULL DEFAULT 0,
    started_at         TIMESTAMP,
    ended_at           TIMESTAMP,
    error_message      TEXT,
    created_at         TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_job_execution_flow_execution_id
    ON demo_galaxy_jobs.job_execution (flow_execution_id);
