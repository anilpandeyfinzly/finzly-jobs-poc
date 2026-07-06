-- finzly-jobs-poc schema: flow-oriented job model.
--
--   project           -- owns a set of job definitions
--   job_definition    -- a reusable unit of work belonging to a project
--   flow_definition   -- a versioned, enabled/disabled flow (DAG of jobs) with SLA policy
--   scheduled_trigger -- cron schedule that fires a flow_definition (per tenant)
--   flow_execution    -- one run of a flow_definition (idempotent per fire)
--   job_execution     -- one run of a job within a flow_execution
--   execution_lock    -- "job already running?" guard for concurrency policies
--
-- Timestamps use TIMESTAMP (without time zone) and structured columns use JSONB.
-- Fully schema-qualified so it applies regardless of search_path. Idempotent so
-- re-running is safe. NOTE: the base schema is set as the connection search_path
-- by phoenix-postgres-sdk and must already exist for the migration ledger.
CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;

-- --- project: top-level owner of job definitions. ---
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.project (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(128) NOT NULL UNIQUE,
    created_by  VARCHAR(128),
    created_at  TIMESTAMP  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP  NOT NULL DEFAULT now()
);

-- --- job_definition: a reusable unit of work belonging to a project. ---
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.job_definition (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID         NOT NULL REFERENCES demo_galaxy_jobs.project(id),
    name            VARCHAR(128) NOT NULL,
    description     VARCHAR(512),
    job_type        VARCHAR(64)  NOT NULL,
    service_context VARCHAR(128) NOT NULL,
    parameters      JSONB,
    retry_policy    JSONB,
    created_at      TIMESTAMP  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP  NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX IF NOT EXISTS idx_job_definition_project_id
    ON demo_galaxy_jobs.job_definition (project_id);

-- --- flow_definition: a versioned, toggleable flow (DAG of jobs) with SLA policy. ---
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.flow_definition (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name             VARCHAR(128) NOT NULL,
    description      VARCHAR(512),
    is_enabled       BOOLEAN      NOT NULL DEFAULT true,
    version          INTEGER      NOT NULL DEFAULT 1,
    flow             JSONB        NOT NULL,
    skip_on_holiday  BOOLEAN      NOT NULL DEFAULT false,
    sla_policy       JSONB,
    created_by       VARCHAR(128),
    created_at       TIMESTAMP  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP  NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

-- --- scheduled_trigger: cron schedule that fires a flow_definition, per tenant. ---
-- This is the master scheduler table: it lives in the finzly (common) DB shared
-- across tenants, so it references the flow by NAME (flow_definition_name), not a
-- cross-DB UUID FK. Cron can't be evaluated in SQL, so next_fire_time (computed in
-- Rust) drives the claim query `next_fire_time <= now() ... FOR UPDATE SKIP LOCKED`.
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.scheduled_trigger (
    id                    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_name           VARCHAR(64)  NOT NULL,
    flow_definition_name  VARCHAR(128) NOT NULL,
    cron_expression       VARCHAR(64)  NOT NULL,
    next_fire_time        TIMESTAMP,
    last_fire_time        TIMESTAMP,
    created_by            VARCHAR(128),
    created_at            TIMESTAMP  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP  NOT NULL DEFAULT now(),
    -- One trigger per (tenant, flow); lets registration upsert on re-upload.
    UNIQUE (tenant_name, flow_definition_name)
);

CREATE INDEX IF NOT EXISTS idx_scheduled_trigger_next_fire_time
    ON demo_galaxy_jobs.scheduled_trigger (next_fire_time);

-- --- flow_execution: one run of a flow_definition. ---
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.flow_execution (
    id                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_definition_id   UUID         NOT NULL REFERENCES demo_galaxy_jobs.flow_definition(id),
    flow_version         INTEGER      NOT NULL,
    idempotency_key      VARCHAR(255),
    status               VARCHAR(24)  NOT NULL DEFAULT 'SCHEDULED', -- SCHEDULED|RUNNING|SUCCESS|FAILED|TIMED_OUT
    started_at           TIMESTAMP,
    ended_at             TIMESTAMP,
    error_message        TEXT,
    created_at           TIMESTAMP  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP  NOT NULL DEFAULT now()
);

-- Idempotency: a given fire is recorded at most once.
CREATE UNIQUE INDEX IF NOT EXISTS uq_flow_execution_idempotency_key
    ON demo_galaxy_jobs.flow_execution (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_flow_execution_flow_definition_id
    ON demo_galaxy_jobs.flow_execution (flow_definition_id);
CREATE INDEX IF NOT EXISTS idx_flow_execution_status
    ON demo_galaxy_jobs.flow_execution (status);

-- --- job_execution: one run of a job within a flow_execution. ---
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.job_execution (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_execution_id  UUID         NOT NULL REFERENCES demo_galaxy_jobs.flow_execution(id),
    job_name           VARCHAR(128) NOT NULL,
    status             VARCHAR(24)  NOT NULL DEFAULT 'SCHEDULED', -- SCHEDULED|QUEUED|RUNNING|SUCCESS|FAILED|TIMED_OUT|SKIPPED
    output             VARCHAR(2048),
    attempt            INTEGER      NOT NULL DEFAULT 0,
    started_at         TIMESTAMP,
    ended_at           TIMESTAMP,
    error_message      TEXT,
    created_at         TIMESTAMP  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_execution_flow_execution_id
    ON demo_galaxy_jobs.job_execution (flow_execution_id);
CREATE INDEX IF NOT EXISTS idx_job_execution_status
    ON demo_galaxy_jobs.job_execution (status);

-- --- execution_lock: "is this job already running?" guard for SKIP/QUEUE policies. ---
-- A row exists while a (tenant, service, job) is in flight. Insert with
-- ON CONFLICT (tenant_name, service_context, job_name) DO NOTHING to make the
-- "acquire" race-safe across pods; the row is deleted on job completion.
CREATE TABLE IF NOT EXISTS demo_galaxy_jobs.execution_lock (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_name        VARCHAR(64)  NOT NULL,
    service_context    VARCHAR(128) NOT NULL,
    job_name           VARCHAR(128) NOT NULL,
    flow_execution_id  UUID         NOT NULL REFERENCES demo_galaxy_jobs.flow_execution(id),
    job_execution_id   UUID         NOT NULL REFERENCES demo_galaxy_jobs.job_execution(id),
    locked_at          TIMESTAMP  NOT NULL DEFAULT now(),
    UNIQUE (tenant_name, service_context, job_name)
);
