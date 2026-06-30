-- finzly-jobs-poc starter schema.
-- Models the job-registration contract from cron-job/job-config.yml:
-- a job has a schedule (cron, optional), a delivery channel (trigger_type),
-- a reporting cadence (status_report_mode), and SLA/timeout/retry policy.
-- Idempotent so re-running migrations is safe.
CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;

CREATE TABLE IF NOT EXISTS job_registration (
    id                  BIGSERIAL PRIMARY KEY,
    name                VARCHAR(128) NOT NULL UNIQUE,
    cron                VARCHAR(64),                       -- NULL => manual-only (never auto-fires)
    trigger_type        VARCHAR(16)  NOT NULL,             -- API | EVENT
    callback_bean       VARCHAR(128) NOT NULL,
    callback_endpoint   VARCHAR(255),                      -- required when trigger_type = API
    event_topic         VARCHAR(255),                      -- required when trigger_type = EVENT
    status_report_mode  VARCHAR(16)  NOT NULL,             -- FIXED_TIME | COMPOSITIONAL
    interval_seconds    INTEGER,                           -- used when status_report_mode = FIXED_TIME
    sla_seconds         INTEGER      NOT NULL,
    timeout_seconds     INTEGER      NOT NULL,
    max_retries         INTEGER      NOT NULL DEFAULT 0,
    contract_version    VARCHAR(16)  NOT NULL DEFAULT '1.0',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- One row per launch of a registered job.
CREATE TABLE IF NOT EXISTS job_execution (
    id              BIGSERIAL PRIMARY KEY,
    job_name        VARCHAR(128) NOT NULL REFERENCES job_registration(name),
    execution_id    VARCHAR(128) NOT NULL,
    status          VARCHAR(24)  NOT NULL DEFAULT 'SCHEDULED', -- SCHEDULED|IN_PROGRESS|SUCCESS|FAILED|TIMED_OUT
    attempt         INTEGER      NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    last_report_at  TIMESTAMPTZ,
    detail          TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (job_name, execution_id)
);

CREATE INDEX IF NOT EXISTS idx_job_execution_status   ON job_execution (status);
CREATE INDEX IF NOT EXISTS idx_job_execution_job_name ON job_execution (job_name);
