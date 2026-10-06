# finzly-jobs-poc — Design

Proof of concept for the new **finzly-job-service**: a multi-tenant, cron-driven,
Kafka-orchestrated job/flow scheduler. This document is the reference for the data
model, the message topics, and the end-to-end flows.

> **Scope update (2026):** the system was split into a **Scheduler**, a separate
> **Orchestrator** (assignment/flow execution/concurrency), and **Worker = Host Service
> + Client SDK**. **This POC is the Scheduler Service** — the **control plane**: it owns
> the catalog (project / job / flow definitions) + schedules, serves the UI read APIs
> (`GET /projects /jobs /flows /schedules /history/{flows,jobs}`), polls due triggers
> every 5s, and publishes a `finzly.jobs.trigger.due` event (via a transactional outbox).
> Execution **history** rows are written by the Orchestrator and only *read* here.
> **Dispatch, concurrency (SKIP/QUEUE/PARALLEL), the worker, and the SDK are the
> Orchestrator** (separate service, not in this POC). See [ARCHITECTURE.md](ARCHITECTURE.md)
> and [LOW-LEVEL-DESIGN.md](LOW-LEVEL-DESIGN.md).

---

## 1. Components

| Component | Role |
|---|---|
| **galaxy-scheduler** | Owns definitions (project/job/flow) and the master `scheduled_trigger` table. Polls for due triggers and publishes `flow.execution.requested`. |
| **galaxy-job-orchestrator** | Consumes flow/job events. Opens `flow_execution`, walks the flow DAG, applies concurrency policy, dispatches jobs, and closes flows on completion. |
| **workflow-processor-service** | Loads the job's bean by class and runs it, then publishes `job.execution.completed`. (In this POC, simulated by the `worker` module.) |
| **JOB SDK** | Embedded in each processor; heartbeats running jobs to Redis for crash detection. *(designed, not built)* |

In this single-binary POC all roles run in one process, decoupled by Kafka topics —
so the same code can later be split across pods/services unchanged.

---

## 2. Data model

Schema `demo_galaxy_jobs` (migration `0001_jobs_poc_ddl.sql`). All timestamps are
`TIMESTAMP` (no time zone); structured columns are `JSONB`; keys/FKs are `UUID`.

```
project ──< job_definition
flow_definition ──< scheduled_trigger   (references flow by NAME, master DB)
flow_definition ──< flow_execution ──< job_execution
execution_lock ── guards (tenant, service_context, job_name)
```

| Table | Purpose | Key columns |
|---|---|---|
| `project` | Owner of a service's jobs | `id`, `name` (unique) |
| `job_definition` | Reusable unit of work | `project_id→project`, `name`, `job_type`, `service_context`, `parameters` JSONB, `retry_policy` JSONB; unique `(project_id, name)` |
| `flow_definition` | Versioned, toggleable DAG of jobs | `name`, `version`, `is_enabled`, `flow` JSONB, `skip_on_holiday`, `sla_policy` JSONB; unique `(name, version)` |
| `scheduled_trigger` | Cron schedule (per tenant) | `tenant_name`, `flow_definition_name`, `cron_expression`, `next_fire_time`, `last_fire_time`; unique `(tenant_name, flow_definition_name)` |
| `flow_execution` | One run of a flow | `flow_definition_id→flow_definition`, `flow_version`, `idempotency_key` (unique), `status` |
| `job_execution` | One run of a job in a flow | `flow_execution_id→flow_execution`, `job_name`, `status`, `output`, `attempt` |
| `execution_lock` | "Job already running?" guard | unique `(tenant_name, service_context, job_name)` |

**Statuses**: `SCHEDULED | QUEUED | RUNNING | SUCCESS | FAILED | TIMED_OUT | SKIPPED`.

**Why `scheduled_trigger` references by name, not UUID FK**: it lives in the finzly
**master DB** (common across tenants); the flow definitions live per-tenant, so a
cross-DB FK isn't possible — the orchestrator resolves name → enabled latest version.

### `flow` JSON

POC shape (single-job flow): `{"jobs": ["daily-settlement"]}`. The target design is a
DAG: `tasks[]` each with `taskId, name, jobType, targetService, concurrencyPolicy,
retryPolicy{retryCount,delay}, parameters, next[]`.

---

## 3. Kafka topics

| Topic (config key) | Direction | Payload |
|---|---|---|
| `finzly.jobs.flow.execution.requested` | scheduler → orchestrator | `{flowDefinitionId, tenantName, scheduleFireTime}` |
| `finzly.jobs.job.execution.requested` | orchestrator → worker | `{flowExecutionId, jobExecutionId, tenantName, jobName, serviceContext, attempt}` |
| `finzly.jobs.job.execution.completed` | worker → orchestrator | `{…, status, output, errorMessage}` |

Ordering: QUEUE-policy jobs are published **ordered** (SDK routes by
`hash(key) % partitions`, key = job name) → FIFO per job. Others are unordered.

---

## 4. Flows

### 4.1 Create & schedule (REST, galaxy-scheduler)
1. `POST /projects/{p}/jobs` → insert `job_definition`.
2. `POST /flows` → validate job defs (cycle / edge / endpoint checks) → insert
   `flow_definition` with the `flow` DAG. A single scheduled job auto-gets a wrapper flow.
3. `POST /flows/{id}/schedules` → insert `scheduled_trigger` (cron + tenant).

> POC entry point: `POST /job/registrations` uploads `job-config.yml`, which maps
> service→`project`, each job→`job_definition` + single-job `flow_definition`, and
> cron jobs→`scheduled_trigger`. Manual (no-cron) jobs get no trigger.

### 4.2 Claim (galaxy-scheduler, polls every 10–15s)
In one transaction: `SELECT … FOR UPDATE OF st SKIP LOCKED LIMIT 50` due triggers,
`UPDATE next_fire_time`/`last_fire_time`, and publish `flow.execution.requested`.
`SKIP LOCKED` guarantees each fire is claimed by exactly one pod.

### 4.3 Flow dispatch (orchestrator ← `flow.execution.requested`)
- Insert `flow_execution` `ON CONFLICT (idempotency_key) DO NOTHING` (dedupes
  re-delivery); status `RUNNING`.
- For each job, apply **concurrency policy**:
  - **SKIP** — acquire `execution_lock` (`ON CONFLICT DO NOTHING`); if already held,
    mark the job `SKIPPED` and don't dispatch.
  - **QUEUE** — insert `QUEUED`, publish ordered (FIFO per job name).
  - **PARALLEL** — insert `RUNNING`, publish immediately.

### 4.4 Execute (worker ← `job.execution.requested`)
Load the job's bean and run it; publish `job.execution.completed` with SUCCESS/FAILED
+ output. *(POC: log-only, always SUCCESS.)*

### 4.5 Completion (orchestrator ← `job.execution.completed`)
Update `job_execution`; delete its `execution_lock`; if no jobs of the flow remain
`QUEUED`/`RUNNING`, close the `flow_execution` (FAILED if any job failed, else SUCCESS).

### 4.6 Crash detection *(designed, not built)*
JOB SDK sets Redis `heartbeat-<jobExecId>:RUNNING` (TTL 120s), beating every 60s. A
scheduler sweep (every 60s) over RUNNING jobs marks any whose Redis key is gone as
`FAILED` (job + flow), and notifies the UI.

### 4.7 Resume *(designed, not built)*
`POST /api/flows/{flowId}/executions/{executionId}/resume`: concurrency-check (no other
active execution), find FAILED job execs, insert new `job_execution` with `attempt+1`,
flip `flow_execution` FAILED→RUNNING, publish `job.execution.requested`.

---

## 5. Concurrency & correctness invariants

- **Exactly-once claim** across pods — `FOR UPDATE … SKIP LOCKED` on `scheduled_trigger`.
- **Idempotent fires** — `flow_execution.idempotency_key` = `flowDefinitionId:scheduleFireTime`, unique index.
- **No double-run** — `execution_lock` unique `(tenant, service_context, job_name)` acquired via `ON CONFLICT DO NOTHING`.
- **Versioning** — `flow_execution` pins `flow_version`; trigger resolves to the enabled latest version.

---

## 6. Roadmap

- [ ] Real job invocation (HTTP callback for API jobs; the worker currently only logs).
- [ ] Rich `tasks[]` DAG with `next[]` sequencing and per-task concurrency (POC uses a flat job list, all SKIP).
- [ ] Redis heartbeat + crash-detection sweep (§4.6).
- [ ] Resume endpoint (§4.7).
- [ ] Delayed delivery for future-dated fires (Kafka SDK has no native delay).

> **Note:** the base schema (`demo_galaxy_jobs`) must exist before boot — the SDK
> sets it as the connection `search_path` and creates the migration ledger there.
> Provision it out-of-band (`CREATE SCHEMA IF NOT EXISTS demo_galaxy_jobs;`).
