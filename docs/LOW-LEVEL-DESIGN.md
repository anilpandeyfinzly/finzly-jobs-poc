# finzly-jobs — Low-Level Design

End-to-end design of the cron-driven, Kafka-orchestrated job scheduler: crate
boundaries, data model, message contracts, state machines, and every runtime flow
with its failure/edge cases. Companion to the architecture diagram and
[GAP-ANALYSIS.md](GAP-ANALYSIS.md).

---

## 0. Locked decisions & assumptions

| Decision | Choice |
|---|---|
| Deployables | **orchestrator** (binary), **worker** (binary), **finzly-jobs-sdk** (lib), **finzly-jobs-model** (lib, shared contract) |
| Scheduler | **folded into the orchestrator** binary (not a separate service) |
| DAG execution | **centralized in the orchestrator** (not embedded per host) |
| Dispatch topics | **per service_context**: `job-dispatch-<service_context>` |
| Worker runtime | in the SDK; **reuse `phoenix-jobs-sdk`** `Job` trait + `JobRegistry` + `JobStatusService`, add Kafka + heartbeat |
| Delivery | **at-least-once + idempotent consumers**; **transactional outbox** for fire publish |
| Offset commit | manual, **after** successful handling; poison → DLQ |

**Assumptions to confirm (A#):**
- **A1** `scheduled_trigger` is the **master** table in a common DB (one pool, key `bankos.jobs.master.tenant`); it carries `tenant_name`. Definitions and executions live in the **per-tenant** DBs (`get_tenant_pool(tenant_name)`).
- **A2** Holiday calendar comes from a config/table source `bankos.jobs.holiday.calendar` (details TBD).
- **A3** Cron is 6-field (sec min hour dom mon dow), evaluated in the trigger's `timezone` (default UTC).

---

## 1. Crates & module layout

```
finzly-jobs/                      (workspace)
├── finzly-jobs-model/            lib — shared contract, no infra
│   └── messages, enums (ConcurrencyPolicy, FlowStatus, JobStatus), ids
├── finzly-jobs-sdk/              lib (published) — worker runtime
│   ├── job (trait Job, JobContext, JobResult)
│   ├── registry (JobRegistry)
│   ├── runtime (JobRuntime: consume job-dispatch-<ctx>, run, publish completed)
│   ├── heartbeat (Redis TTL beat + stop)
│   └── depends on: finzly-jobs-model, phoenix-{kafka,redis,postgres,security,config}-sdk, phoenix-jobs-sdk
├── finzly-jobs-orchestrator/     bin — the brain
│   ├── api        (REST control plane: projects/jobs/flows/schedules/resume/queries)
│   ├── scheduler  (cron claim loop + outbox publisher)
│   ├── dispatcher (consume flow.execution.requested → DAG walk → dispatch)
│   ├── completion (consume job.execution.completed → advance/close)
│   ├── sweeper    (retry + SLA/timeout + crash detection)
│   ├── repository (all SQL), migrations/
│   └── depends on: finzly-jobs-model, phoenix SDKs
└── finzly-jobs-worker/           bin — reference worker
    └── main.rs: build JobRuntime, register Job impls, start
```

### SDK public API (the extension point)
```rust
#[async_trait]
pub trait Job: Send + Sync {
    fn name(&self) -> &'static str;
    async fn execute(&self, ctx: &JobContext) -> JobResult;   // returns Success{output} | Failure{reason}
}

pub struct JobContext {
    pub tenant: String,
    pub flow_execution_id: Uuid,
    pub job_execution_id: Uuid,
    pub attempt: i32,
    pub parameters: serde_json::Value,   // from job_definition.parameters
}

// host wiring (mirrors phoenix-workflow-client):
let mut rt = JobRuntime::new(KafkaConfig::from_config(), service_context);
rt.register(DailySettlementJob);           // by job.name()
rt.start().await;                          // subscribes job-dispatch-<ctx>, beats heartbeat, publishes completed
```

---

## 2. Data model (final)

Deltas over [migrations/0001](../migrations/0001_jobs_poc_ddl.sql). New columns **bold**.

**scheduled_trigger** (master DB) — add:
- **`timezone VARCHAR(64) NOT NULL DEFAULT 'UTC'`** (A3)
- **`enabled BOOLEAN NOT NULL DEFAULT true`**
- **`misfire_policy VARCHAR(16) NOT NULL DEFAULT 'SKIP'`** — `SKIP` (jump to next) | `FIRE_ONCE` (catch up one)

**flow_definition** — `flow` JSON becomes the DAG (§3.4). Keep `is_enabled`, `version`, `sla_policy`, `skip_on_holiday`.

**flow_execution** — statuses `RUNNING | SUCCESS | FAILED | TIMED_OUT`; unique `idempotency_key`.

**job_execution** — add:
- **`max_attempts INTEGER NOT NULL DEFAULT 1`** (from retry_policy)
- **`next_retry_at TIMESTAMP`** (backoff target; NULL = not scheduled)
- **`deadline TIMESTAMP`** (started_at + timeoutSeconds; drives SLA sweep)
- **`last_report_at TIMESTAMP`** — updated by a heartbeat *or* an HTTP progress push; the liveness signal for the SLA sweep (§11)
- **`dispatch_mode VARCHAR(8)`** — `EVENT` (Kafka) | `API` (HTTP fire) (§11)
- statuses `QUEUED | RUNNING | SUCCESS | FAILED | TIMED_OUT | SKIPPED`
- index `idx_job_exec(flow_execution_id, status)`; index `idx_job_exec_sweep(status, deadline)`; index `idx_job_exec_retry(status, next_retry_at)`

**execution_lock** — unique `(tenant_name, service_context, job_name)`; carries `job_execution_id`; released on terminal transition. (Redis heartbeat is the liveness source; the lock row is the mutual-exclusion source.)

**outbox** (master or per-tenant, same tx as the claim) — **new**:
```sql
CREATE TABLE outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  topic VARCHAR(255) NOT NULL,
  msg_key VARCHAR(255) NOT NULL,
  tenant_name VARCHAR(64) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  published_at TIMESTAMP           -- NULL until the publisher loop sends it
);
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
```

---

## 3. Topics & message contracts

All payloads are wrapped by `phoenix-kafka-sdk` in `MessageWrapper` (camelCase, real
body under `object`). Bodies below are the `object`.

| Topic | Key | Producer → Consumer |
|---|---|---|
| `finzly.jobs.flow.execution.requested` | `flowDefinitionId` | orchestrator(scheduler) → orchestrator(dispatcher) |
| `job-dispatch-<service_context>` | `jobName` (QUEUE ⇒ FIFO per key) | orchestrator(dispatcher) → worker SDK |
| `job-report-<service_context>` | `jobExecutionId` | business service → **worker** — Kafka report transport (§11.3) |
| `finzly.jobs.job.execution.completed` | `jobName` | **worker → orchestrator** — final verdict + `slaBreached` (§11.4) |
| `<topic>.DLT` | — | dead-letter after N failed handles |

**HTTP transports (§11):**
| Endpoint | Direction |
|---|---|
| `POST <callbackEndpoint>` (on the business service) | **worker → service** — API fire (TLS-verified), returns `202 Accepted` |
| `POST <worker>/report/{jobExecutionId}` (on the worker) | **service → worker** — HTTP report transport (terminal + progress) |

### 3.1 FlowExecutionRequested
```json
{ "flowDefinitionId": "uuid", "tenantName": "banka", "scheduleFireTime": "2026-07-06T04:51:06.788593" }
```
`idempotencyKey = flowDefinitionId + ":" + scheduleFireTime` (recomputed by the consumer).

### 3.2 JobExecutionRequested
```json
{ "flowExecutionId":"uuid", "jobExecutionId":"uuid", "tenantName":"banka",
  "jobName":"daily-settlement", "serviceContext":"settlement-service",
  "attempt":1, "parameters":{ ... } }
```

### 3.3 JobExecutionCompleted
```json
{ "flowExecutionId":"uuid", "jobExecutionId":"uuid", "tenantName":"banka",
  "jobName":"daily-settlement", "serviceContext":"settlement-service",
  "status":"SUCCESS", "output":"...", "errorMessage":null }
```

### 3.4 flow DAG (`flow_definition.flow`)
```json
{ "startTask": "t1",
  "tasks": [
    { "taskId":"t1", "jobName":"daily-settlement", "serviceContext":"settlement-service",
      "concurrencyPolicy":"SKIP", "retryPolicy":{"maxRetries":3,"delaySeconds":60},
      "next":["t2"] },
    { "taskId":"t2", "jobName":"post-recon", "serviceContext":"settlement-service",
      "concurrencyPolicy":"QUEUE", "retryPolicy":{"maxRetries":1,"delaySeconds":0}, "next":[] }
  ] }
```

---

## 4. State machines

**flow_execution**
```
(insert) → RUNNING ──all jobs terminal, none FAILED/TIMED_OUT──▶ SUCCESS
                   ──any job FAILED/TIMED_OUT (no retries left)─▶ FAILED
        resume: FAILED ─▶ RUNNING
```
**job_execution**
```
QUEUED ─dispatched─▶ RUNNING ─completed SUCCESS─▶ SUCCESS
RUNNING ─completed FAILED, attempt<max─▶ (schedule retry) ─▶ RUNNING (attempt+1)
RUNNING ─completed FAILED, attempt=max─▶ FAILED
RUNNING ─deadline passed─▶ TIMED_OUT           (SLA sweep)
RUNNING ─heartbeat lost─▶ FAILED(WORKER_LOST)  (crash sweep)
QUEUED  ─SKIP lock held─▶ SKIPPED               (at dispatch)
```
Terminal = SUCCESS | FAILED | TIMED_OUT | SKIPPED. All terminal transitions **release the execution_lock**.

---

## 5. End-to-end flows

Each flow lists steps (DB / Kafka / state) and the **cases** it must handle.

### F1 — Create & schedule (REST, orchestrator/api)
1. `POST /projects/{p}/jobs` → insert `job_definition` (per-tenant DB).
2. `POST /flows` → validate DAG (§6.V), insert `flow_definition` (new version).
3. `POST /flows/{name}/schedules` → insert `scheduled_trigger` (master DB) with cron+timezone, `next_fire_time = next(cron, tz)`.
- Cases: **invalid cron** → 400 at validation; **DAG cycle / unknown jobName / bad endpoint** → 400; **duplicate name** → upsert/version bump; single job → auto-wrap in a one-task flow.

### F2 — Scheduler claim & fire (orchestrator/scheduler)
Runs every `poll.interval` (default 5s). Per master DB:
1. **Tx:** `SELECT ... FROM scheduled_trigger WHERE enabled AND next_fire_time ≤ now() ORDER BY next_fire_time FOR UPDATE SKIP LOCKED LIMIT N`.
2. For each: compute `next = next_fire(cron, timezone)`; apply **misfire policy** (SKIP → jump to next future; FIRE_ONCE → keep one catch-up). `UPDATE next_fire_time, last_fire_time = now()`.
3. **Same tx:** `INSERT outbox(topic=flow.execution.requested, key=flowDefinitionId, tenant, payload)`. **Commit.**
4. **Outbox publisher loop** (separate, every 1s): `SELECT ... WHERE published_at IS NULL FOR UPDATE SKIP LOCKED`, publish to Kafka, set `published_at`.
- Cases: **multi-tenant** — one master query returns all tenants (rows carry `tenant_name`); publish per row. **Pod crash between claim & publish** — outbox row survives, published on recovery (no lost fire). **Invalid/omitted cron** (manual jobs have no trigger) — never claimed. **Bad cron on an existing trigger** → `next` = None → set `enabled=false` + log (don't NULL silently). **Duplicate publish** from outbox retry → deduped downstream by idempotencyKey.

### F3 — Flow dispatch (orchestrator/dispatcher ← flow.execution.requested)
1. Resolve tenant pool from `tenantName`; load enabled latest `flow_definition` by id.
2. `INSERT flow_execution (... status=RUNNING) ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id`. **No row ⇒ duplicate fire ⇒ ack & stop.**
3. Dispatch `startTask` (and any task with no unmet predecessors). For each task → **§F3a**.
- Cases: **unknown flow id** → log+ack; **flow disabled since fire** → close flow as SKIPPED; **duplicate delivery** → idempotency guard (step 2); **DAG with multiple roots** → dispatch all roots.

#### F3a — Dispatch one job (per concurrency policy)
Load `job_definition` (scoped to the flow's project) for `service_context` + `parameters`. In a tx:
- Insert `job_execution` (attempt=1, max_attempts from retryPolicy, deadline=NULL until RUNNING).
- **SKIP:** `INSERT execution_lock ... ON CONFLICT DO NOTHING RETURNING id`. If **no row** (already running) → set job `SKIPPED`, commit, don't publish. Else status `RUNNING`.
- **QUEUE:** status `QUEUED`; publish **ordered** (key=jobName) so the worker drains FIFO.
- **PARALLEL:** status `RUNNING`, no lock, publish unordered.
- Commit, then `INSERT outbox(job-dispatch-<ctx>, key=jobName, payload=JobExecutionRequested)` — **publish via outbox** so a crash after commit doesn't drop the dispatch.
- Cases: **no job_definition** → mark job FAILED(CONFIG); **publish failure** → outbox retries; **lock held by a dead worker** → released by crash sweep (§F8), then this flow's retry re-dispatches.

### F4 — Execute (worker SDK ← job-dispatch-<ctx>)
1. Consume `JobExecutionRequested`. **Idempotency:** load `job_execution`; if already terminal → ack & skip (duplicate delivery).
2. Set `job_execution` RUNNING + `deadline = now()+timeoutSeconds` (if not already); start **heartbeat** `SET heartbeat-<jobExecId>:RUNNING EX 120`, refresh every 60s.
3. Look up `Job` by name in the registry; run `execute(ctx)`.
4. Stop heartbeat; publish `JobExecutionCompleted{status, output|errorMessage}` (via the worker's outbox or direct — retry on failure).
- Cases: **job not registered** → publish FAILED(NO_HANDLER); **panic/error in job** → FAILED(reason); **duplicate request** → step 1 guard; **worker dies mid-run** → heartbeat lapses → §F8; **completed publish fails** → retry; if worker dies after run before publish → job still RUNNING → §F8 marks it (may double-run on retry — jobs should be idempotent; documented contract).

### F5 — Completion (orchestrator/completion ← job.execution.completed)
In a tx, `SELECT flow_execution FOR UPDATE` (serialize completion):
1. `UPDATE job_execution SET status,output,error,ended_at` **WHERE not already terminal** (idempotent).
2. `DELETE execution_lock WHERE job_execution_id = ...`.
3. If FAILED and `attempt < max_attempts` → **schedule retry** (§F6) instead of failing.
4. Else advance DAG: dispatch tasks whose predecessors are now all SUCCESS (§F3a).
5. If no jobs `QUEUED|RUNNING` and none retry-pending → close `flow_execution`: `FAILED` if any terminal-failed, else `SUCCESS`.
- Cases: **out-of-order / duplicate completion** → idempotent update + `FOR UPDATE` serialization; **completion for an already-closed flow** → no-op; **concurrent completions** → serialized by the flow row lock.

### F6 — Retry
On FAILED with `attempt < max_attempts`: set `next_retry_at = now() + delaySeconds`, keep status FAILED (retry-pending). **Retry sweep** (every N s): `SELECT job_execution WHERE status='FAILED' AND next_retry_at ≤ now() AND attempt < max_attempts FOR UPDATE SKIP LOCKED` → insert/redispatch with `attempt+1` (§F3a). Exhausted retries → job stays FAILED, flow can fail.
- Cases: **backoff** = linear (delaySeconds) or exponential (policy); **retry storms** bounded by SKIP LOCKED batch.

### F7 — SLA / timeout sweep
Every N s: `SELECT job_execution WHERE status='RUNNING' AND deadline ≤ now()` → set `TIMED_OUT`, release lock, publish nothing (worker may still finish — its late completion is ignored by the idempotent guard). Flow-level SLA (`slaSeconds`) breach → flag/alert (no state change) or fail per policy.

### F8 — Crash detection (heartbeat sweep)
Every 60s: `SELECT job_execution WHERE status='RUNNING'`; for each `RedisClient.exists("heartbeat-<id>:RUNNING")`. **Missing ⇒ worker lost** → set `FAILED(WORKER_LOST)`, release lock, then normal retry (§F6) / flow-fail (§F5). (No Redis SCAN needed — we drive from Postgres, per the SDK audit.)
- Cases: **heartbeat set but worker wedged** (not dead) → covered by SLA timeout (§F7); **Redis blip** → key present or sweep tolerates one miss (grace = 2 cycles) to avoid false positives.

### F9 — Resume (REST)
`POST /api/flows/{flowId}/executions/{executionId}/resume`:
1. **Concurrency check:** reject if the flow has an active (RUNNING) execution elsewhere.
2. Find FAILED/TIMED_OUT `job_execution` rows; for each insert new attempt (`attempt+1`), status QUEUED/RUNNING.
3. Flip `flow_execution` FAILED → RUNNING; re-dispatch the failed tasks (§F3a). Normal flow resumes.

### F10 — Manual trigger
`POST /jobs/{name}/trigger` (no-cron jobs): synthesize a one-task flow fire → publish `flow.execution.requested` with `scheduleFireTime=now()` (idempotencyKey makes double-clicks safe).

---

## 6. Correctness invariants & validation

- **Exactly-once claim** across pods — `FOR UPDATE SKIP LOCKED` on `scheduled_trigger`.
- **No lost fire** — claim + outbox insert in one tx; outbox publisher is at-least-once.
- **Idempotent fires** — unique `flow_execution.idempotency_key`.
- **Idempotent job execution** — worker checks terminal status before running; completion updates guard on non-terminal.
- **Mutual exclusion** — `execution_lock` unique `(tenant, service_context, job_name)`; every terminal transition releases it; crash sweep is the backstop.
- **Completion serialization** — `SELECT flow_execution FOR UPDATE` before deciding closure.
- **Offset discipline** — consumers `enable.auto.commit=false`; commit only after successful handle; return `Err` on transient failure (redelivery); route to `<topic>.DLT` after N attempts (poison).
- **(V) DAG validation** at `POST /flows`: acyclic (topo-sort), every `next` target exists, every `jobName` resolves to a `job_definition`, `startTask` exists, `serviceContext` known.

---

## 7. Edge-case matrix

| Case | Handled by |
|---|---|
| Pod dies between claim and publish | outbox (F2.3) |
| Duplicate `flow.execution.requested` | idempotency_key (F3.2) |
| Duplicate `job-dispatch` | worker terminal-status guard (F4.1) |
| Duplicate `job.execution.completed` | idempotent update (F5.1) |
| Two completions race to close flow | `FOR UPDATE` on flow row (F5) |
| Worker crash mid-job | heartbeat sweep → FAILED → retry (F8/F6) |
| Worker wedged (alive, stuck) | SLA deadline → TIMED_OUT (F7) |
| Late completion after timeout | ignored by terminal guard (F7) |
| SKIP job already running | lock conflict → SKIPPED (F3a) |
| Orphan lock (dead holder) | crash sweep releases (F8) |
| Bad cron on live trigger | disable + log (F2) |
| Missed fires (outage) | misfire policy (F2.2) |
| Flow disabled after fire | close SKIPPED (F3) |
| Job handler not registered | FAILED(NO_HANDLER) (F4) |
| Poison message | DLQ after N (F6 invariants) |
| Multi-tenant | master query + per-row tenant pool (F2) |

---

## 8. Config keys

```
bankos.tenants                              # comma list (multi-tenant iteration)
bankos.jobs.master.tenant                   # A1: master DB for scheduled_trigger + outbox
bankos.scheduler.poll.interval=5
bankos.scheduler.claim.batch=50
bankos.scheduler.outbox.interval=1
bankos.jobs.sweep.interval=30               # retry + SLA + crash sweeps
bankos.jobs.heartbeat.ttl=120
bankos.jobs.heartbeat.refresh=60
bankos.jobs.consumer.max-deliveries=5       # then DLQ
finzly.jobs.flow.execution.requested.topic
finzly.jobs.job.execution.completed.topic
finzly.jobs.dispatch.topic.prefix=job-dispatch-
spring.kafka.bootstrap-servers / properties.security-protocol
bankos.redis.cache.common.member.{ip,port}
```

---

## 9. Delivery guarantees summary

- Scheduler → orchestrator: **at-least-once** (outbox), deduped by idempotency_key.
- Orchestrator → worker: **at-least-once** (outbox), deduped by job terminal-status guard.
- Worker → orchestrator: **at-least-once**, deduped by idempotent completion update.
- Net effect: **effectively-once state transitions**; job side-effects must be idempotent (documented worker contract), because a crash-after-run-before-report can re-dispatch.

---

## 10. Still open

- **A1** master DB placement (common vs per-tenant `scheduled_trigger`) — changes F2 query shape.
- **A2** holiday calendar source for `skip_on_holiday`.
- Retry backoff shape (linear vs exponential) — policy field.
- Whether flow-level SLA breach only alerts or also fails the flow.
- Build-vs-reuse depth of `phoenix-jobs-sdk` (trait+registry+status reuse confirmed; how much of its REST/Azkaban parts we ignore).

---

## 11. Dispatch & the worker-mediated report path

**Key principle:** the **worker fires the job, so the worker owns the SLA verdict.** The
business service reports its result **back to the worker** (not the orchestrator) —
because only the worker knows the fire→response elapsed time for that attempt. The worker
then reports the *final* outcome (with the SLA/timeout verdict) up to the orchestrator.

```
orchestrator ──job-dispatch-<ctx>──▶ worker ──fire (HTTP/TLS or event)──▶ service
                                       ▲                                      │
                                       └──────── job-report-<ctx> ────────────┘   (SDK)
worker ──job.execution.completed (+ sla_breached, durations)──▶ orchestrator
```

### 11.1 Assignment: orchestrator → worker
Orchestrator publishes `JobExecutionRequested` to `job-dispatch-<service_context>` (§F3a,
concurrency policy applied). The **worker** consumes it — the worker is "handled by" the
orchestrator (assigned work), and is the component that actually invokes the service.

### 11.2 Fire: worker → service (SSL-safe)
The worker invokes the business service and **must not let TLS be downgraded/bypassed** —
verify the cert chain, no `danger_accept_invalid_certs`, honor the configured CA. Modes:
- **API (HTTP)** — worker `POST`s the fire to the service `callbackEndpoint` over HTTPS;
  service returns **`202 Accepted`** (ack, *not* completion). Worker records `fired_at`,
  sets `deadline = fired_at + timeoutSeconds`, starts the SLA timer.
- **EVENT (Kafka)** — worker publishes to the service's `eventTopic`; same SLA timer starts.

| Fire result | Meaning | Action |
|---|---|---|
| `2xx` (ideally 202) | Accepted | job `RUNNING`; SLA timer armed |
| non-2xx / TLS error / timeout | Not accepted | dispatch failure → retry the fire (bounded); not a job failure |

### 11.3 Report back: service → worker (`JobReporter`)
The service reports the terminal result (and optional progress) **to the worker** via one
SDK call. Transport is the service's choice (`finzly.jobs.report.transport = kafka | http`):
- **kafka** → publish to **`job-report-<service_context>`** (consumed by the worker pool).
- **http** → `POST` the worker's report ingress (worker base URL from the fire payload).

Either way the message is correlated by `job_execution_id`; any worker in the pool can
handle it because the fire record (`fired_at`, `deadline`) is in shared Postgres — so the
"worker layer" owns SLA without pinning to one stateful pod.

```rust
pub struct JobHandle {                 // delivered to the service in the fire; echoed back
    pub job_execution_id: Uuid, pub flow_execution_id: Uuid,
    pub tenant: String, pub service_context: String, pub job_name: String,
    pub report_to: ReportTarget,       // worker topic (kafka) or worker URL (http)
}
impl JobReporter {                     // SDK helper on the SERVICE side
    pub fn from_config() -> Result<Self>;
    pub async fn progress(&self, h: &JobHandle, note: &str) -> Result<()>;
    pub async fn success(&self,  h: &JobHandle, output: impl Into<String>) -> Result<()>;
    pub async fn failure(&self,  h: &JobHandle, reason: impl Into<String>) -> Result<()>;
}
```

### 11.4 Verdict & forward: worker → orchestrator
On receiving the service's report (or when its SLA timer fires first), the worker:
1. computes `elapsed = now - fired_at`; sets `sla_breached = elapsed > slaSeconds`;
2. if the timer fired with no report → verdict `TIMED_OUT` (and ignores any late report);
3. publishes `JobExecutionCompleted` (§3.3, **+ `slaBreached`, `elapsedMs`**) to the
   orchestrator, which runs completion (F5).

The orchestrator keeps only a **coarse backstop**: if the *worker itself* dies (heartbeat
lapses, §F8) the orchestrator sweep fails the job. Fine-grained SLA lives in the worker.

### 11.5 Liveness
`job_execution.last_report_at` is bumped by the worker on each service progress push (or
the worker's own heartbeat while awaiting). Two levels:
- **worker-local SLA timer** — precise, per fire (primary).
- **orchestrator sweep** on `deadline`/heartbeat — backstop for a dead worker.

### 11.6 Case matrix (transport-specific)
| Case | Handling |
|---|---|
| Fire: TLS handshake fails / cert invalid | fire rejected → dispatch retry; never silently downgraded (11.2) |
| API fire 202, service never reports | worker SLA timer → TIMED_OUT → report up (11.4) |
| API fire 5xx / timeout | worker retries the fire (bounded), not a job failure |
| Service reports to worker but worker pod restarted | another worker consumes `job-report-<ctx>`, correlates via shared fire record (11.3) |
| Duplicate report (retry, or kafka+http) | idempotent completion update (F5.1) |
| Report arrives after worker already timed out | ignored by terminal-status guard |
| Worker itself dies mid-wait | orchestrator heartbeat sweep → FAILED(WORKER_LOST) (F8) |
| Service picks kafka but broker down | `JobReporter` buffers/retries; worker SLA timer still fires as backstop |
