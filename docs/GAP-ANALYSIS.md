# finzly-jobs-poc — Low-Level Gap Analysis

What's built vs. what the full design (the flow diagrams) needs, layer by layer.
Grounded in the current code and the Finzly SDKs we'd build on.

**Severity:** P0 = correctness/data-loss (fix before trusting it) · P1 = needed for
design fidelity · P2 = hardening / nice-to-have.
**Effort:** S ≤ half day · M ≈ 1–2 days · L ≈ 3+ days.

Current state: happy-path pipeline works (scheduler → `flow.execution.requested` →
dispatch with SKIP/QUEUE/PARALLEL + `execution_lock` → worker → `job.execution.completed`
→ flow closed). Execution is **log-only**. Verified DB-side against Postgres 16; the
Kafka leg and demo native build are unverified.

---

## Layer 0 — SDK / shared "JOB SDK"

The design's **JOB SDK** (heartbeat embedded in every processor) does not exist. The
audit shows what to reuse vs. build.

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 0.1 | **Heartbeat library** | P1 | M | `phoenix-redis-sdk` gives the primitives — no abstraction to reuse. Build a small module: on job start `RedisClient.set_string("heartbeat-{jobExecId}:RUNNING", pod_id, Some(120))`; refresh every 60s via `set_string`/`expire`; on completion `delete`. `use phoenix_redis_sdk::RedisClient`. Config keys `bankos.redis.cache.common.member.{ip,port}`. Add `phoenix-redis-sdk` to `Cargo.toml` (already a dep). |
| 0.2 | **Crash-detection sweeper** | P1 | M | Redis SDK has **no SCAN**. Don't enumerate `heartbeat-*`; instead sweep Postgres: every 60s select `job_execution WHERE status='RUNNING'`, and for each call `RedisClient.exists("heartbeat-{id}:RUNNING")`. Missing key ⇒ mark job + its flow `FAILED`. New module in `orchestrator/`. |
| 0.3 | **Reuse `phoenix-jobs-sdk` for real execution** | P1 | M | It already has `Job` trait (`execute(&JobContext)->JobResult`), `JobRegistry` (name→handler), `JobService` (spawn+track), `JobStatusService` (Postgres). Our `worker.rs` is a log-only stand-in — replace with a `JobRegistry` of real `Job` impls. Note: jobs-sdk is Azkaban/HTTP-oriented and has **no Kafka**, so we keep our Kafka transport and only borrow the trait+registry+execution bits. |
| 0.4 | **Multi-tenant iteration helper** | P1 | S | No `get_all_tenants()` in any SDK. Establish the convention: read `bankos.tenants` (comma-split) — or reuse `JobsConfig::from_config().tenants` — and loop `phoenix_postgres_sdk::get_tenant_pool(t)`. Needed by scheduler (1.1) and sweeper (0.2). |

---

## Layer 1 — Job Scheduler ([scheduler.rs](../src/orchestrator/scheduler.rs))

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 1.1 | **Single-tenant claim** | P0 | M | `claim_due_jobs()` uses `default_tenant()` only (`scheduler.rs:76`). `scheduled_trigger` is the master table with a `tenant_name` column, but we query one pool. Decide where the master table lives; if per-tenant, loop tenants (0.4); if one common DB, query once and group by `tenant_name`. Today non-default tenants never fire. |
| 1.2 | **Fire loss on publish failure** | P0 | M | `claim_due_jobs` advances `next_fire_time` and commits, *then* `publish_flow_requested` runs after commit (`scheduler.rs:59,135`). If the publish fails, the fire is logged and **lost** (at-most-once). Options: transactional **outbox** table drained by a publisher, or publish-then-commit. Idempotency at the consumer already dedupes, so at-least-once is safe. |
| 1.3 | **No misfire / catch-up policy** | P1 | S | On advance, `next_fire_from_now(cron)` computes the next occurrence **relative to now** (`scheduler.rs:116`). A pod outage silently skips every missed fire. Add a policy (fire-once-immediately vs. skip) computed from `last_fire_time` + cron. |
| 1.4 | **No per-schedule timezone** | P1 | M | Cron is evaluated in UTC (`schedule.rs` uses `SystemTime`→UTC). The design's create-flow has a "cron time zone" input. Add `scheduled_trigger.timezone` and evaluate cron in that zone (chrono-tz). |
| 1.5 | **`skip_on_holiday` ignored** | P1 | M | Column exists on `flow_definition` but the scheduler never checks it. Needs a holiday calendar source + skip/defer logic on claim. |
| 1.6 | **Invalid cron → silent stop** | P1 | S | If `next_fire_from_now` returns `None` (bad cron), `next_fire_time` is set `NULL` (`scheduler.rs:116,122`) and the trigger never fires again, silently. Validate cron at registration and/or mark the trigger errored + log. |
| 1.7 | **No delayed delivery for future-dated** | P2 | M | Design computes delivery delay = `max(0, next_fire - now)`. Kafka SDK has no native delay. Not implemented; fine while fires are "due now", needed for pre-published future fires. |
| 1.8 | **No trigger enable/disable** | P2 | S | Only `flow_definition.is_enabled` gates. No way to pause a single `scheduled_trigger`. Add an `enabled` column + WHERE clause. |

---

## Layer 2 — Orchestrator: flow dispatch ([executor.rs](../src/orchestrator/executor.rs))

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 2.1 | **Flat flow JSON, no DAG** | P1 | L | `flow` is `{"jobs":[...]}` and all jobs dispatch at once (`executor.rs:79`). The design is a `tasks[]` DAG with `next[]`, `startTask`, and per-task `concurrencyPolicy`/`retryPolicy`. Needs a DAG model + topological execution driven by completions (2.7). |
| 2.2 | **`job_definition` looked up by name, unscoped** | P0 | S | `dispatch_job` does `WHERE name=$1 LIMIT 1` (`executor.rs:95`). `job_definition` is unique per `(project_id, name)`, so a name reused across projects picks an arbitrary row. Scope the flow's jobs to their project/definition id. |
| 2.3 | **Orphan `execution_lock` on failure/crash** | P0 | M | SKIP acquires the lock in-tx then publishes after commit (`executor.rs:128,160`). If the publish fails or the worker dies, the lock is **never released** (completion never arrives) → that job is skipped forever. Depends on crash-detection (0.2) to release, plus a lock TTL/`locked_at` reaper. |
| 2.4 | **No job-dispatch idempotency** | P1 | M | A re-delivered `job.execution.requested` re-runs the job (`worker.rs` has no dedupe). Add an idempotency key per (job_execution_id) or make the worker check job_execution status before running. |
| 2.5 | **Real job invocation missing** | P1 | L | `worker.rs` logs and returns SUCCESS. Real design: API jobs → HTTP callback to `service_context`/`callbackEndpoint`; or run the bean via `phoenix-jobs-sdk` `Job` (0.3). `parameters` already carries `callbackEndpoint`/`serviceBaseUrl`. |
| 2.6 | **Retry policy never applied** | P1 | M | `retry_policy` (`{maxRetries}`) is stored but unused. On FAILED, no re-dispatch with `attempt+1`/backoff. Wire into completion (Layer 3). |
| 2.7 | **PIPELINE / QUEUE state fidelity** | P2 | M | `ConcurrencyPolicy` handles SKIP/QUEUE/PARALLEL; design also shows PIPELINE. QUEUE rows stay `QUEUED` and never transition to `RUNNING` (worker doesn't flip it). Add proper state transitions and DAG-driven "dispatch next" (ties to 2.1). |

---

## Layer 3 — Completion, lifecycle & reliability ([completion.rs](../src/orchestrator/completion.rs), [consumer.rs](../src/orchestrator/consumer.rs))

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 3.1 | **Consumer swallows errors → offset committed on failure** | P0 | S | `OrchestratorHandler::handle_message` logs internal errors and returns `Ok(())` (`consumer.rs`). With the SDK committing offsets after `Ok`, a failed DB write is **lost** (at-most-once). Return `Err` to force redelivery (needs handlers idempotent — mostly true) and/or a dead-letter topic. |
| 3.2 | **Flow-completion race** | P1 | M | Two concurrent `job.execution.completed` for the same flow each run "in_flight==0 ⇒ complete" (`completion.rs`) in separate txns; interleaving can double-close or miss. Serialize via `SELECT ... FOR UPDATE` on the flow_execution row, or advisory lock per flow. |
| 3.3 | **No SLA / timeout enforcement** | P1 | M | `sla_policy` (`{slaSeconds,timeoutSeconds}`) stored, never enforced. No `TIMED_OUT` transition. Needs a deadline sweep (pairs with 0.2). |
| 3.4 | **Crash detection** | P1 | M | See 0.2 — nothing marks a dead worker's job FAILED; it sits RUNNING forever. |
| 3.5 | **Resume endpoint** | P1 | M | `POST /api/flows/{flowId}/executions/{executionId}/resume` not built. Design: concurrency-check, find FAILED job execs, insert `attempt+1`, flip flow FAILED→RUNNING, re-publish `job.execution.requested`. |

---

## Layer 4 — REST API / registration ([registration/](../src/registration))

Only `POST /job/registrations` (YAML upload) exists. The design's control-plane API is absent.

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 4.1 | `POST /projects/{p}/jobs`, `POST /flows`, `POST /flows/{id}/schedules` | P1 | L | First-class CRUD instead of only the bulk YAML mapper. |
| 4.2 | **Flow validation** (cycle / edge / endpoint checks) | P1 | M | Design's PHASE B validates the DAG before insert. Nothing validates today. |
| 4.3 | Manual trigger `POST /jobs/{id}/trigger` | P2 | S | For manual-only (no-cron) jobs. |
| 4.4 | Query/status endpoints (`GET` flow/job executions) | P2 | S | No way to read execution state over HTTP for a UI. |

---

## Layer 5 — Data model ([migrations/0001](../migrations/0001_jobs_poc_ddl.sql))

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 5.1 | `execution_lock` has no TTL / reaper | P0 | S | Add `locked_at` usage + a reaper (ties to 2.3/0.2). Column exists; no expiry logic. |
| 5.2 | Composite index for in-flight query | P2 | S | `completion.rs` counts `WHERE flow_execution_id=? AND status IN (...)`; add `idx_job_execution(flow_execution_id, status)`. |
| 5.3 | `scheduled_trigger` missing columns | P1 | S | `timezone` (1.4), `enabled` (1.8), misfire policy (1.3). |
| 5.4 | Retry/SLA columns | P1 | S | `job_execution.max_attempts`, `flow_execution.sla_deadline` for 2.6/3.3 (or derive from policy JSON). |

---

## Layer 6 — Config / infra / ops

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 6.1 | **Config precedence: test3 overrides local Kafka** | P0 | S | `get_configs()` loads `.env` then the test3 cloud config **last (overwrites)**. Our local `spring.kafka.*` wins only if test3 doesn't define them. Verify with `RUST_LOG=debug`; ensure those keys are absent from the test3 `finzly.jobs` config, or add a local-override hook. |
| 6.2 | `migration_enabled()` hard-coded | P2 | S | Currently returns `true` in [main.rs](../src/main.rs); should read `spring.liquibase.enabled`. |
| 6.3 | Base schema must pre-exist | P1 | S | SDK sets `search_path` to `demo_galaxy_jobs` and puts the migration ledger there; provision `CREATE SCHEMA` out-of-band (documented in DESIGN.md). |
| 6.4 | No graceful shutdown / readiness | P2 | M | `/ping` is liveness only; nothing reflects consumer/Kafka health; no signal handling to drain. |
| 6.5 | No dead-letter topic | P1 | S | Pairs with 3.1 — poison messages currently either loop or are dropped. |
| 6.6 | Trace propagation / metrics | P2 | M | SDK wrapper carries `traceDetails`, but we don't thread a span across scheduler→dispatch→worker→completion; no metrics on delay/throughput. |

---

## Layer 7 — Testing

| # | Item | Sev | Eff | Low-level notes |
|---|---|---|---|---|
| 7.1 | Unit tests | P1 | S | `ConcurrencyPolicy::parse`, message serde round-trips, `idempotency_key` — no infra needed. |
| 7.2 | DB integration tests | P1 | M | Seed project/flow/trigger; assert `claim_due_jobs` returns + advances, and dispatch/completion transitions. (The SQL dry-run already validated the happy path + idempotency + skip-locked.) |
| 7.3 | Kafka leg unverified | P1 | M | Publish/consume/serde untested (no broker was up). Bring up compose Kafka + run the pipeline. |
| 7.4 | Demo native build | P2 | S | `kafka-scheduler-demo` needs `cmake` (rdkafka `cmake-build`); not built here. |

---

## Suggested order

1. **P0 correctness first:** 3.1 (error/commit), 1.2 (fire loss), 2.2 (job lookup), 2.3+5.1 (orphan locks), 6.1 (Kafka config), 1.1 (multi-tenant).
2. **Reliability & lifecycle:** 0.1+0.2+3.4 (heartbeat/crash), 2.6 (retry), 3.3 (SLA), 3.2 (completion race), 3.5 (resume).
3. **Fidelity:** 2.1/2.7 (DAG), 0.3 (real execution), 2.5, Layer 4 (REST + validation), 1.3–1.5 (misfire/tz/holiday).
4. **Hardening:** Layer 6 ops, Layer 7 tests.
