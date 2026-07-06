# finzly-jobs-sdk — Integration Guide

How any service becomes a **job worker** by importing `finzly-jobs-sdk`. You write
`Job` implementations and register them; the SDK does all the plumbing — consuming
the dispatch topic, idempotency, heartbeat, running your job, and reporting the
result back to the orchestrator.

This mirrors how services embed `phoenix-workflow-client` today (a process-global
runtime you register handlers into, then start).

---

## 1. What the SDK gives you vs. what you write

| The SDK handles (you get for free) | You provide |
|---|---|
| Consume `job-dispatch-<service_context>` | Your `service_context` + Kafka/Redis config |
| Duplicate-delivery guard (idempotency) | `Job` implementations |
| Set job `RUNNING` + `deadline` | Register them + `start()` in `main` |
| **Heartbeat** to Redis (TTL 120s, refresh 60s) | (jobs must be idempotent — see §7) |
| Run your job, catch panics/errors | |
| Publish `job.execution.completed` (SUCCESS/FAILED + output) | |
| Manual offset commit, retry/DLQ on the consumer | |

Retries, SLA/timeout, crash-recovery, and concurrency policy are decided **by the
orchestrator**, not your code (see LOW-LEVEL-DESIGN §5–7). You just run the work.

---

## 2. Add the dependency

`finzly-jobs-sdk` is published to the private registry `finzly-finzly-rust-hosted`.

```toml
# Cargo.toml
[dependencies]
finzly-jobs-sdk = "=0.1.0"          # from finzly-finzly-rust-hosted
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
serde_json = "1"
# The SDK re-exports what you need, but these are the transitive infra deps it uses:
# phoenix-config-sdk, phoenix-kafka-sdk, phoenix-redis-sdk,
# phoenix-postgres-sdk (only if your jobs touch the DB), phoenix-security-sdk
```

---

## 3. Configuration the host must supply

Read from `.env` / SSM / cloud-config via `phoenix-config-sdk` (same mechanism as
every Finzly service).

```properties
env=test3
rust.application.name=settlement.jobs
config_url=...                                  # or leave unset for pure-local

# Which dispatch topic this worker consumes: job-dispatch-<service_context>
finzly.jobs.service.context=settlement-service

# Kafka (local dev shown; prod comes from cloud-config)
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.properties.security-protocol=PLAINTEXT     # PLAINTEXT locally; SSL in prod

# Redis (heartbeat)
bankos.redis.cache.common.member.ip=localhost
bankos.redis.cache.common.member.port=6379

# Postgres per-tenant (only if your jobs query the DB)
bankos.tenants=banka,bankb
# pg.db.tenant.<t>.{ip,port,username,password}
```

> **Note:** cloud-config is loaded **last and overwrites** local `.env`. If your env
> supplies `spring.kafka.*`, the local values win only when cloud-config doesn't
> define them (see LOW-LEVEL-DESIGN / gap 6.1).

---

## 4. Implement a `Job`

One `impl Job` per unit of work. `name()` must match the `jobName` used in the flow
definition. Return `JobResult::success(output)` or `JobResult::failure(reason)`.

```rust
use finzly_jobs_sdk::{Job, JobContext, JobResult};
use async_trait::async_trait;

pub struct DailySettlementJob;

#[async_trait]
impl Job for DailySettlementJob {
    fn name(&self) -> &'static str { "daily-settlement" }

    async fn execute(&self, ctx: &JobContext) -> JobResult {
        // Per-tenant DB pool (only if you need it):
        let pool = match phoenix_postgres_sdk::get_tenant_pool(&ctx.tenant).await {
            Ok(p) => p,
            Err(e) => return JobResult::failure(format!("no db pool: {e}")),
        };

        // Read your parameters (from job_definition.parameters):
        let cutoff = ctx.parameters.get("cutoff").and_then(|v| v.as_str()).unwrap_or("EOD");

        // ... do the settlement work, using ctx.attempt for retry-awareness ...

        match run_settlement(&pool, ctx.tenant.as_str(), cutoff).await {
            Ok(count) => JobResult::success(format!("settled {count} txns")),
            Err(e)    => JobResult::failure(e.to_string()),
        }
    }
}
```

### `JobContext` (what you receive)
```rust
pub struct JobContext {
    pub tenant: String,            // resolve DB/cache per-tenant with this
    pub flow_execution_id: Uuid,
    pub job_execution_id: Uuid,    // stable per attempt-instance
    pub attempt: i32,              // 1-based; >1 means this is a retry
    pub parameters: serde_json::Value,   // from job_definition.parameters
}
```

### `JobResult` (what you return)
```rust
JobResult::success(output: impl Into<String>)     // -> status SUCCESS, output stored
JobResult::failure(reason: impl Into<String>)     // -> status FAILED, error stored; orchestrator retries per policy
```
A panic or an `Err` bubbling out is treated as `failure` — but prefer returning
`JobResult::failure` with a clear reason.

---

## 5. Register and start (in `main`)

Build one `JobRuntime` for the process, register every job, then `start()`. The
runtime subscribes to your `job-dispatch-<service_context>` and runs forever.

```rust
use finzly_jobs_sdk::JobRuntime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config first (populates the config map the SDK reads).
    let _ = phoenix_config_sdk::config_properties::get_configs().await;

    let mut rt = JobRuntime::from_config()?;     // reads finzly.jobs.service.context, kafka, redis
    rt.register(DailySettlementJob);
    rt.register(PostReconJob);
    rt.start().await?;                            // blocks: runs the consumer loop
    Ok(())
}
```

`from_config()` reads `finzly.jobs.service.context`, Kafka, and Redis settings. If
you prefer explicit wiring: `JobRuntime::new(kafka_config, "settlement-service")`.

That's the entire worker binary — everything else is the SDK.

---

## 6. What happens at runtime (per dispatched job)

1. SDK consumes `JobExecutionRequested` from `job-dispatch-settlement-service`.
2. **Idempotency:** if that `job_execution_id` is already terminal, it acks and skips.
3. Marks the job `RUNNING`, sets `deadline = now + timeoutSeconds`, starts the heartbeat.
4. Looks up your `Job` by `name`, calls `execute(ctx)`.
5. Stops the heartbeat; publishes `JobExecutionCompleted` with your result.
6. Commits the Kafka offset only after all that succeeds; transient failures redeliver, poison messages go to `<topic>.DLT`.

---

## 7. The worker contract (things you MUST know)

- **Jobs must be idempotent.** Delivery is at-least-once and a crash *after* your job
  runs but *before* the result is reported will cause a **re-dispatch** on recovery.
  Use `job_execution_id` / natural keys to no-op on repeats.
- **Timeouts are cooperative.** If your job exceeds `timeoutSeconds`, the orchestrator
  marks it `TIMED_OUT` and may retry — but the SDK **cannot force-kill** your async
  task. For long jobs, check for cancellation at checkpoints and keep work restartable.
- **`name()` is the contract.** It must exactly match `jobName` in the flow definition,
  or the orchestrator dispatches a job no worker will run.
- **One `service_context` per worker process.** All jobs registered in a process are
  served from that one dispatch topic. Split into separate deployables for separate
  contexts.
- **Concurrency (SKIP/QUEUE/PARALLEL) is not your concern** — it's set per task in the
  flow and enforced by the orchestrator before dispatch.
- **Don't publish `job.execution.completed` yourself** — the SDK owns that.

---

## 8. Local testing

```bash
# 1. Broker + Redis (compose in the repo root)
docker compose up -d           # kafka; add a redis service if not present

# 2. Run your worker
finzly.jobs.service.context=settlement-service cargo run

# 3. Simulate a dispatch without the orchestrator: produce a JobExecutionRequested
#    (wrapped in MessageWrapper.object) to job-dispatch-settlement-service, then
#    watch the worker log RUNNING -> completed, and see job.execution.completed.
```
Unit-test a `Job` directly by constructing a `JobContext` and asserting the `JobResult`
— no Kafka needed.

---

## 9. Versioning & upgrades

- Pin an exact version (`=0.1.0`); the SDK follows the registry's semver.
- The **message contract** (`finzly-jobs-model`) is what actually couples you to the
  orchestrator. Contract changes are additive/backward-compatible; a breaking change
  bumps the model major and both sides upgrade together.
- The `Job` trait is stable API; new optional `JobContext` fields are additive.

---

## 10. Full public surface (reference)

```rust
// finzly-jobs-sdk
pub use finzly_jobs_model::{/* JobExecutionRequested, JobExecutionCompleted, ... */};

pub trait Job: Send + Sync {                 // #[async_trait]
    fn name(&self) -> &'static str;
    async fn execute(&self, ctx: &JobContext) -> JobResult;
}

pub struct JobContext { pub tenant: String, pub flow_execution_id: Uuid,
                        pub job_execution_id: Uuid, pub attempt: i32,
                        pub parameters: serde_json::Value }

pub enum JobResult { Success { output: Option<String> }, Failure { reason: String } }
impl JobResult { pub fn success(_: impl Into<String>) -> Self; pub fn failure(_: impl Into<String>) -> Self; }

pub struct JobRuntime { /* ... */ }
impl JobRuntime {
    pub fn from_config() -> Result<Self>;                       // reads finzly.jobs.service.context + infra
    pub fn new(kafka: KafkaConfig, service_context: &str) -> Self;
    pub fn register<J: Job + 'static>(&mut self, job: J) -> &mut Self;
    pub async fn start(self) -> Result<()>;                     // runs the consumer loop
}
```

---

### TL;DR for a consuming team
1. `cargo add finzly-jobs-sdk`.
2. Set `finzly.jobs.service.context` + Kafka + Redis config.
3. `impl Job` for each job (`name` = the flow's `jobName`).
4. `JobRuntime::from_config()` → `register(...)` → `start()`.
5. Make jobs idempotent. That's it — the SDK runs, heartbeats, and reports.
