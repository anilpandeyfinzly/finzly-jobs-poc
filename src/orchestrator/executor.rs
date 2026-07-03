//! Executor / flow dispatcher: consumes `flow.execution.requested`, opens a
//! `flow_execution`, then dispatches each job in the flow to the worker over
//! `job.execution.requested`, honouring the job's concurrency policy:
//!
//!   - SKIP     : acquire an `execution_lock`; if one is already held, mark the
//!                job SKIPPED and don't dispatch.
//!   - QUEUE    : dispatch ordered (FIFO per job name) so the worker runs one at a time.
//!   - PARALLEL : dispatch immediately, no lock.
//!
//! The worker later publishes `job.execution.completed`, handled in `completion`.

use serde_json::Value as JsonValue;
use tracing::{info, warn};
use uuid::Uuid;

use phoenix_postgres_sdk::get_tenant_pool;

use super::messages::{ConcurrencyPolicy, FlowExecutionRequested, JobExecutionRequested};
use crate::kafka;

pub struct Executor;

impl Executor {
    /// Run a requested flow: open a flow_execution (idempotent per fire) and
    /// dispatch each of its jobs.
    pub async fn run_flow(msg: &FlowExecutionRequested) -> Result<(), sqlx::Error> {
        let pool = get_tenant_pool(&msg.tenant_name)
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        let row: Option<(i32, JsonValue, String)> = sqlx::query_as(
            "SELECT version, flow, name FROM demo_galaxy_jobs.flow_definition WHERE id = $1",
        )
        .bind(msg.flow_definition_id)
        .fetch_optional(&pool)
        .await?;

        let Some((flow_version, flow, flow_name)) = row else {
            info!(flow_definition_id = %msg.flow_definition_id,
                  "flow.execution.requested for unknown flow definition; skipping");
            return Ok(());
        };

        let job_names = job_names(&flow);
        info!(flow = %flow_name, flow_version, jobs = ?job_names, "Flow requested");

        // Open the flow_execution once per fire; DO NOTHING on re-delivery.
        let flow_execution_id: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO demo_galaxy_jobs.flow_execution
                (flow_definition_id, flow_version, idempotency_key, status, started_at)
            VALUES ($1, $2, $3, 'RUNNING', now())
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(msg.flow_definition_id)
        .bind(flow_version)
        .bind(msg.idempotency_key())
        .fetch_optional(&pool)
        .await?;

        let Some(flow_execution_id) = flow_execution_id else {
            info!(idempotency_key = %msg.idempotency_key(),
                  "Fire already recorded; skipping (idempotent)");
            return Ok(());
        };

        for job_name in &job_names {
            dispatch_job(&pool, &msg.tenant_name, flow_execution_id, job_name).await?;
        }
        Ok(())
    }
}

/// Job names declared in the flow JSON (`{"jobs": [...]}`); empty if malformed.
fn job_names(flow: &JsonValue) -> Vec<String> {
    flow.get("jobs")
        .and_then(JsonValue::as_array)
        .map(|jobs| jobs.iter().filter_map(|j| j.as_str().map(str::to_owned)).collect())
        .unwrap_or_default()
}

/// Dispatch a single job per its concurrency policy. Records the job_execution row
/// (and, for SKIP, an execution_lock) inside a transaction, then publishes the
/// dispatch message after commit.
async fn dispatch_job(
    pool: &sqlx::PgPool,
    tenant: &str,
    flow_execution_id: Uuid,
    job_name: &str,
) -> Result<(), sqlx::Error> {
    // Resolve the job definition for its service context + concurrency policy.
    let def: Option<(String, JsonValue)> = sqlx::query_as(
        "SELECT service_context, parameters FROM demo_galaxy_jobs.job_definition WHERE name = $1 LIMIT 1",
    )
    .bind(job_name)
    .fetch_optional(pool)
    .await?;

    let Some((service_context, parameters)) = def else {
        warn!(job = %job_name, "No job_definition; cannot dispatch");
        return Ok(());
    };
    let policy = ConcurrencyPolicy::parse(
        parameters.get("concurrency").and_then(JsonValue::as_str).unwrap_or("SKIP"),
    );

    let mut tx = pool.begin().await?;

    // Provisional status: QUEUE waits in the FIFO queue; others start RUNNING.
    let status = if policy == ConcurrencyPolicy::Queue { "QUEUED" } else { "RUNNING" };
    let job_execution_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO demo_galaxy_jobs.job_execution
            (flow_execution_id, job_name, status, attempt, started_at)
        VALUES ($1, $2, $3, 1, CASE WHEN $3 = 'RUNNING' THEN now() ELSE NULL END)
        RETURNING id
        "#,
    )
    .bind(flow_execution_id)
    .bind(job_name)
    .bind(status)
    .fetch_one(&mut *tx)
    .await?;

    // SKIP: acquire the lock; if one is already held, skip this dispatch.
    if policy == ConcurrencyPolicy::Skip {
        let acquired: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO demo_galaxy_jobs.execution_lock
                (tenant_name, service_context, job_name, flow_execution_id, job_execution_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_name, service_context, job_name) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(tenant)
        .bind(&service_context)
        .bind(job_name)
        .bind(flow_execution_id)
        .bind(job_execution_id)
        .fetch_optional(&mut *tx)
        .await?;

        if acquired.is_none() {
            sqlx::query(
                "UPDATE demo_galaxy_jobs.job_execution \
                 SET status = 'SKIPPED', ended_at = now(), updated_at = now() WHERE id = $1",
            )
            .bind(job_execution_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            info!(job = %job_name, "Job already running (SKIP policy); skipped");
            return Ok(());
        }
    }

    tx.commit().await?;

    // Publish the dispatch. QUEUE is ordered (FIFO per job name); others are not.
    let ordered = policy == ConcurrencyPolicy::Queue;
    let request = JobExecutionRequested {
        flow_execution_id,
        job_execution_id,
        tenant_name: tenant.to_string(),
        job_name: job_name.to_string(),
        service_context,
        attempt: 1,
    };
    if let Err(e) =
        kafka::publish(&kafka::topic_job_requested(), job_name, tenant, ordered, &request).await
    {
        warn!(job = %job_name, error = %e, "Failed to publish job.execution.requested");
    }
    Ok(())
}
