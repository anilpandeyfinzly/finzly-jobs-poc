//! Executor: runs a requested flow. Consumes `flow.execution.requested`, loads the
//! flow definition, opens a `flow_execution`, and records a `job_execution` per job.
//!
//! POC behaviour is log-only (every job is marked SUCCESS inline). The split into
//! per-job `job.execution.requested` / `job.execution.completed` dispatch with
//! concurrency policies is layered on in the dispatcher (stage 3).

use serde_json::Value as JsonValue;
use tracing::info;
use uuid::Uuid;

use phoenix_postgres_sdk::get_tenant_pool;

use super::messages::FlowExecutionRequested;

pub struct Executor;

impl Executor {
    /// Run a flow requested over Kafka: load its definition, open a flow_execution
    /// (idempotent per fire), and record each job.
    pub async fn run_flow(msg: &FlowExecutionRequested) -> Result<(), sqlx::Error> {
        let pool = get_tenant_pool(&msg.tenant_name)
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        // Load the flow definition to get its version and DAG.
        let row: Option<(i32, JsonValue, String)> = sqlx::query_as(
            r#"
            SELECT version, flow, name
            FROM demo_galaxy_jobs.flow_definition
            WHERE id = $1
            "#,
        )
        .bind(msg.flow_definition_id)
        .fetch_optional(&pool)
        .await?;

        let Some((flow_version, flow, flow_name)) = row else {
            info!(
                flow_definition_id = %msg.flow_definition_id,
                "flow.execution.requested for unknown flow definition; skipping"
            );
            return Ok(());
        };

        let job_names = job_names(&flow);
        info!(
            flow = %flow_name,
            flow_version,
            schedule_fire_time = %msg.schedule_fire_time,
            jobs = ?job_names,
            "Flow requested"
        );

        record_execution(&pool, msg, flow_version, &job_names).await
    }
}

/// Job names declared in the flow JSON (`{"jobs": [...]}`); empty if malformed.
fn job_names(flow: &JsonValue) -> Vec<String> {
    flow.get("jobs")
        .and_then(JsonValue::as_array)
        .map(|jobs| {
            jobs.iter()
                .filter_map(|j| j.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Open a `flow_execution` for this fire (idempotent on the per-fire key), record
/// one `job_execution` per job, then mark the flow SUCCESS. If the fire was already
/// recorded (idempotency conflict), this is a no-op.
async fn record_execution(
    pool: &sqlx::PgPool,
    msg: &FlowExecutionRequested,
    flow_version: i32,
    job_names: &[String],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let flow_execution_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        INSERT INTO demo_galaxy_jobs.flow_execution
            (flow_definition_id, flow_version, idempotency_key, status, started_at)
        VALUES ($1, $2, $3, 'IN_PROGRESS', now())
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(msg.flow_definition_id)
    .bind(flow_version)
    .bind(msg.idempotency_key())
    .fetch_optional(&mut *tx)
    .await?;

    let Some(flow_execution_id) = flow_execution_id else {
        info!(
            idempotency_key = %msg.idempotency_key(),
            "Fire already recorded; skipping (idempotent)"
        );
        tx.commit().await?;
        return Ok(());
    };

    // POC: log-only run of each job in the flow, all SUCCESS.
    for job_name in job_names {
        sqlx::query(
            r#"
            INSERT INTO demo_galaxy_jobs.job_execution
                (flow_execution_id, job_name, status, attempt, started_at, ended_at)
            VALUES ($1, $2, 'SUCCESS', 1, now(), now())
            "#,
        )
        .bind(flow_execution_id)
        .bind(job_name.as_str())
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        UPDATE demo_galaxy_jobs.flow_execution
        SET status = 'SUCCESS', ended_at = now(), updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(flow_execution_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}
