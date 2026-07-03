//! Completion handler: consumes `job.execution.completed`. Updates the
//! job_execution, releases its execution_lock, and — when no jobs of the flow
//! remain in flight — closes out the flow_execution (SUCCESS if all jobs
//! succeeded, else FAILED).

use tracing::{info, warn};
use uuid::Uuid;

use phoenix_postgres_sdk::get_tenant_pool;

use super::messages::JobExecutionCompleted;

/// Apply a job completion and advance the flow if it's now done.
pub async fn handle_job_completed(msg: &JobExecutionCompleted) -> Result<(), sqlx::Error> {
    let pool = get_tenant_pool(&msg.tenant_name)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    let mut tx = pool.begin().await?;

    // 1. Record the job's terminal status.
    sqlx::query(
        r#"
        UPDATE demo_galaxy_jobs.job_execution
        SET status = $2, output = $3, error_message = $4, ended_at = now(), updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(msg.job_execution_id)
    .bind(&msg.status)
    .bind(msg.output.as_deref())
    .bind(msg.error_message.as_deref())
    .execute(&mut *tx)
    .await?;

    // 2. Release the concurrency lock (no-op if this job didn't hold one).
    sqlx::query("DELETE FROM demo_galaxy_jobs.execution_lock WHERE job_execution_id = $1")
        .bind(msg.job_execution_id)
        .execute(&mut *tx)
        .await?;

    // 3. Any jobs still in flight for this flow?
    let in_flight: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM demo_galaxy_jobs.job_execution \
         WHERE flow_execution_id = $1 AND status IN ('QUEUED', 'RUNNING')",
    )
    .bind(msg.flow_execution_id)
    .fetch_one(&mut *tx)
    .await?;

    info!(job = %msg.job_name, status = %msg.status, in_flight, "Job completed");

    if in_flight == 0 {
        complete_flow(&mut tx, msg.flow_execution_id).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Close a flow_execution: FAILED if any job failed, else SUCCESS.
async fn complete_flow(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    flow_execution_id: Uuid,
) -> Result<(), sqlx::Error> {
    let failed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM demo_galaxy_jobs.job_execution \
         WHERE flow_execution_id = $1 AND status = 'FAILED'",
    )
    .bind(flow_execution_id)
    .fetch_one(&mut **tx)
    .await?;

    let flow_status = if failed > 0 { "FAILED" } else { "SUCCESS" };
    sqlx::query(
        "UPDATE demo_galaxy_jobs.flow_execution \
         SET status = $2, ended_at = now(), updated_at = now() WHERE id = $1",
    )
    .bind(flow_execution_id)
    .bind(flow_status)
    .execute(&mut **tx)
    .await?;

    if failed > 0 {
        warn!(%flow_execution_id, "Flow completed with FAILED jobs");
    } else {
        info!(%flow_execution_id, "Flow completed SUCCESS");
    }
    Ok(())
}
