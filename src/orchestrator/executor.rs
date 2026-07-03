//! Executor: performs a fired trigger. POC behaviour — log the fire, open a
//! `flow_execution`, and record a `job_execution` row per job in the flow.
//! Routing to real triggers (HTTP callback for API jobs, Kafka publish for EVENT
//! jobs) is future work; here every job is marked SUCCESS (log-only).

use tracing::info;
use uuid::Uuid;

use phoenix_postgres_sdk::get_tenant_pool;

use crate::registration::model::ClaimedTrigger;

pub struct Executor;

impl Executor {
    pub async fn execute(trigger: &ClaimedTrigger) -> Result<(), sqlx::Error> {
        let job_names = trigger.job_names();
        info!(
            flow = %trigger.flow_name,
            flow_version = trigger.flow_version,
            scheduled_fire_time = %trigger.scheduled_fire_time,
            jobs = ?job_names,
            "Flow fired"
        );

        record_execution(trigger, &job_names).await
    }
}

/// Open a `flow_execution` for this fire (idempotent on the per-fire key) and
/// record one `job_execution` per job, then mark the flow SUCCESS. If the fire
/// was already recorded (idempotency conflict), this is a no-op.
async fn record_execution(
    trigger: &ClaimedTrigger,
    job_names: &[String],
) -> Result<(), sqlx::Error> {
    let pool = get_tenant_pool(&trigger.tenant_name)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    let mut tx = pool.begin().await?;

    // One flow_execution per fire. ON CONFLICT (idempotency_key) DO NOTHING means
    // a re-delivered fire returns no row, and we skip it.
    let flow_execution_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        INSERT INTO demo_galaxy_jobs.flow_execution
            (flow_definition_id, flow_version, scheduled_fire_time, idempotency_key,
             status, started_at)
        VALUES ($1, $2, $3, $4, 'IN_PROGRESS', now())
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(trigger.flow_definition_id)
    .bind(trigger.flow_version)
    .bind(trigger.scheduled_fire_time)
    .bind(trigger.idempotency_key())
    .fetch_optional(&mut *tx)
    .await?;

    let Some(flow_execution_id) = flow_execution_id else {
        info!(
            flow = %trigger.flow_name,
            idempotency_key = %trigger.idempotency_key(),
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
