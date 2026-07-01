//! Executor: performs a fired job. POC behaviour — log the fire and record a
//! `job_execution` row. Routing to real triggers (HTTP callback for API jobs,
//! Kafka publish for EVENT jobs) is future work.

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::info;

use phoenix_postgres_sdk::get_tenant_pool;

use crate::registration::{model::JobRow, repository::default_tenant};

pub struct Executor;

impl Executor {
    pub async fn execute(job: &JobRow) -> Result<(), sqlx::Error> {
        info!(
            job = %job.name,
            trigger_type = %job.trigger_type,
            event_topic = ?job.event_topic,
            callback_endpoint = ?job.callback_endpoint,
            "Job fired"
        );

        // TODO: route by trigger_type — API -> HTTP POST callback_endpoint,
        // EVENT -> publish to event_topic. For now we only record the execution.
        record_execution(job).await
    }
}

/// Insert a `job_execution` row marking a successful (log-only) fire.
async fn record_execution(job: &JobRow) -> Result<(), sqlx::Error> {
    let tenant = default_tenant();
    let pool = get_tenant_pool(&tenant)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    // Unique-per-fire id without pulling in uuid/chrono-clock: epoch nanos.
    let execution_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string());

    sqlx::query(
        r#"
        INSERT INTO demo_galaxy_jobs.job_execution
            (job_name, execution_id, status, started_at, finished_at)
        VALUES ($1, $2, 'SUCCESS', now(), now())
        "#,
    )
    .bind(job.name.as_str())
    .bind(execution_id)
    .execute(&pool)
    .await?;

    Ok(())
}
