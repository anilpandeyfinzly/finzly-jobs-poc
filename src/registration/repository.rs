//! Persistence layer for job registrations.

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use super::model::Config;

/// Default tenant, resolved the same way `init_db_pool` does.
pub fn default_tenant() -> String {
    get_config_property("bankos.application.default.tenant")
        .or_else(|| std::env::var("DEFAULT_TENANT_ID").ok())
        .unwrap_or_else(|| "finzly".to_string())
}

/// Upsert every job in the parsed config into `job_registration`. Idempotent on
/// `name` so re-registering the same service updates the existing rows. All jobs
/// are written in a single transaction.
pub async fn save_registration(config: &Config) -> Result<u64, sqlx::Error> {
    let tenant = default_tenant();
    let pool = get_tenant_pool(&tenant)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    let contract_version = &config.finzly.job.contract_version;
    let jobs = &config.finzly.job.jobs;

    let mut tx = pool.begin().await?;
    for job in jobs {
        // Cron-driven schedule: the first fire is the next cron occurrence.
        // interval_seconds is only the status-report cadence, not the schedule.
        let next_fire_time = job
            .cron
            .as_deref()
            .and_then(crate::orchestrator::schedule::next_fire_from_now);

        sqlx::query(
            r#"
            INSERT INTO job_registration
                (name, cron, trigger_type, callback_bean, callback_endpoint, event_topic,
                 status_report_mode, interval_seconds, sla_seconds, timeout_seconds,
                 max_retries, contract_version, next_fire_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (name) DO UPDATE SET
                cron               = EXCLUDED.cron,
                trigger_type       = EXCLUDED.trigger_type,
                callback_bean      = EXCLUDED.callback_bean,
                callback_endpoint  = EXCLUDED.callback_endpoint,
                event_topic        = EXCLUDED.event_topic,
                status_report_mode = EXCLUDED.status_report_mode,
                interval_seconds   = EXCLUDED.interval_seconds,
                sla_seconds        = EXCLUDED.sla_seconds,
                timeout_seconds    = EXCLUDED.timeout_seconds,
                max_retries        = EXCLUDED.max_retries,
                contract_version   = EXCLUDED.contract_version,
                next_fire_time     = EXCLUDED.next_fire_time,
                updated_at         = now()
            "#,
        )
        .bind(job.name.as_str())
        .bind(job.cron.as_deref())
        .bind(job.trigger_type.as_str())
        .bind(job.callback_bean.as_str())
        .bind(job.callback_endpoint.as_deref())
        .bind(job.event_topic.as_deref())
        .bind(job.status_report.mode.as_str())
        .bind(job.status_report.interval_seconds.map(|v| v as i32))
        .bind(job.sla_seconds as i32)
        .bind(job.timeout_seconds as i32)
        .bind(job.max_retries as i32)
        .bind(contract_version.as_str())
        .bind(next_fire_time)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(jobs.len() as u64)
}
