//! Persistence for schedule registration.
//!
//! The Scheduler owns only `scheduled_trigger`. Each job in the uploaded config that
//! has a cron becomes one trigger (`target_ref` = the job/flow name to fire, opaque
//! to us; the Orchestrator resolves it). Manual jobs (no cron) get no trigger.
//! Idempotent per (tenant, target_ref); the whole upload is one transaction.

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use super::model::Config;
use crate::scheduler::schedule::next_fire_from_now_tz;

/// Default tenant, resolved the same way `init_db_pool` does.
pub fn default_tenant() -> String {
    get_config_property("bankos.application.default.tenant")
        .or_else(|| std::env::var("DEFAULT_TENANT_ID").ok())
        .unwrap_or_else(|| "finzly".to_string())
}

/// Upsert one `scheduled_trigger` per cron'd job. Returns the number of schedules
/// created/updated (manual, no-cron jobs are skipped).
pub async fn save_registration(config: &Config) -> Result<u64, sqlx::Error> {
    let tenant = default_tenant();
    let pool = get_tenant_pool(&tenant)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    let job_cfg = &config.finzly.job;
    let mut tx = pool.begin().await?;
    let mut scheduled = 0u64;

    for job in &job_cfg.jobs {
        let Some(cron) = job.cron.as_deref() else {
            continue; // manual-only job → no schedule
        };
        // First fire is the next cron occurrence (default UTC timezone for uploads).
        let next_fire_time = next_fire_from_now_tz(cron, "UTC");

        sqlx::query(
            r#"
            INSERT INTO demo_galaxy_jobs.scheduled_trigger
                (tenant_name, target_ref, target_type, cron_expression, timezone,
                 next_fire_time, created_by)
            VALUES ($1, $2, 'FLOW', $3, 'UTC', $4, $5)
            ON CONFLICT (tenant_name, target_ref) DO UPDATE SET
                cron_expression = EXCLUDED.cron_expression,
                next_fire_time  = EXCLUDED.next_fire_time,
                updated_at      = now()
            "#,
        )
        .bind(&tenant)
        .bind(job.name.as_str())
        .bind(cron)
        .bind(next_fire_time)
        .bind(job_cfg.service.as_str())
        .execute(&mut *tx)
        .await?;
        scheduled += 1;
    }

    tx.commit().await?;
    Ok(scheduled)
}
