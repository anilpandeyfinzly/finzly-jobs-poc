//! Persistence for the catalog + schedules (the Scheduler control plane).
//!
//! Maps the finzly `job-config.yml` contract onto the catalog:
//!   - the service becomes a `project`
//!   - each job becomes a `job_definition` (under that project) plus a single-job
//!     `flow_definition`
//!   - each job with a cron gets a `scheduled_trigger` (target_ref = the flow name)
//! Manual jobs (no cron) get no trigger. Idempotent; one transaction per upload.

use serde_json::json;
use uuid::Uuid;

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use super::model::{Config, Job};
use crate::scheduler::schedule::next_fire_from_now_tz;

/// Default tenant, resolved the same way `init_db_pool` does.
pub fn default_tenant() -> String {
    get_config_property("bankos.application.default.tenant")
        .or_else(|| std::env::var("DEFAULT_TENANT_ID").ok())
        .unwrap_or_else(|| "finzly".to_string())
}

/// Upsert the parsed config into the catalog: project, job_definition,
/// flow_definition, and a scheduled_trigger per cron'd job. Returns the job count.
pub async fn save_registration(config: &Config) -> Result<u64, sqlx::Error> {
    let tenant = default_tenant();
    let pool = get_tenant_pool(&tenant)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    let job_cfg = &config.finzly.job;
    let jobs = &job_cfg.jobs;

    let mut tx = pool.begin().await?;

    // 1. project — owner of this service's job definitions.
    let project_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO demo_galaxy_jobs.project (name, created_by)
        VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE SET updated_at = now()
        RETURNING id
        "#,
    )
    .bind(job_cfg.service.as_str())
    .bind(job_cfg.service.as_str())
    .fetch_one(&mut *tx)
    .await?;

    for job in jobs {
        // 2. job_definition — the reusable unit of work. Transport-only contract
        //    fields live in parameters; retry policy is its own column.
        let parameters = json!({
            "concurrency": "SKIP",
            "serviceBaseUrl": job_cfg.service_base_url,
            "callbackBean": job.callback_bean,
            "callbackEndpoint": job.callback_endpoint,
            "eventTopic": job.event_topic,
            "statusReport": {
                "mode": job.status_report.mode.as_str(),
                "intervalSeconds": job.status_report.interval_seconds,
            },
            "slaSeconds": job.sla_seconds,
            "timeoutSeconds": job.timeout_seconds,
            "contractVersion": job_cfg.contract_version,
        });
        let retry_policy = json!({ "maxRetries": job.max_retries });

        sqlx::query(
            r#"
            INSERT INTO demo_galaxy_jobs.job_definition
                (project_id, name, job_type, service_context, parameters, retry_policy)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (project_id, name) DO UPDATE SET
                job_type        = EXCLUDED.job_type,
                service_context = EXCLUDED.service_context,
                parameters      = EXCLUDED.parameters,
                retry_policy    = EXCLUDED.retry_policy,
                updated_at      = now()
            "#,
        )
        .bind(project_id)
        .bind(job.name.as_str())
        .bind(job.trigger_type.as_str())
        .bind(job_cfg.service.as_str())
        .bind(&parameters)
        .bind(&retry_policy)
        .execute(&mut *tx)
        .await?;

        // 3. flow_definition — a single-job flow that runs this job (version 1).
        let flow = json!({ "jobs": [job.name] });
        let sla_policy = json!({
            "slaSeconds": job.sla_seconds,
            "timeoutSeconds": job.timeout_seconds,
        });
        sqlx::query(
            r#"
            INSERT INTO demo_galaxy_jobs.flow_definition
                (name, version, flow, sla_policy, created_by)
            VALUES ($1, 1, $2, $3, $4)
            ON CONFLICT (name, version) DO UPDATE SET
                flow       = EXCLUDED.flow,
                sla_policy = EXCLUDED.sla_policy,
                updated_at = now()
            "#,
        )
        .bind(job.name.as_str())
        .bind(&flow)
        .bind(&sla_policy)
        .bind(job_cfg.service.as_str())
        .execute(&mut *tx)
        .await?;

        // 4. scheduled_trigger — only for jobs with a cron. target_ref = flow name.
        upsert_trigger(&mut tx, &tenant, job.name.as_str(), job).await?;
    }

    tx.commit().await?;
    Ok(jobs.len() as u64)
}

/// Upsert the cron trigger for a job's flow. No-op when the job has no cron.
async fn upsert_trigger(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
    target_ref: &str,
    job: &Job,
) -> Result<(), sqlx::Error> {
    let Some(cron) = job.cron.as_deref() else {
        return Ok(());
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
    .bind(tenant)
    .bind(target_ref)
    .bind(cron)
    .bind(next_fire_time)
    .bind(job.name.as_str())
    .execute(&mut **tx)
    .await?;

    Ok(())
}
