//! Persistence layer for job registrations.
//!
//! Maps the finzly `job-config.yml` contract onto the flow schema (0003):
//!   - the service becomes a `project`
//!   - each job becomes a `job_definition` (under that project) plus a single-job
//!     `flow_definition`
//!   - each job with a cron gets a `scheduled_trigger` that fires its flow
//! Manual jobs (no cron) get no trigger, so they never auto-fire.

use serde_json::json;
use uuid::Uuid;

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use super::model::{Config, Job};

/// Default tenant, resolved the same way `init_db_pool` does.
pub fn default_tenant() -> String {
    get_config_property("bankos.application.default.tenant")
        .or_else(|| std::env::var("DEFAULT_TENANT_ID").ok())
        .unwrap_or_else(|| "finzly".to_string())
}

/// Upsert the parsed config into the flow schema. Idempotent on the service name
/// (project), each job name (job_definition + flow_definition), and each trigger
/// (per tenant + flow). The whole upload is written in one transaction.
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
        // 2. job_definition — the reusable unit of work. The transport-only
        //    contract fields live in parameters; retry policy is its own column.
        let parameters = json!({
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
                (project_id, name, job_type, target_service, parameters, retry_policy)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (project_id, name) DO UPDATE SET
                job_type       = EXCLUDED.job_type,
                target_service = EXCLUDED.target_service,
                parameters     = EXCLUDED.parameters,
                retry_policy   = EXCLUDED.retry_policy,
                updated_at     = now()
            "#,
        )
        .bind(project_id)
        .bind(job.name.as_str())
        .bind(job.trigger_type.as_str())
        .bind(job_cfg.service_base_url.as_str())
        .bind(&parameters)
        .bind(&retry_policy)
        .execute(&mut *tx)
        .await?;

        // 3. flow_definition — a single-job flow that runs this job. version is
        //    pinned to 1 for the POC (re-uploads update the v1 row).
        let flow = json!({ "jobs": [job.name] });
        let sla_policy = json!({
            "slaSeconds": job.sla_seconds,
            "timeoutSeconds": job.timeout_seconds,
        });
        let flow_definition_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO demo_galaxy_jobs.flow_definition
                (name, version, flow, sla_policy, created_by)
            VALUES ($1, 1, $2, $3, $4)
            ON CONFLICT (name, version) DO UPDATE SET
                flow       = EXCLUDED.flow,
                sla_policy = EXCLUDED.sla_policy,
                updated_at = now()
            RETURNING id
            "#,
        )
        .bind(job.name.as_str())
        .bind(&flow)
        .bind(&sla_policy)
        .bind(job_cfg.service.as_str())
        .fetch_one(&mut *tx)
        .await?;

        // 4. scheduled_trigger — only for jobs with a cron. next_fire_time is the
        //    next cron occurrence; manual jobs get no trigger so never auto-fire.
        upsert_trigger(&mut tx, &tenant, flow_definition_id, job).await?;
    }

    tx.commit().await?;
    Ok(jobs.len() as u64)
}

/// Upsert the cron trigger for a job's flow. No-op when the job has no cron.
async fn upsert_trigger(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant: &str,
    flow_definition_id: Uuid,
    job: &Job,
) -> Result<(), sqlx::Error> {
    let Some(cron) = job.cron.as_deref() else {
        return Ok(());
    };
    // Cron-driven schedule: the first fire is the next cron occurrence.
    let next_fire_time = crate::orchestrator::schedule::next_fire_from_now(cron)
        .map(|dt| dt.naive_utc());

    sqlx::query(
        r#"
        INSERT INTO demo_galaxy_jobs.scheduled_trigger
            (tenant_name, flow_definition_id, cron_expression, next_fire_time, created_by)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (tenant_name, flow_definition_id) DO UPDATE SET
            cron_expression = EXCLUDED.cron_expression,
            next_fire_time  = EXCLUDED.next_fire_time,
            updated_at      = now()
        "#,
    )
    .bind(tenant)
    .bind(flow_definition_id)
    .bind(cron)
    .bind(next_fire_time)
    .bind(job.name.as_str())
    .execute(&mut **tx)
    .await?;

    Ok(())
}
