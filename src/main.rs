//! finzly-jobs-poc — proof of concept for the new finzly-job-service.
//!
//! Boots like the real Phoenix services: load config, init the DB pool, run
//! migrations, start Kafka (consumer + producer), then serve a minimal Axum
//! app. Startup ordering mirrors phoenix-fedwire-workflow-processor-service
//! and phoenix-workflow-service.

mod job;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::Multipart;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Router, routing::get};
use tracing::info;

use phoenix_config_sdk::config_properties::{get_config_property, get_configs};
use phoenix_postgres_sdk::tenant_config::run_migration;
use phoenix_postgres_sdk::{get_tenant_pool, init_db_pool};

// use crate::kafka::{build_publisher, start_jobs_consumer};

const SERVICE_NAME: &str = "finzly-jobs-poc";

fn main() -> Result<(), String> {
    // Load .env early so config + SDKs can read local overrides.
    let _ = dotenvy::dotenv();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to build tokio runtime: {e}"))?;
    runtime.block_on(async_main())
}

async fn async_main() -> Result<(), String> {
    // 1. Config first — everything else reads from it.
    if let Err(e) = get_configs().await {
        println!("Failed to fetch configuration: error={e}");
    }

    // 2. Tracing.
    tracing_subscriber::fmt::init();
    info!("Starting {SERVICE_NAME} service");

    // 3. Database connection pool (created once, reused across the app).
    init_db_pool()
        .await
        .map_err(|e| format!("Failed to initialize DB pool: {e}"))?;
    info!("Database connection pool initialized successfully");

    // 4. Migrations — runner lives in phoenix-postgres-sdk; SQL is embedded
    //    from this crate's ./migrations folder. Gated like the reference services.
    if migration_enabled() {
        info!("Starting database migrations");
        let migrator = Arc::new(sqlx::migrate!("./migrations"));
        run_migration(migrator)
            .await
            .map_err(|e| format!("Migration failed: {e}"))?;
        info!("Database migrations completed");
    } else {
        info!("Migrations disabled (spring.liquibase.enabled != true); skipping");
    }
    
    // 7. Minimal Axum server.
    let app = Router::new()
        .route("/ping", get(ping))
        .route("/job/registrations", post(register_job));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("Failed to bind to {addr}: {e}"))?;
    info!("Server listening on http://{addr}");

    axum::serve(listener, app)
        .await
        .map_err(|e| format!("Server error: {e}"))?;

    info!("Server shutdown complete");
    Ok(())
}

/// GET /ping — liveness check.
async fn ping() -> &'static str {
    "pong"
}

/// Whether migrations should run, from `spring.liquibase.enabled` (default false).
fn migration_enabled() -> bool {
    // get_config_property("spring.liquibase.enabled")
    //     .map(|v| {
    //         let v = v.trim().to_ascii_lowercase();
    //         v == "true" || v == "1"
    //     })
    //     .unwrap_or(false)
    false
}

fn read_yaml_config(job_yaml: &str) -> serde_yaml::Result<job::Config> {
    serde_yaml::from_str::<job::Config>(job_yaml)
}

async fn register_job(mut multipart: Multipart) -> Result<impl IntoResponse, (StatusCode, String)> {
    let field = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .ok_or((StatusCode::BAD_REQUEST, "No file uploaded".to_string()))?;
    if let Some(file_name) = field.file_name() {
        println!("Uploaded file: {file_name}");
        if !(file_name.ends_with(".yml") || file_name.ends_with(".yaml")) {
            return Err((
                StatusCode::BAD_REQUEST,
                "Only .yml or .yaml files are supported".to_string(),
            ));
        }
    }

    let yaml = field
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // Parse the YAML
    let config = read_yaml_config(&yaml)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid YAML: {e}")))?;

    // Save registration to PostgreSQL (validation / event publishing still TODO).
    let count = save_registration(&config)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("DB insert failed: {e}")))?;

    println!(
        "Registered {count} job(s) for service {}",
        config.finzly.job.service
    );

    Ok((StatusCode::CREATED, format!("Registered {count} job(s)")))
}

/// Default tenant, resolved the same way `init_db_pool` does.
fn default_tenant() -> String {
    get_config_property("bankos.application.default.tenant")
        .or_else(|| std::env::var("DEFAULT_TENANT_ID").ok())
        .unwrap_or_else(|| "finzly".to_string())
}

/// Upsert every job in the parsed config into `job_registration`. Idempotent on
/// `name` so re-registering the same service updates the existing rows. All jobs
/// are written in a single transaction.
async fn save_registration(config: &job::Config) -> Result<u64, sqlx::Error> {
    let tenant = default_tenant();
    let pool = get_tenant_pool(&tenant)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    let contract_version = &config.finzly.job.contract_version;
    let jobs = &config.finzly.job.jobs;

    let mut tx = pool.begin().await?;
    for job in jobs {
        sqlx::query(
            r#"
            INSERT INTO job_registration
                (name, cron, trigger_type, callback_bean, callback_endpoint, event_topic,
                 status_report_mode, interval_seconds, sla_seconds, timeout_seconds,
                 max_retries, contract_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(jobs.len() as u64)
}

#[cfg(test)]
mod tests {
    use tokio::fs::read_to_string;

    use super::*;

    #[tokio::test]
    async fn yml_test() {
        let read_data = read_to_string("../job-config.yml").await.unwrap();
        let read_yml = read_yaml_config(read_data.as_str());

        assert!(read_yml.is_ok());
        let yml_data = read_yml.unwrap();

        println!("test:: {:?}", yml_data);
        assert_eq!(yml_data.finzly.job.service, "settlement-service");
    }
}
