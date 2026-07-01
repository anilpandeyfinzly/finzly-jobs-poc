//! finzly-jobs-poc — proof of concept for the new finzly-job-service.
//!
//! Boots like the real Phoenix services: load config, init the DB pool, run
//! migrations, then serve an axum app. Feature logic lives in its own modules
//! (see `registration`).

mod registration;
mod orchestrator;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::get, Router};
use tracing::info;

use phoenix_config_sdk::config_properties::get_configs;
use phoenix_postgres_sdk::init_db_pool;
use phoenix_postgres_sdk::tenant_config::run_migration;

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

    // 5. Start the background job orchestrator (scheduler -> dispatcher -> executor).
    orchestrator::init_orchestrator();

    // 6. Axum server — feature routers are merged in here.
    let app = Router::new()
        .route("/ping", get(ping))
        .merge(registration::router());

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
