//! UI read APIs for the Scheduler control plane: the catalog (project / job / flow /
//! schedule) and execution history. History rows are written by the Orchestrator;
//! the Scheduler only reads them here.

use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

use phoenix_postgres_sdk::get_tenant_pool;

use crate::registration::repository::default_tenant;

/// Routes owned by the read API.
pub fn router() -> Router {
    Router::new()
        .route("/projects", get(list_projects))
        .route("/jobs", get(list_jobs))
        .route("/flows", get(list_flows))
        .route("/schedules", get(list_schedules))
        .route("/history/flows", get(list_flow_executions))
        .route("/history/jobs", get(list_job_executions))
}

/// `?limit=` for the history endpoints (default 100).
#[derive(Debug, Deserialize)]
struct ListParams {
    limit: Option<i64>,
}

async fn pool() -> Result<sqlx::PgPool, (StatusCode, String)> {
    get_tenant_pool(&default_tenant())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("query failed: {e}"))
}

// ---- row types ----

#[derive(Debug, Serialize, FromRow)]
struct Project {
    id: Uuid,
    name: String,
    created_by: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, FromRow)]
struct JobDefinition {
    id: Uuid,
    project_id: Uuid,
    name: String,
    description: Option<String>,
    job_type: String,
    service_context: String,
    parameters: Option<Value>,
    retry_policy: Option<Value>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, FromRow)]
struct FlowDefinition {
    id: Uuid,
    name: String,
    description: Option<String>,
    is_enabled: bool,
    version: i32,
    flow: Value,
    sla_policy: Option<Value>,
    created_by: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, FromRow)]
struct ScheduledTrigger {
    id: Uuid,
    tenant_name: String,
    target_ref: String,
    target_type: String,
    cron_expression: String,
    timezone: String,
    enabled: bool,
    misfire_policy: String,
    skip_on_holiday: bool,
    next_fire_time: Option<NaiveDateTime>,
    last_fire_time: Option<NaiveDateTime>,
    created_by: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, FromRow)]
struct FlowExecution {
    id: Uuid,
    flow_definition_id: Option<Uuid>,
    flow_name: Option<String>,
    flow_version: Option<i32>,
    tenant_name: Option<String>,
    idempotency_key: Option<String>,
    status: String,
    started_at: Option<NaiveDateTime>,
    ended_at: Option<NaiveDateTime>,
    error_message: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, FromRow)]
struct JobExecution {
    id: Uuid,
    flow_execution_id: Uuid,
    job_name: String,
    status: String,
    output: Option<String>,
    attempt: i32,
    started_at: Option<NaiveDateTime>,
    ended_at: Option<NaiveDateTime>,
    error_message: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

// ---- handlers ----

async fn list_projects() -> Result<Json<Vec<Project>>, (StatusCode, String)> {
    let pool = pool().await?;
    let rows = sqlx::query_as::<_, Project>(
        "SELECT id, name, created_by, created_at, updated_at \
         FROM demo_galaxy_jobs.project ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .map_err(db_err)?;
    Ok(Json(rows))
}

async fn list_jobs() -> Result<Json<Vec<JobDefinition>>, (StatusCode, String)> {
    let pool = pool().await?;
    let rows = sqlx::query_as::<_, JobDefinition>(
        "SELECT id, project_id, name, description, job_type, service_context, \
                parameters, retry_policy, created_at, updated_at \
         FROM demo_galaxy_jobs.job_definition ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .map_err(db_err)?;
    Ok(Json(rows))
}

async fn list_flows() -> Result<Json<Vec<FlowDefinition>>, (StatusCode, String)> {
    let pool = pool().await?;
    let rows = sqlx::query_as::<_, FlowDefinition>(
        "SELECT id, name, description, is_enabled, version, flow, sla_policy, \
                created_by, created_at, updated_at \
         FROM demo_galaxy_jobs.flow_definition ORDER BY name, version",
    )
    .fetch_all(&pool)
    .await
    .map_err(db_err)?;
    Ok(Json(rows))
}

async fn list_schedules() -> Result<Json<Vec<ScheduledTrigger>>, (StatusCode, String)> {
    let pool = pool().await?;
    let rows = sqlx::query_as::<_, ScheduledTrigger>(
        "SELECT id, tenant_name, target_ref, target_type, cron_expression, timezone, \
                enabled, misfire_policy, skip_on_holiday, next_fire_time, last_fire_time, \
                created_by, created_at, updated_at \
         FROM demo_galaxy_jobs.scheduled_trigger ORDER BY next_fire_time NULLS LAST",
    )
    .fetch_all(&pool)
    .await
    .map_err(db_err)?;
    Ok(Json(rows))
}

async fn list_flow_executions(
    Query(p): Query<ListParams>,
) -> Result<Json<Vec<FlowExecution>>, (StatusCode, String)> {
    let pool = pool().await?;
    let rows = sqlx::query_as::<_, FlowExecution>(
        "SELECT id, flow_definition_id, flow_name, flow_version, tenant_name, \
                idempotency_key, status, started_at, ended_at, error_message, \
                created_at, updated_at \
         FROM demo_galaxy_jobs.flow_execution ORDER BY created_at DESC LIMIT $1",
    )
    .bind(p.limit.unwrap_or(100))
    .fetch_all(&pool)
    .await
    .map_err(db_err)?;
    Ok(Json(rows))
}

async fn list_job_executions(
    Query(p): Query<ListParams>,
) -> Result<Json<Vec<JobExecution>>, (StatusCode, String)> {
    let pool = pool().await?;
    let rows = sqlx::query_as::<_, JobExecution>(
        "SELECT id, flow_execution_id, job_name, status, output, attempt, \
                started_at, ended_at, error_message, created_at, updated_at \
         FROM demo_galaxy_jobs.job_execution ORDER BY created_at DESC LIMIT $1",
    )
    .bind(p.limit.unwrap_or(100))
    .fetch_all(&pool)
    .await
    .map_err(db_err)?;
    Ok(Json(rows))
}
