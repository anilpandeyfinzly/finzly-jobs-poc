//! Kafka transport: a process-wide publisher plus the topic names used by the
//! orchestration pipeline. Mirrors how the Phoenix services use phoenix-kafka-sdk
//! (`Publisher::send_message` / `Consumer`), so this POC can later run against the
//! same brokers with no code change.
//!
//! Three topics carry the pipeline (see docs/DESIGN):
//!   - flow.execution.requested  : scheduler -> orchestrator (a due flow should run)
//!   - job.execution.requested   : orchestrator -> worker    (dispatch one job)
//!   - job.execution.completed   : worker -> orchestrator     (job finished)

use std::sync::OnceLock;

use serde::Serialize;
use tracing::debug;

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_kafka_sdk::{KafkaConfig, Publisher};
use phoenix_security_sdk::TenantContext;

static PUBLISHER: OnceLock<Publisher> = OnceLock::new();

/// Topic a due flow-run request is published to (scheduler -> orchestrator).
pub fn topic_flow_requested() -> String {
    get_config_property("finzly.jobs.flow.execution.requested.topic")
        .unwrap_or_else(|| "finzly.jobs.flow.execution.requested".to_string())
}

/// Topic a single job dispatch is published to (orchestrator -> worker).
#[allow(dead_code)] // consumed in stage 3 (per-job dispatch)
pub fn topic_job_requested() -> String {
    get_config_property("finzly.jobs.job.execution.requested.topic")
        .unwrap_or_else(|| "finzly.jobs.job.execution.requested".to_string())
}

/// Topic a job-completion is published to (worker -> orchestrator).
#[allow(dead_code)] // consumed in stage 3 (per-job dispatch)
pub fn topic_job_completed() -> String {
    get_config_property("finzly.jobs.job.execution.completed.topic")
        .unwrap_or_else(|| "finzly.jobs.job.execution.completed".to_string())
}

/// Build the process-wide publisher. Call once at startup after config is loaded.
pub fn init_publisher() -> Result<(), String> {
    let config = KafkaConfig::from_config();
    let publisher =
        Publisher::new(&config).map_err(|e| format!("Failed to create Kafka publisher: {e}"))?;
    PUBLISHER
        .set(publisher)
        .map_err(|_| "Kafka publisher already initialized".to_string())?;
    Ok(())
}

/// Publish `msg` to `topic` keyed by `key`. When `ordered` is true the SDK routes
/// by `hash(key) % partitions`, giving FIFO ordering per key (used for the QUEUE
/// concurrency policy, where key = job name).
pub async fn publish<T: Serialize>(
    topic: &str,
    key: &str,
    tenant: &str,
    ordered: bool,
    msg: &T,
) -> Result<(), String> {
    let publisher = PUBLISHER
        .get()
        .ok_or_else(|| "Kafka publisher not initialized".to_string())?;
    let ctx = TenantContext::from_tenant_name(tenant);
    publisher
        .send_message(topic, key, msg, Some(ordered), &ctx, None)
        .await
        .map_err(|e| format!("Kafka publish to {topic} failed: {e}"))?;
    debug!(topic, key, "Published message");
    Ok(())
}
