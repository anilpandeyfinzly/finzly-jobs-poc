//! Kafka transport: a process-wide publisher plus the Scheduler's one topic.
//! Mirrors how the Phoenix services use phoenix-kafka-sdk (`Publisher::send_message`),
//! so this POC can later run against the same brokers with no code change.
//!
//! The Scheduler publishes a single event when a trigger is due:
//!   - finzly.jobs.trigger.due : scheduler -> orchestrator (a due trigger fired)

use std::sync::OnceLock;

use serde::Serialize;
use tracing::debug;

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_kafka_sdk::{KafkaConfig, Publisher};
use phoenix_security_sdk::TenantContext;

static PUBLISHER: OnceLock<Publisher> = OnceLock::new();

/// Topic the "trigger due" event is published to (scheduler -> orchestrator).
pub fn topic_due() -> String {
    get_config_property("finzly.jobs.due.topic")
        .unwrap_or_else(|| "finzly.jobs.trigger.due".to_string())
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
/// by `hash(key) % partitions` (FIFO per key); the due event uses `false`.
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
