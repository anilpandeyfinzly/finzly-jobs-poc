//! Kafka message DTOs for the orchestration pipeline. Field names are camelCase
//! on the wire to match the Java/Phoenix event conventions.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `flow.execution.requested` — a due flow should run. Published by the scheduler
/// when a trigger fires; the orchestrator loads the flow definition to run it. The
/// fire time travels here (not in a DB column) and forms the idempotency key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowExecutionRequested {
    pub flow_definition_id: Uuid,
    pub tenant_name: String,
    pub schedule_fire_time: NaiveDateTime,
}

impl FlowExecutionRequested {
    /// Deterministic per-fire key so re-delivery is deduped by flow_execution's
    /// unique index on idempotency_key.
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.flow_definition_id, self.schedule_fire_time)
    }
}
