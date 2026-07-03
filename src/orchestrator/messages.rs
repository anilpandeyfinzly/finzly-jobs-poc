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

/// Per-job concurrency policy, decided by the orchestrator before dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConcurrencyPolicy {
    /// Don't run if an instance of this job is already running.
    Skip,
    /// Queue behind the running instance (FIFO per job name).
    Queue,
    /// Run regardless of other instances.
    Parallel,
}

impl ConcurrencyPolicy {
    /// Parse from the string stored in job_definition.parameters.concurrency.
    /// Unknown / missing values default to the safest policy, SKIP.
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "QUEUE" => Self::Queue,
            "PARALLEL" => Self::Parallel,
            _ => Self::Skip,
        }
    }
}

/// `job.execution.requested` — dispatch one job to the worker (workflow-processor).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobExecutionRequested {
    pub flow_execution_id: Uuid,
    pub job_execution_id: Uuid,
    pub tenant_name: String,
    pub job_name: String,
    pub service_context: String,
    pub attempt: i32,
}

/// `job.execution.completed` — the worker finished a job (SUCCESS or FAILED).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobExecutionCompleted {
    pub flow_execution_id: Uuid,
    pub job_execution_id: Uuid,
    pub tenant_name: String,
    pub job_name: String,
    pub service_context: String,
    pub status: String, // SUCCESS | FAILED
    pub output: Option<String>,
    pub error_message: Option<String>,
}
