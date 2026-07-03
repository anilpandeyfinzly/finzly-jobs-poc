//! Data structures mapping the finzly `job-config.yml` registration contract.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub finzly: Finzly,
}

#[derive(Debug, Deserialize)]
pub struct Finzly {
    pub job: JobConfiguration,
}

#[derive(Debug, Deserialize)]
pub struct JobConfiguration {
    pub service: String,
    #[serde(rename = "serviceBaseUrl")]
    pub service_base_url: String,

    #[serde(rename = "contractVersion")]
    pub contract_version: String,

    pub jobs: Vec<Job>,
}

#[derive(Debug, Deserialize)]
pub struct Job {
    pub name: String,

    // Optional because manual jobs don't have a cron.
    pub cron: Option<String>,

    #[serde(rename = "triggerType")]
    pub trigger_type: TriggerType,

    #[serde(rename = "callbackBean")]
    pub callback_bean: String,

    #[serde(rename = "callbackEndpoint")]
    pub callback_endpoint: Option<String>,

    #[serde(rename = "eventTopic")]
    pub event_topic: Option<String>,

    #[serde(rename = "statusReport")]
    pub status_report: StatusReport,

    #[serde(rename = "slaSeconds")]
    pub sla_seconds: u64,

    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: u64,

    #[serde(rename = "maxRetries")]
    pub max_retries: u32,
}

#[derive(Debug, Deserialize)]
pub struct StatusReport {
    pub mode: StatusReportMode,

    #[serde(rename = "intervalSeconds")]
    pub interval_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub enum TriggerType {
    API,
    EVENT,
}

impl TriggerType {
    /// Canonical string stored as job_definition.job_type.
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerType::API => "API",
            TriggerType::EVENT => "EVENT",
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum StatusReportMode {
    #[serde(rename = "FIXED_TIME")]
    FIXEDTIME,
    COMPOSITIONAL,
}

impl StatusReportMode {
    /// Canonical string stored inside job_definition.parameters (statusReport.mode).
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusReportMode::FIXEDTIME => "FIXED_TIME",
            StatusReportMode::COMPOSITIONAL => "COMPOSITIONAL",
        }
    }
}


use chrono::NaiveDateTime;
use sqlx::FromRow;
use uuid::Uuid;

/// A due `scheduled_trigger` resolved to its enabled `flow_definition`, as claimed
/// by the scheduler. The scheduler turns this into a `flow.execution.requested`
/// message; the orchestrator reloads the flow definition to run it.
#[derive(Debug, Clone, FromRow)]
pub struct ClaimedTrigger {
    /// scheduled_trigger.id
    pub trigger_id: Uuid,
    pub tenant_name: String,
    pub flow_definition_id: Uuid,
    pub cron_expression: String,
    /// The fire instant this claim represents (the trigger's next_fire_time at
    /// claim time). Travels in the message and forms the idempotency key.
    pub scheduled_fire_time: NaiveDateTime,
    /// flow_definition.name — for logging.
    pub flow_name: String,
}