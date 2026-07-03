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
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use uuid::Uuid;

/// A due `scheduled_trigger` joined to its `flow_definition`, as claimed by the
/// scheduler and carried over the channel to the executor. The executor uses it
/// to open a `flow_execution` and record a `job_execution` per job in the flow.
#[derive(Debug, Clone, FromRow)]
pub struct ClaimedTrigger {
    /// scheduled_trigger.id
    pub trigger_id: Uuid,
    pub tenant_name: String,
    pub flow_definition_id: Uuid,
    pub cron_expression: String,
    /// The fire instant this claim represents (the trigger's next_fire_time at
    /// claim time). Feeds flow_execution.scheduled_fire_time + the idempotency key.
    pub scheduled_fire_time: NaiveDateTime,
    /// flow_definition.name — for logging.
    pub flow_name: String,
    /// flow_definition.version — pinned onto the flow_execution.
    pub flow_version: i32,
    /// flow_definition.flow — the DAG. POC shape: {"jobs": ["<name>", ...]}.
    pub flow: JsonValue,
}

impl ClaimedTrigger {
    /// Job names declared in the flow JSON (`{"jobs": [...]}`); empty if malformed.
    pub fn job_names(&self) -> Vec<String> {
        self.flow
            .get("jobs")
            .and_then(JsonValue::as_array)
            .map(|jobs| {
                jobs.iter()
                    .filter_map(|j| j.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Deterministic per-fire key so re-delivery of the same fire is deduped by
    /// flow_execution's unique index on idempotency_key.
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.flow_definition_id, self.scheduled_fire_time)
    }
}