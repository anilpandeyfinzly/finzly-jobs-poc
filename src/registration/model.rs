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
    /// Canonical string stored in the DB (job_registration.trigger_type).
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
    /// Canonical string stored in the DB (job_registration.status_report_mode).
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusReportMode::FIXEDTIME => "FIXED_TIME",
            StatusReportMode::COMPOSITIONAL => "COMPOSITIONAL",
        }
    }
}


use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct JobRow {
    pub id: i64,
    pub name: String,
    pub cron: Option<String>,
    pub trigger_type: String,
    pub callback_bean: String,
    pub callback_endpoint: Option<String>,
    pub event_topic: Option<String>,
    pub status_report_mode: String,
    pub interval_seconds: Option<i32>,
    pub sla_seconds: i32,
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub contract_version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}