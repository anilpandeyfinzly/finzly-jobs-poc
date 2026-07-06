//! Data structures for the finzly `job-config.yml` contract — reduced to what the
//! Scheduler needs (a service name + jobs with optional crons). Extra keys in the
//! YAML (triggerType, callbacks, SLA, etc.) are the Orchestrator's concern and are
//! ignored here by serde.

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
    pub jobs: Vec<Job>,
}

#[derive(Debug, Deserialize)]
pub struct Job {
    pub name: String,
    /// Optional — a job with no cron is manual-only and gets no schedule.
    pub cron: Option<String>,
}
