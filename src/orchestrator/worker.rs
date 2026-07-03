//! Worker: stands in for the `workflow-processor-service`. Consumes
//! `job.execution.requested`, "runs" the job (POC = log-only, always SUCCESS), and
//! publishes `job.execution.completed`. In production this is a separate service
//! that loads the job's bean by class and executes it.

use tracing::info;

use super::messages::{JobExecutionCompleted, JobExecutionRequested};
use crate::kafka;

/// Handle one dispatched job: run it (log-only) and publish its completion.
pub async fn handle_job_requested(req: &JobExecutionRequested) {
    info!(
        job = %req.job_name,
        service_context = %req.service_context,
        attempt = req.attempt,
        "Worker running job"
    );

    // POC: no real work — report SUCCESS with a synthetic output.
    let completed = JobExecutionCompleted {
        flow_execution_id: req.flow_execution_id,
        job_execution_id: req.job_execution_id,
        tenant_name: req.tenant_name.clone(),
        job_name: req.job_name.clone(),
        service_context: req.service_context.clone(),
        status: "SUCCESS".to_string(),
        output: Some(format!("{} completed (log-only)", req.job_name)),
        error_message: None,
    };

    if let Err(e) = kafka::publish(
        &kafka::topic_job_completed(),
        &req.job_name,
        &req.tenant_name,
        false,
        &completed,
    )
    .await
    {
        // Nothing ran, so on publish failure the orchestrator will eventually treat
        // the job as stalled (crash-detection, future work). Just log here.
        tracing::warn!(job = %req.job_name, error = %e, "Failed to publish job.execution.completed");
    }
}
