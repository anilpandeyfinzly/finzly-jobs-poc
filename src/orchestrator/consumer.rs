//! Orchestrator Kafka consumer: subscribes to `flow.execution.requested` and runs
//! each requested flow via the [`Executor`]. Replaces the old in-process mpsc
//! channel between scheduler and dispatcher — the scheduler now publishes and this
//! consumer (which may run in a different pod) receives.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{error, info, warn};

use phoenix_kafka_sdk::{Consumer, KafkaConfig, MessageHandler, MessageMetadata};

use super::completion;
use super::executor::Executor;
use super::messages::{FlowExecutionRequested, JobExecutionCompleted, JobExecutionRequested};
use super::worker;
use crate::kafka;

const CONSUMER_GROUP_ID: &str = "finzly-jobs-orchestrator";

/// Routes consumed messages by topic. In this single-binary POC one consumer wears
/// all three hats (scheduler-facing, worker, completion); in production these are
/// separate services/consumer groups.
struct OrchestratorHandler {
    flow_requested_topic: String,
    job_requested_topic: String,
    job_completed_topic: String,
}

#[async_trait]
impl MessageHandler for OrchestratorHandler {
    async fn handle_message(
        &self,
        message: Vec<u8>,
        metadata: MessageMetadata,
    ) -> phoenix_kafka_sdk::Result<()> {
        let topic = metadata.topic.as_str();

        if topic == self.flow_requested_topic {
            match serde_json::from_slice::<FlowExecutionRequested>(&message) {
                Ok(msg) => {
                    if let Err(e) = Executor::run_flow(&msg).await {
                        error!(flow_definition_id = %msg.flow_definition_id, error = %e,
                               "Failed to run requested flow");
                    }
                }
                Err(e) => error!(%topic, error = %e, "Bad flow.execution.requested payload"),
            }
        } else if topic == self.job_requested_topic {
            match serde_json::from_slice::<JobExecutionRequested>(&message) {
                Ok(req) => worker::handle_job_requested(&req).await,
                Err(e) => error!(%topic, error = %e, "Bad job.execution.requested payload"),
            }
        } else if topic == self.job_completed_topic {
            match serde_json::from_slice::<JobExecutionCompleted>(&message) {
                Ok(msg) => {
                    if let Err(e) = completion::handle_job_completed(&msg).await {
                        error!(job = %msg.job_name, error = %e, "Failed to apply job completion");
                    }
                }
                Err(e) => error!(%topic, error = %e, "Bad job.execution.completed payload"),
            }
        } else {
            warn!(%topic, "No handler for topic");
        }
        Ok(())
    }
}

/// Spawn the orchestrator consumer. Runs the SDK consumer loop on a background task.
pub fn start() {
    let flow_requested_topic = kafka::topic_flow_requested();
    let job_requested_topic = kafka::topic_job_requested();
    let job_completed_topic = kafka::topic_job_completed();
    let handler = Arc::new(OrchestratorHandler {
        flow_requested_topic: flow_requested_topic.clone(),
        job_requested_topic: job_requested_topic.clone(),
        job_completed_topic: job_completed_topic.clone(),
    });

    tokio::spawn(async move {
        let mut config = KafkaConfig::from_config();
        config.group_id(CONSUMER_GROUP_ID).enable_auto_commit(false);

        let topics = [
            flow_requested_topic.as_str(),
            job_requested_topic.as_str(),
            job_completed_topic.as_str(),
        ];
        let consumer = match Consumer::new(config).and_then(|c| c.subscribe(&topics)) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to start orchestrator consumer");
                return;
            }
        };

        info!(?topics, "Orchestrator consumer started");
        if let Err(e) = consumer.start(handler).await {
            error!(error = %e, "Orchestrator consumer stopped with error");
        }
    });
}
