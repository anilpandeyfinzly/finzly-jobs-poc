//! Dispatcher (consumer): drains the channel of due jobs and runs each one
//! through the executor. Analogous to `PaymentEventConsumer`.

use tokio::sync::mpsc::Receiver;
use tracing::{error, info};

use super::executor::Executor;
use crate::registration::model::JobRow;

pub struct Dispatcher {
    receiver: Receiver<JobRow>,
}

impl Dispatcher {
    pub fn new(receiver: Receiver<JobRow>) -> Self {
        Self { receiver }
    }

    /// Spawn the consume loop. Runs until the channel is closed (i.e. the
    /// scheduler/sender is dropped).
    pub fn start(mut self) {
        tokio::spawn(async move {
            info!("Dispatcher started");
            while let Some(job) = self.receiver.recv().await {
                if let Err(e) = Executor::execute(&job).await {
                    error!(job = %job.name, error = %e, "Executor failed");
                }
            }
            info!("Dispatcher channel closed; stopping");
        });
    }
}
