//! Dispatcher (consumer): drains the channel of due jobs and runs each one
//! through the executor. Analogous to `PaymentEventConsumer`.

use tokio::sync::mpsc::Receiver;
use tracing::{error, info};

use super::executor::Executor;
use crate::registration::model::ClaimedTrigger;

pub struct Dispatcher {
    receiver: Receiver<ClaimedTrigger>,
}

impl Dispatcher {
    pub fn new(receiver: Receiver<ClaimedTrigger>) -> Self {
        Self { receiver }
    }

    /// Spawn the consume loop. Runs until the channel is closed (i.e. the
    /// scheduler/sender is dropped).
    pub fn start(mut self) {
        tokio::spawn(async move {
            info!("Dispatcher started");
            while let Some(trigger) = self.receiver.recv().await {
                if let Err(e) = Executor::execute(&trigger).await {
                    error!(flow = %trigger.flow_name, error = %e, "Executor failed");
                }
            }
            info!("Dispatcher channel closed; stopping");
        });
    }
}
