//! Background job orchestrator.
//!
//! Mirrors the payment-event pipeline: an init-once entry point wires a bounded
//! channel between a producer (the `Scheduler`, which polls the DB for due jobs)
//! and a consumer (the `Dispatcher`, which drains the channel and runs each job
//! via the `Executor`). Both run as background tokio tasks.

pub mod dispatcher;
pub mod executor;
pub mod scheduler;

use std::sync::OnceLock;

use tokio::sync::mpsc;
use tracing::info;

use phoenix_config_sdk::config_properties::get_config_property;

use crate::registration::model::JobRow;
use dispatcher::Dispatcher;
use scheduler::Scheduler;

/// Guards against double initialization (like the payment system's OnceCell).
static ORCHESTRATOR: OnceLock<()> = OnceLock::new();

/// Initialize the orchestrator once: wire the channel and start the dispatcher
/// (consumer) and scheduler (producer). Call at startup after the DB pool is ready.
pub fn init_orchestrator() {
    if ORCHESTRATOR.set(()).is_err() {
        return; // already initialized
    }

    let capacity = get_config_property("bankos.scheduler.channel.capacity")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10_000);

    let (sender, receiver) = mpsc::channel::<JobRow>(capacity);

    Dispatcher::new(receiver).start();
    Scheduler::new(sender).start();

    info!(channel_capacity = capacity, "Orchestrator initialized");
}
