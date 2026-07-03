//! Background job orchestrator.
//!
//! Kafka-driven pipeline: the `Scheduler` (producer) polls the DB for due triggers
//! and publishes `flow.execution.requested`; the orchestrator `consumer` receives
//! those and runs each flow via the `Executor`. Producer and consumer are decoupled
//! by Kafka, so they can run in the same process (this POC) or across pods.

pub mod consumer;
pub mod executor;
pub mod messages;
pub mod schedule;
pub mod scheduler;

use std::sync::OnceLock;

use tracing::info;

use scheduler::Scheduler;

/// Guards against double initialization (like the payment system's OnceCell).
static ORCHESTRATOR: OnceLock<()> = OnceLock::new();

/// Initialize the orchestrator once: start the scheduler (publishes due flows) and
/// the Kafka consumer (runs them). Call at startup after the DB pool and Kafka
/// publisher are ready.
pub fn init_orchestrator() {
    if ORCHESTRATOR.set(()).is_err() {
        return; // already initialized
    }

    consumer::start();
    Scheduler::new().start();

    info!("Orchestrator initialized");
}
