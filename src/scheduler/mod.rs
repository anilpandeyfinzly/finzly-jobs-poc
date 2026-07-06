//! Scheduler — the timing brain (and the whole job of this POC).
//!
//! It polls due `scheduled_trigger` rows, advances them, and stages a
//! `TriggerDueEvent` in the outbox; the outbox publisher sends it to
//! `finzly.jobs.trigger.due`. Everything past that topic (assignment, flow/DAG,
//! execution, concurrency, workers) is the separate Orchestrator service.

pub mod claim;
pub mod event;
pub mod model;
pub mod outbox;
pub mod schedule;

use std::sync::OnceLock;

use tracing::info;

use claim::Scheduler;
use outbox::OutboxPublisher;

/// Guards against double initialization.
static SCHEDULER: OnceLock<()> = OnceLock::new();

/// Start the claim loop (producer) and the outbox publisher. Call at startup after
/// the DB pool and Kafka publisher are ready.
pub fn init_scheduler() {
    if SCHEDULER.set(()).is_err() {
        return; // already initialized
    }
    OutboxPublisher::new().start();
    Scheduler::new().start();
    info!("Scheduler initialized");
}
