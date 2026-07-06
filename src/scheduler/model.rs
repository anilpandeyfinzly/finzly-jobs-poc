//! Row types for the scheduler.

use chrono::NaiveDateTime;
use sqlx::FromRow;
use uuid::Uuid;

/// A due `scheduled_trigger` row as claimed by the poll loop. The scheduler turns
/// this into a `TriggerDueEvent` and publishes it; the Orchestrator resolves
/// `target_ref` and runs it.
#[derive(Debug, Clone, FromRow)]
pub struct ClaimedTrigger {
    pub id: Uuid,
    pub tenant_name: String,
    pub target_ref: String,
    pub target_type: String,
    pub cron_expression: String,
    pub timezone: String,
    /// The fire instant this claim represents (the trigger's `next_fire_time` at
    /// claim time). Travels in the event and forms the idempotency key.
    pub next_fire_time: NaiveDateTime,
}
