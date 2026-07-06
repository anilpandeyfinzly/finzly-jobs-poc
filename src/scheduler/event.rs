//! The Scheduler's single output: the "trigger due" event, published to
//! `finzly.jobs.trigger.due` for the Orchestrator to consume.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::model::ClaimedTrigger;

/// Published when a trigger fires. The Orchestrator resolves `target_ref` to a flow
/// and takes over from here. `idempotency_key` lets it dedupe re-delivered fires.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerDueEvent {
    pub trigger_id: Uuid,
    pub tenant_name: String,
    pub target_type: String,
    pub target_ref: String,
    pub schedule_fire_time: NaiveDateTime,
    pub idempotency_key: String,
}

impl TriggerDueEvent {
    pub fn from_claim(t: &ClaimedTrigger) -> Self {
        Self {
            trigger_id: t.id,
            tenant_name: t.tenant_name.clone(),
            target_type: t.target_type.clone(),
            target_ref: t.target_ref.clone(),
            schedule_fire_time: t.next_fire_time,
            // Deterministic per fire → the Orchestrator dedupes on it.
            idempotency_key: format!("{}:{}", t.id, t.next_fire_time),
        }
    }
}
