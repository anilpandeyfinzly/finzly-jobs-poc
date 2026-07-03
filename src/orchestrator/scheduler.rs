//! Scheduler (producer): periodically claims due jobs and enqueues them.
//!
//! Selection is **cron-driven**: a job is due when its `next_fire_time` (computed
//! from `cron`) has passed. `interval_seconds` is NOT used for scheduling — it is
//! the status-report cadence.
//!
//! The claim uses `FOR UPDATE SKIP LOCKED` inside a transaction so that in a
//! multi-pod deployment each due row is picked by exactly one pod, and advances
//! `next_fire_time` in the same transaction so it won't be re-claimed.

use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{debug, error, info};

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use super::schedule::next_fire_from_now;
use crate::registration::{model::ClaimedTrigger, repository::default_tenant};

pub struct Scheduler {
    sender: Sender<ClaimedTrigger>,
    poll_interval_secs: u64,
    claim_batch: i64,
}

impl Scheduler {
    pub fn new(sender: Sender<ClaimedTrigger>) -> Self {
        let poll_interval_secs = get_config_property("bankos.scheduler.poll.interval")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5);
        let claim_batch = get_config_property("bankos.scheduler.claim.batch")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(50);
        Self {
            sender,
            poll_interval_secs,
            claim_batch,
        }
    }

    /// Spawn the poll loop. Every `poll_interval_secs`, claim due jobs (advancing
    /// their `next_fire_time` atomically) and enqueue them for dispatch.
    pub fn start(self) {
        tokio::spawn(async move {
            let interval = Duration::from_secs(self.poll_interval_secs);
            info!(
                interval_secs = self.poll_interval_secs,
                claim_batch = self.claim_batch,
                "Scheduler poller started"
            );

            loop {
                match self.claim_due_jobs().await {
                    Ok(triggers) if !triggers.is_empty() => {
                        debug!(count = triggers.len(), "Scheduler claimed due triggers");
                        for trigger in triggers {
                            let flow = trigger.flow_name.clone();
                            if let Err(e) = self.sender.try_send(trigger) {
                                error!(flow = %flow, error = %e, "Failed to enqueue claimed trigger");
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "claim_due_jobs failed"),
                }
                sleep(interval).await;
            }
        });
    }

    /// Claim due triggers concurrency-safely (multi-pod). In a single transaction:
    /// select due `scheduled_trigger` rows (joined to their enabled flow) with
    /// `FOR UPDATE SKIP LOCKED`, then advance each trigger's `next_fire_time` to
    /// the next cron occurrence so it isn't re-claimed. The `scheduled_fire_time`
    /// captured here is the instant the fire represents.
    pub async fn claim_due_jobs(&self) -> Result<Vec<ClaimedTrigger>, sqlx::Error> {
        let tenant = default_tenant();
        let pool = get_tenant_pool(&tenant)
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        let mut tx = pool.begin().await?;

        // The trigger references the flow by name (master-DB table); resolve it to
        // the enabled flow's latest version to get the id/version/flow DAG.
        let triggers: Vec<ClaimedTrigger> = sqlx::query_as::<_, ClaimedTrigger>(
            r#"
            SELECT
                st.id              AS trigger_id,
                st.tenant_name     AS tenant_name,
                fd.id              AS flow_definition_id,
                st.cron_expression AS cron_expression,
                st.next_fire_time  AS scheduled_fire_time,
                fd.name            AS flow_name,
                fd.version         AS flow_version,
                fd.flow            AS flow
            FROM demo_galaxy_jobs.scheduled_trigger st
            JOIN demo_galaxy_jobs.flow_definition fd
              ON fd.name = st.flow_definition_name
             AND fd.is_enabled = true
             AND fd.version = (
                 SELECT max(f2.version)
                 FROM demo_galaxy_jobs.flow_definition f2
                 WHERE f2.name = st.flow_definition_name AND f2.is_enabled = true
             )
            WHERE st.next_fire_time IS NOT NULL
              AND st.next_fire_time <= now()
            ORDER BY st.next_fire_time
            FOR UPDATE OF st SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(self.claim_batch)
        .fetch_all(&mut *tx)
        .await?;

        for trigger in &triggers {
            // Advance to the next cron occurrence so it won't fire again until due.
            let next = next_fire_from_now(&trigger.cron_expression).map(|dt| dt.naive_utc());
            sqlx::query(
                "UPDATE demo_galaxy_jobs.scheduled_trigger \
                 SET next_fire_time = $1, last_fire_time = now(), updated_at = now() \
                 WHERE id = $2",
            )
            .bind(next)
            .bind(trigger.trigger_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(triggers)
    }
}
