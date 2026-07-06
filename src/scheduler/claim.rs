//! Claim loop (producer): periodically claims due triggers and writes a due event
//! to the outbox — all in one transaction, so a crash never loses a fire.
//!
//! The claim uses `FOR UPDATE SKIP LOCKED` so that in a multi-pod deployment each due
//! trigger is picked by exactly one pod, and advances `next_fire_time` in the same
//! transaction so it won't be re-claimed.

use std::time::Duration;

use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use super::event::TriggerDueEvent;
use super::model::ClaimedTrigger;
use super::schedule::next_fire_from_now_tz;
use crate::kafka;
use crate::registration::repository::default_tenant;

pub struct Scheduler {
    poll_interval_secs: u64,
    claim_batch: i64,
}

impl Scheduler {
    pub fn new() -> Self {
        let poll_interval_secs = get_config_property("bankos.scheduler.poll.interval")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5);
        let claim_batch = get_config_property("bankos.scheduler.claim.batch")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(50);
        Self {
            poll_interval_secs,
            claim_batch,
        }
    }

    /// Spawn the poll loop. Every `poll_interval_secs`, claim due triggers (advancing
    /// their `next_fire_time` atomically) and stage a due event in the outbox.
    pub fn start(self) {
        tokio::spawn(async move {
            let interval = Duration::from_secs(self.poll_interval_secs);
            info!(
                interval_secs = self.poll_interval_secs,
                claim_batch = self.claim_batch,
                "Scheduler poller started"
            );
            loop {
                match self.claim_due_triggers().await {
                    Ok(n) if n > 0 => debug!(count = n, "Scheduler claimed due triggers"),
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "claim_due_triggers failed"),
                }
                sleep(interval).await;
            }
        });
    }

    /// One claim cycle. In a single transaction: select due triggers with
    /// `FOR UPDATE SKIP LOCKED`, advance each `next_fire_time` (in its timezone), and
    /// stage the due event in the outbox. Returns the number of triggers claimed.
    pub async fn claim_due_triggers(&self) -> Result<u64, sqlx::Error> {
        let tenant = default_tenant();
        let pool = get_tenant_pool(&tenant)
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        let mut tx = pool.begin().await?;

        let triggers: Vec<ClaimedTrigger> = sqlx::query_as::<_, ClaimedTrigger>(
            r#"
            SELECT id, tenant_name, target_ref, target_type, cron_expression, timezone,
                   next_fire_time
            FROM demo_galaxy_jobs.scheduled_trigger
            WHERE enabled = true
              AND next_fire_time IS NOT NULL
              AND next_fire_time <= now()
            ORDER BY next_fire_time
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(self.claim_batch)
        .fetch_all(&mut *tx)
        .await?;

        for trigger in &triggers {
            // Advance to the next cron occurrence (misfire = SKIP: relative to now).
            let next = next_fire_from_now_tz(&trigger.cron_expression, &trigger.timezone);
            if next.is_none() {
                // Bad cron on a live trigger → disable it rather than NULL it silently.
                warn!(trigger = %trigger.id, cron = %trigger.cron_expression,
                      "Invalid cron; disabling trigger");
                sqlx::query(
                    "UPDATE demo_galaxy_jobs.scheduled_trigger \
                     SET enabled = false, updated_at = now() WHERE id = $1",
                )
                .bind(trigger.id)
                .execute(&mut *tx)
                .await?;
                continue;
            }

            sqlx::query(
                "UPDATE demo_galaxy_jobs.scheduled_trigger \
                 SET next_fire_time = $1, last_fire_time = now(), updated_at = now() \
                 WHERE id = $2",
            )
            .bind(next)
            .bind(trigger.id)
            .execute(&mut *tx)
            .await?;

            // Stage the due event in the outbox (published by the outbox loop).
            let event = TriggerDueEvent::from_claim(trigger);
            let payload = serde_json::to_value(&event)
                .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
            sqlx::query(
                r#"
                INSERT INTO demo_galaxy_jobs.outbox (topic, msg_key, tenant_name, payload)
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(kafka::topic_due())
            .bind(trigger.id.to_string())
            .bind(&trigger.tenant_name)
            .bind(&payload)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(triggers.len() as u64)
    }
}
