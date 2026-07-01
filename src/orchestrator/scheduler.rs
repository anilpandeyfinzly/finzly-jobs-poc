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
use crate::registration::{model::JobRow, repository::default_tenant};

pub struct Scheduler {
    sender: Sender<JobRow>,
    poll_interval_secs: u64,
    claim_batch: i64,
}

impl Scheduler {
    pub fn new(sender: Sender<JobRow>) -> Self {
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
                    Ok(jobs) if !jobs.is_empty() => {
                        debug!(count = jobs.len(), "Scheduler claimed due jobs");
                        for job in jobs {
                            let job_name = job.name.clone();
                            if let Err(e) = self.sender.try_send(job) {
                                error!(job = %job_name, error = %e, "Failed to enqueue claimed job");
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

    /// Claim due jobs concurrency-safely (multi-pod). In a single transaction:
    /// select due rows with `FOR UPDATE SKIP LOCKED`, then advance each row's
    /// `next_fire_time` to the next cron occurrence so it isn't re-claimed.
    pub async fn claim_due_jobs(&self) -> Result<Vec<JobRow>, sqlx::Error> {
        let tenant = default_tenant();
        let pool = get_tenant_pool(&tenant)
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        let mut tx = pool.begin().await?;

        let jobs: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
            r#"
            SELECT * FROM demo_galaxy_jobs.job_registration
            WHERE cron IS NOT NULL
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

        for job in &jobs {
            // Advance to the next cron occurrence so it won't fire again until due.
            let next = job.cron.as_deref().and_then(next_fire_from_now);
            sqlx::query(
                "UPDATE demo_galaxy_jobs.job_registration \
                 SET next_fire_time = $1, updated_at = now() WHERE name = $2",
            )
            .bind(next)
            .bind(job.name.as_str())
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(jobs)
    }
}
