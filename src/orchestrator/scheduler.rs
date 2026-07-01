//! Scheduler (producer): periodically polls the DB for due jobs and enqueues
//! them onto the dispatch channel.

use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{debug, error, info};

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use crate::registration::{model::JobRow, repository::default_tenant};

pub struct Scheduler {
    sender: Sender<JobRow>,
    poll_interval_secs: u64,
}

impl Scheduler {
    pub fn new(sender: Sender<JobRow>) -> Self {
        let poll_interval_secs = get_config_property("bankos.scheduler.poll.interval")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5);
        Self {
            sender,
            poll_interval_secs,
        }
    }

    /// Spawn the poll loop. Every `poll_interval_secs`, pick due jobs, enqueue
    /// each for dispatch, and bump `updated_at` so they don't re-fire until the
    /// interval elapses again.
    pub fn start(self) {
        tokio::spawn(async move {
            let interval = Duration::from_secs(self.poll_interval_secs);
            info!(
                interval_secs = self.poll_interval_secs,
                "Scheduler poller started"
            );

            loop {
                match self.job_picker().await {
                    Ok(Some(jobs)) => {
                        debug!(count = jobs.len(), "Scheduler picked due jobs");
                        for job in jobs {
                            let job_name = job.name.clone();
                            // Enqueue for the dispatcher (non-blocking, like the
                            // payment-event producer's try_send).
                            if let Err(e) = self.sender.try_send(job) {
                                error!(job = %job_name, error = %e, "Failed to enqueue due job");
                                continue;
                            }
                            // Reset the interval window so it isn't picked again next tick.
                            if let Err(e) = mark_picked(&job_name).await {
                                error!(job = %job_name, error = %e, "Failed to mark job picked");
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => error!(error = %e, "job_picker failed"),
                }
                sleep(interval).await;
            }
        });
    }

    /// Due jobs: recurring (cron + interval set) whose interval window has elapsed
    /// since the last `updated_at`.
    pub async fn job_picker(&self) -> Result<Option<Vec<JobRow>>, sqlx::Error> {
        let tenant = default_tenant();
        let pool = get_tenant_pool(&tenant)
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        let query = r#"
            SELECT *
            FROM demo_galaxy_jobs.job_registration
            WHERE interval_seconds IS NOT NULL
              AND cron IS NOT NULL
              AND updated_at + (interval_seconds * INTERVAL '1 second') <= NOW()
        "#;

        let jobs: Vec<JobRow> = sqlx::query_as::<_, JobRow>(query)
            .fetch_all(&pool)
            .await?;

        if jobs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(jobs))
        }
    }
}

/// Bump `updated_at` for a picked job so `job_picker` won't re-select it until
/// its interval elapses again.
async fn mark_picked(job_name: &str) -> Result<(), sqlx::Error> {
    let tenant = default_tenant();
    let pool = get_tenant_pool(&tenant)
        .await
        .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

    sqlx::query("UPDATE demo_galaxy_jobs.job_registration SET updated_at = now() WHERE name = $1")
        .bind(job_name)
        .execute(&pool)
        .await?;
    Ok(())
}
