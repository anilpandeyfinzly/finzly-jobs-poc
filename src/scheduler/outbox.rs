//! Outbox publisher: drains staged due events to Kafka. Runs on its own interval,
//! decoupled from the claim loop, so publishing can't lose or block a claim.
//!
//! `FOR UPDATE SKIP LOCKED` lets multiple pods drain concurrently without
//! double-publishing. A row is marked `published_at` only after a successful send;
//! a failed send is left for the next round (at-least-once — the Orchestrator
//! dedupes on the event's idempotency key).

use std::time::Duration;

use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, error, info};
use uuid::Uuid;

use phoenix_config_sdk::config_properties::get_config_property;
use phoenix_postgres_sdk::get_tenant_pool;

use crate::kafka;
use crate::registration::repository::default_tenant;

pub struct OutboxPublisher {
    interval_secs: u64,
    batch: i64,
}

impl OutboxPublisher {
    pub fn new() -> Self {
        let interval_secs = get_config_property("bankos.scheduler.outbox.interval")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1);
        let batch = get_config_property("bankos.scheduler.outbox.batch")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(100);
        Self { interval_secs, batch }
    }

    pub fn start(self) {
        tokio::spawn(async move {
            let interval = Duration::from_secs(self.interval_secs);
            info!(interval_secs = self.interval_secs, "Outbox publisher started");
            loop {
                match self.drain().await {
                    Ok(n) if n > 0 => debug!(published = n, "Outbox drained"),
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "Outbox drain failed"),
                }
                sleep(interval).await;
            }
        });
    }

    /// Publish one batch of unpublished rows; mark each published on success.
    async fn drain(&self) -> Result<u64, sqlx::Error> {
        let pool = get_tenant_pool(&default_tenant())
            .await
            .map_err(|e| sqlx::Error::Configuration(e.to_string().into()))?;

        let mut tx = pool.begin().await?;

        let rows: Vec<(Uuid, String, String, String, Value)> = sqlx::query_as(
            r#"
            SELECT id, topic, msg_key, tenant_name, payload
            FROM demo_galaxy_jobs.outbox
            WHERE published_at IS NULL
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(self.batch)
        .fetch_all(&mut *tx)
        .await?;

        let mut published = 0u64;
        for (id, topic, key, tenant, payload) in &rows {
            match kafka::publish(topic, key, tenant, false, payload).await {
                Ok(()) => {
                    sqlx::query(
                        "UPDATE demo_galaxy_jobs.outbox SET published_at = now() WHERE id = $1",
                    )
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                    published += 1;
                }
                Err(e) => {
                    // Leave unpublished; retried next round.
                    error!(outbox_id = %id, topic = %topic, error = %e, "Publish failed; will retry");
                }
            }
        }

        tx.commit().await?;
        Ok(published)
    }
}
