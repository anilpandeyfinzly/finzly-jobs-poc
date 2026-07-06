//! kafka-scheduler-demo
//!
//! A standalone tool to validate that finzly-jobs-poc publishes scheduled Kafka
//! messages at the expected times. It:
//!   1. runs a small HTTP API (`GET /health`, `POST /jobs`, `GET /observations`)
//!   2. consumes the service's topic and, for each message, records the scheduled
//!      fire time vs. the actual receive time and the delay between them.
//!
//! The service publishes via phoenix-kafka-sdk, which wraps payloads in a
//! `MessageWrapper` (camelCase) with the real payload under `object`. This demo
//! unwraps that (and also tolerates un-wrapped messages), then reads the schedule
//! time from `scheduleFireTime` / `scheduledAt` / `scheduled_at`.

use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{error, info, warn};

/// Config from the environment (see README / .env.example).
struct Config {
    http_addr: String,
    brokers: String,
    topic: String,
    group_id: String,
    security_protocol: String,
    offset_reset: String,
}

impl Config {
    fn from_env() -> Self {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            http_addr: get("HTTP_ADDR", "0.0.0.0:8090"),
            brokers: get("KAFKA_BROKERS", "localhost:9092"),
            topic: get("KAFKA_TOPIC", "finzly.jobs.trigger.due"),
            group_id: get("KAFKA_GROUP_ID", "kafka-scheduler-demo"),
            security_protocol: get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            offset_reset: get("KAFKA_AUTO_OFFSET_RESET", "latest"),
        }
    }
}

/// One consumed message, with timing analysis.
#[derive(Clone, Serialize)]
struct Observation {
    topic: String,
    key: Option<String>,
    /// Scheduled fire time parsed from the payload (if present), as RFC3339.
    scheduled_at: Option<String>,
    /// When this demo received the message, as RFC3339 (UTC).
    received_at: String,
    /// received_at - scheduled_at, in milliseconds. Negative => arrived early.
    delay_ms: Option<i64>,
    /// The decoded payload (unwrapped from MessageWrapper.object when present).
    payload: Value,
}

/// A job submitted to `POST /jobs` (a simple test-harness endpoint).
#[derive(Clone, Serialize, Deserialize)]
struct JobRequest {
    job_id: String,
    #[serde(default)]
    scheduled_at: Option<String>,
    #[serde(default)]
    payload: Value,
}

#[derive(Default)]
struct AppState {
    observations: Mutex<Vec<Observation>>,
    // Retained for inspection/debugging; not currently surfaced via an endpoint.
    #[allow(dead_code)]
    submitted_jobs: Mutex<Vec<JobRequest>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env();
    let state = Arc::new(AppState::default());

    info!(
        brokers = %cfg.brokers,
        topic = %cfg.topic,
        group_id = %cfg.group_id,
        "Starting kafka-scheduler-demo"
    );

    // Kafka consumer on a background task.
    {
        let state = state.clone();
        let topic = cfg.topic.clone();
        let consumer = build_consumer(&cfg)?;
        tokio::spawn(async move { consume_loop(consumer, topic, state).await });
    }

    // HTTP server.
    let app = Router::new()
        .route("/health", get(health))
        .route("/jobs", post(submit_job))
        .route("/observations", get(list_observations))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.http_addr).await?;
    info!(addr = %cfg.http_addr, "HTTP server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_consumer(cfg: &Config) -> anyhow::Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("security.protocol", &cfg.security_protocol)
        .set("auto.offset.reset", &cfg.offset_reset)
        .set("enable.auto.commit", "true")
        .create()?;
    Ok(consumer)
}

async fn consume_loop(consumer: StreamConsumer, topic: String, state: Arc<AppState>) {
    if let Err(e) = consumer.subscribe(&[topic.as_str()]) {
        error!(error = %e, "Failed to subscribe");
        return;
    }
    info!(%topic, "Subscribed; waiting for messages");
    loop {
        match consumer.recv().await {
            Err(e) => warn!(error = %e, "Kafka recv error"),
            Ok(msg) => {
                let received_at = Utc::now();
                let key = msg.key().map(|k| String::from_utf8_lossy(k).into_owned());
                let raw = msg
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .unwrap_or_default();

                let value: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
                let payload = unwrap_payload(&value);
                let scheduled = extract_schedule_time(&payload);
                let delay_ms = scheduled.map(|s| (received_at - s).num_milliseconds());

                info!(
                    topic = %msg.topic(),
                    key = ?key,
                    scheduled_at = ?scheduled.map(|s| s.to_rfc3339()),
                    received_at = %received_at.to_rfc3339(),
                    delay_ms = ?delay_ms,
                    "Message received"
                );

                let obs = Observation {
                    topic: msg.topic().to_string(),
                    key,
                    scheduled_at: scheduled.map(|s| s.to_rfc3339()),
                    received_at: received_at.to_rfc3339(),
                    delay_ms,
                    payload,
                };
                state.observations.lock().unwrap().push(obs);
            }
        }
    }
}

/// Unwrap the phoenix-kafka-sdk `MessageWrapper` (payload under `object`); if the
/// message isn't wrapped, return it as-is.
fn unwrap_payload(value: &Value) -> Value {
    match value.get("object") {
        Some(inner) => inner.clone(),
        None => value.clone(),
    }
}

/// Read a schedule timestamp from common field names, tolerating both RFC3339
/// (with offset/Z) and naive `YYYY-MM-DDTHH:MM:SS[.ffffff]` (assumed UTC).
fn extract_schedule_time(payload: &Value) -> Option<DateTime<Utc>> {
    let s = ["scheduleFireTime", "scheduledAt", "scheduled_at"]
        .iter()
        .find_map(|k| payload.get(*k).and_then(Value::as_str))?;

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(DateTime::from_naive_utc_and_offset(naive, Utc));
        }
    }
    None
}

// ---- HTTP handlers ----

async fn health() -> &'static str {
    "ok"
}

/// Accept a job payload (test harness). Logs and stores it; does not publish.
async fn submit_job(
    State(state): State<Arc<AppState>>,
    Json(job): Json<JobRequest>,
) -> Json<Value> {
    info!(job_id = %job.job_id, scheduled_at = ?job.scheduled_at, "Job submitted");
    state.submitted_jobs.lock().unwrap().push(job.clone());
    Json(json!({ "accepted": true, "job_id": job.job_id }))
}

/// Return everything observed so far — handy for asserting timing in a test.
async fn list_observations(State(state): State<Arc<AppState>>) -> Json<Value> {
    let obs = state.observations.lock().unwrap().clone();
    let count = obs.len();
    Json(json!({ "count": count, "observations": obs }))
}
