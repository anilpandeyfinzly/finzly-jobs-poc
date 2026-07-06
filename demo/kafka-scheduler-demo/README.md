# kafka-scheduler-demo

A tiny standalone service to **validate that finzly-jobs-poc publishes scheduled
Kafka messages at the expected times**. It consumes the service's topic and, for
every message, logs and records:

- when the message was **received**
- the **scheduled execution time** from the payload (if present)
- the **delay** = received − scheduled (ms; negative means it arrived early)

It also exposes a small HTTP API for test harnesses.

This project is independent of `finzly-jobs-poc` (its own Cargo workspace) and
talks to Kafka with plain `rdkafka` — no Finzly SDKs or private registry needed.

## Prerequisites

- Rust (stable) with **cmake + a C compiler + OpenSSL headers** — `rdkafka` is
  built with the `cmake-build` feature, which compiles librdkafka from source.
  - Debian/Ubuntu: `sudo apt-get install -y cmake build-essential libssl-dev`
- The local Kafka broker from the repo's `docker-compose.yml`.

## Run

```bash
# 1. Start the local Kafka broker (from the finzly-jobs-poc repo root)
cd ../..            # -> finzly-jobs-poc
docker compose up -d
docker compose ps   # wait until kafka is "healthy"

# 2. Run the demo
cd demo/kafka-scheduler-demo
cp .env.example .env         # optional; defaults already target localhost
export $(grep -v '^#' .env | xargs)   # or use your own env loader
cargo run
```

You should see `Subscribed; waiting for messages`.

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `0.0.0.0:8090` | HTTP bind address |
| `KAFKA_BROKERS` | `localhost:9092` | Broker list |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Must match the local broker |
| `KAFKA_TOPIC` | `finzly.jobs.flow.execution.requested` | Topic the service publishes fires to |
| `KAFKA_GROUP_ID` | `kafka-scheduler-demo` | Consumer group |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | `earliest` to replay the whole topic |
| `RUST_LOG` | `info` | Log level |

## HTTP API

```bash
# Health
curl localhost:8090/health            # -> ok

# Submit a job (test-harness echo; logged + stored, not published)
curl -X POST localhost:8090/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"123","scheduled_at":"2026-07-06T04:51:06Z","payload":{"foo":"bar"}}'
# -> {"accepted":true,"job_id":"123"}

# List everything observed on the topic, with timing/delay
curl -s localhost:8090/observations | jq
```

Example `/observations` entry:
```json
{
  "count": 1,
  "observations": [{
    "topic": "finzly.jobs.flow.execution.requested",
    "key": "de02cadc-56a1-4220-abb8-b27063e09003",
    "scheduled_at": "2026-07-06T04:51:06.788593+00:00",
    "received_at": "2026-07-06T04:51:07.031000+00:00",
    "delay_ms": 242,
    "payload": { "flowDefinitionId": "…", "tenantName": "finzly", "scheduleFireTime": "…" }
  }]
}
```

## Message format it expects

`finzly-jobs-poc` publishes through `phoenix-kafka-sdk`, which wraps the payload
in a `MessageWrapper` (camelCase) with the real object under `object`:

```json
{
  "tenant": "finzly",
  "eventType": null,
  "traceDetails": "00-…-…-01",
  "object": {
    "flowDefinitionId": "…",
    "tenantName": "finzly",
    "scheduleFireTime": "2026-07-06T04:51:06.788593"
  },
  "messageId": "…"
}
```

The demo unwraps `object` (and tolerates un-wrapped messages), then reads the
schedule time from `scheduleFireTime`, `scheduledAt`, or `scheduled_at`.

## End-to-end validation

1. Start Kafka (`docker compose up -d`) and this demo (`cargo run`).
2. Start `finzly-jobs-poc` (`cd ../.. && cargo run`).
3. Register jobs and force a fire:
   ```bash
   curl -F "file=@../job-config.yml" http://localhost:8080/job/registrations
   psql "$PG_URL" -c "UPDATE demo_galaxy_jobs.scheduled_trigger SET next_fire_time = now();"
   ```
4. Watch this demo log `Message received` with a small `delay_ms`, and confirm via
   `GET /observations`. A low delay means the service fired on schedule.
