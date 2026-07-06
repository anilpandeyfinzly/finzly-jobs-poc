# finzly-jobs — System Diagrams (Mermaid)

Renders on GitHub and any Mermaid-aware viewer. Companion to
[LOW-LEVEL-DESIGN.md](LOW-LEVEL-DESIGN.md) and [SDK-INTEGRATION.md](SDK-INTEGRATION.md).

## 1. System overview

```mermaid
flowchart TB
  %% ---------------- Control plane ----------------
  subgraph ORCH["finzly-jobs-orchestrator (service — the brain)"]
    API["REST control plane<br/>projects / flows / schedules / resume / queries"]
    SCH["Scheduler<br/>cron claim + outbox"]
    DIS["Dispatcher<br/>DAG walk + concurrency"]
    COMP["Completion handler"]
    SWP["Sweeps<br/>retry · SLA backstop · crash"]
  end

  %% ---------------- Kafka ----------------
  subgraph KAFKA["Kafka topics"]
    T1["flow.execution.requested"]
    T2["job-dispatch-[ctx]"]
    T4["job-report-[ctx]"]
    T3["job.execution.completed"]
  end

  %% ---------------- Storage ----------------
  subgraph PG["PostgreSQL"]
    MST[("scheduled_trigger (master)<br/>+ outbox")]
    DEF[("project · job_definition<br/>flow_definition")]
    EXE[("flow_execution · job_execution<br/>execution_lock")]
  end
  RED[("Redis<br/>worker heartbeat")]

  %% ---------------- Worker ----------------
  subgraph WRK["finzly-jobs-worker (fires + owns SLA)"]
    WSDK["SDK worker runtime<br/>fire (TLS) · SLA timer · report-up"]
  end

  %% ---------------- Business service ----------------
  subgraph SVC["business service"]
    SSDK["finzly-jobs-sdk<br/>JobReporter"]
    JOB["Job / business logic"]
  end

  %% definitions & control
  API -->|CRUD / resume| DEF

  %% schedule -> fire
  SCH -->|"claim FOR UPDATE SKIP LOCKED"| MST
  SCH -->|"① publish fire (via outbox)"| T1
  T1  -->|"②"| DIS
  DIS -->|open flow_execution idempotent| EXE
  DIS -->|read enabled version| DEF

  %% assign to worker
  DIS -->|"③ assign (concurrency policy)"| T2
  T2  -->|consume| WSDK

  %% worker fires the service
  WSDK -->|"④ fire (HTTPS/TLS) → 202 ack"| SSDK
  WSDK -->|record fired_at · deadline| EXE
  WSDK -->|heartbeat while awaiting| RED
  SSDK --> JOB

  %% service reports BACK TO THE WORKER
  SSDK -->|"⑤ report (kafka)"| T4
  SSDK -.->|"⑤ report (http)"| WSDK
  T4   -->|consume| WSDK

  %% worker computes SLA verdict, reports up
  WSDK -->|"⑥ verdict + slaBreached"| T3
  T3   -->|consume| COMP

  %% completion & recovery
  COMP -->|"⑦ update · release lock · close/advance"| EXE
  SWP  -->|worker-death backstop| RED
  SWP  -->|"timeout / retry / fail"| EXE
  SWP  -->|"re-assign"| T2
```

## 2. One fire, end to end (EVENT vs API, with report-back)

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler
    participant DB as Postgres
    participant K as Kafka
    participant DIS as Dispatcher
    participant W as Worker
    participant S as Business service (SDK)
    participant R as Redis
    participant CMP as Completion

    SCH->>DB: claim due trigger (SKIP LOCKED), advance, write outbox
    SCH->>K: flow.execution.requested (outbox publisher)
    K->>DIS: consume
    DIS->>DB: open flow_execution (ON CONFLICT DO NOTHING)
    Note over DIS,DB: duplicate fire → no row → stop
    DIS->>K: job-dispatch-[ctx] (assign; SKIP takes execution_lock)
    K->>W: consume assignment
    W->>DB: record fired_at + deadline (RUNNING)
    W->>R: heartbeat while awaiting

    alt API job (HTTP fire)
        W->>S: POST callbackEndpoint (HTTPS/TLS verified) + JobHandle
        S-->>W: 202 Accepted  (ack ≠ complete)
    else EVENT job
        W->>K: publish to service eventTopic
        K->>S: consume
    end

    Note over S: async work, may outlive the fire
    alt service reports (transport = kafka)
        S->>K: job-report-[ctx]
        K->>W: consume report
    else transport = http
        S->>W: POST worker /report/{jobExecutionId}
    end

    W->>W: elapsed vs slaSeconds → slaBreached; or SLA timer fired → TIMED_OUT
    W->>K: job.execution.completed (+ slaBreached, elapsedMs)
    K->>CMP: consume
    CMP->>DB: update job · release lock · retry or close flow (FOR UPDATE)

    opt worker itself dies
        Note over R,CMP: worker heartbeat lapses
        CMP-->>DB: orchestrator sweep → FAILED(WORKER_LOST) → retry
    end
```

## 3. Job execution state machine

```mermaid
stateDiagram-v2
    [*] --> QUEUED: dispatched (QUEUE)
    [*] --> RUNNING: dispatched (SKIP/PARALLEL) or 202 ack
    QUEUED --> RUNNING: worker picks up
    QUEUED --> SKIPPED: SKIP lock already held
    RUNNING --> SUCCESS: report SUCCESS
    RUNNING --> FAILED: report FAILED (attempt = max)
    RUNNING --> RUNNING: report FAILED, retry (attempt+1)
    RUNNING --> TIMED_OUT: deadline / no report
    RUNNING --> FAILED: heartbeat lost (WORKER_LOST)
    SUCCESS --> [*]
    FAILED --> RUNNING: resume
    FAILED --> [*]
    TIMED_OUT --> [*]
    SKIPPED --> [*]
```

Legend: ① scheduler fire · ② dispatch · ③ assign to worker · ④ worker fires service
(TLS) · ⑤ service reports **back to worker** (kafka/http) · ⑥ worker verdict + SLA · ⑦
orchestrator completes. Solid = Kafka/DB, dotted = HTTP. **The worker owns the SLA
verdict because it made the fire.**
