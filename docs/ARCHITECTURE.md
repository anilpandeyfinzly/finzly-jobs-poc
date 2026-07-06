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
    SWP["Sweeps<br/>retry · SLA/timeout · crash"]
  end

  %% ---------------- Kafka ----------------
  subgraph KAFKA["Kafka topics"]
    T1["flow.execution.requested"]
    T2["job-dispatch-[service_context]"]
    T3["job.execution.completed"]
  end

  %% ---------------- Storage ----------------
  subgraph PG["PostgreSQL"]
    MST[("scheduled_trigger (master)<br/>+ outbox")]
    DEF[("project · job_definition<br/>flow_definition")]
    EXE[("flow_execution · job_execution<br/>execution_lock")]
  end
  RED[("Redis<br/>heartbeat-[jobExecId]")]

  %% ---------------- Worker side ----------------
  subgraph WRK["finzly-jobs-worker / any business service"]
    SDK["finzly-jobs-sdk<br/>JobRuntime · JobReporter · heartbeat"]
    JOB["Job impls"]
  end

  %% definitions & control
  API -->|CRUD / resume| DEF
  API -.->|"POST /jobs/.../status,progress"| COMP

  %% schedule -> fire
  SCH -->|"claim FOR UPDATE SKIP LOCKED"| MST
  SCH -->|"① publish fire (via outbox)"| T1
  T1  -->|"②"| DIS
  DIS -->|open flow_execution idempotent| EXE
  DIS -->|read enabled version| DEF

  %% dispatch: two modes
  DIS -->|"③ EVENT: publish"| T2
  T2  -->|consume| SDK
  DIS -->|"③ API: HTTP fire → 202 ack"| SDK

  %% execute
  SDK -->|run| JOB
  SDK -->|beat TTL 120s| RED

  %% report back: two transports
  SDK -->|"④ report (kafka)"| T3
  SDK -.->|"④ report (http) POST /status"| COMP
  T3  -->|consume| COMP

  %% completion & recovery
  COMP -->|"⑤ update · release lock · close/advance"| EXE
  SWP  -->|check liveness| RED
  SWP  -->|"⑥ timeout / retry / fail"| EXE
  SWP  -->|"re-dispatch"| T2
```

## 2. One fire, end to end (EVENT vs API, with report-back)

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler
    participant DB as Postgres
    participant K as Kafka
    participant DIS as Dispatcher
    participant W as Worker SDK
    participant J as Job / business svc
    participant R as Redis
    participant CMP as Completion

    SCH->>DB: claim due trigger (SKIP LOCKED), advance, write outbox
    SCH->>K: flow.execution.requested (outbox publisher)
    K->>DIS: consume
    DIS->>DB: open flow_execution (ON CONFLICT DO NOTHING)
    Note over DIS,DB: duplicate fire → no row → stop

    alt EVENT job (Kafka dispatch)
        DIS->>K: job-dispatch-[ctx]  (SKIP takes execution_lock)
        K->>W: consume JobExecutionRequested
        W->>R: heartbeat (TTL 120s, refresh 60s)
        W->>J: Job::execute(ctx)
        J-->>W: Success / Failure
        W->>K: job.execution.completed   (report transport = kafka)
    else API job (HTTP fire)
        DIS->>J: POST callbackEndpoint (JobExecutionRequested + handle)
        J-->>DIS: 202 Accepted  (ack ≠ complete → RUNNING)
        Note over J: async work, may outlive request
        J->>CMP: POST /api/jobs/executions/{id}/status  (report transport = http)
    end

    K->>CMP: job.execution.completed (kafka path)
    CMP->>DB: update job · release lock · retry or close flow (FOR UPDATE)

    opt worker crashed / stopped reporting
        Note over R,CMP: heartbeat lapses or last_report_at stale
        CMP-->>DB: sweep → FAILED(WORKER_LOST) / TIMED_OUT → retry
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

Legend: ① fire · ② dispatch decision · ③ dispatch (EVENT/API) · ④ report (kafka/http)
· ⑤ complete · ⑥ recover. Solid = Kafka/DB, dotted = HTTP.
