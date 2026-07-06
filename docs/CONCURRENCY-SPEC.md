# Concurrency Control — Exhaustive Spec (SKIP / QUEUE / PARALLEL)

Every detail of how the orchestrator decides and enforces per-job concurrency when a
job of a flow is dispatched. Refines LOW-LEVEL-DESIGN §F3a / §F5.

**Defaults chosen here (flip if needed):**
- **Scope** = *global-per-job*: lock key `(tenant_name, service_context, job_name)`.
- **QUEUE** = *DB-chained FIFO* (not Kafka-partition FIFO).

---

## 0. What concurrency policy is NOT

- It does **not** order tasks inside a flow — that's the DAG (`next[]`): a task runs when
  its predecessors are SUCCESS.
- It governs **overlap of the same job with itself**: a task's job is already RUNNING
  (from a prior fire of this flow that hasn't finished, a retry, or another flow/trigger
  targeting the same job). The policy decides what to do about that overlap.

---

## 1. The concurrency domain (lock key)

Mutual exclusion is a single row in `execution_lock`; its UNIQUE constraint **is** the
lock:

```sql
CREATE TABLE demo_galaxy_jobs.execution_lock (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_name       VARCHAR(64)  NOT NULL,
    service_context   VARCHAR(128) NOT NULL,
    job_name          VARCHAR(128) NOT NULL,
    flow_execution_id UUID NOT NULL REFERENCES demo_galaxy_jobs.flow_execution(id),
    job_execution_id  UUID NOT NULL REFERENCES demo_galaxy_jobs.job_execution(id),
    locked_at         TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (tenant_name, service_context, job_name)     -- ⇐ the concurrency domain
);
```

- **Holding the lock = "this job is currently running."** Exactly one holder at a time.
- Key = `(tenant_name, service_context, job_name)` → the same job is serialized **across
  all flows and fires** within a tenant. (Alt scope in §11.)
- The lock is created when a job goes RUNNING and **deleted on every terminal transition**
  (SUCCESS/FAILED/TIMED_OUT/SKIPPED, and before a retry re-dispatch).

### job_execution columns this subsystem needs
```
task_id            VARCHAR(64)   -- which DAG node (for idempotency + DAG advance)
tenant_name        VARCHAR(64)   -- denormalized for promotion queries
service_context    VARCHAR(128)  -- denormalized for promotion queries
job_name           VARCHAR(128)
concurrency_policy VARCHAR(8)    -- resolved SKIP|QUEUE|PARALLEL (audit + uniform handling)
status             VARCHAR(16)   -- PENDING|QUEUED|RUNNING|SUCCESS|FAILED|TIMED_OUT|SKIPPED
attempt            INTEGER
created_at         TIMESTAMP     -- FIFO ordering for QUEUE
UNIQUE (flow_execution_id, task_id, attempt)            -- dispatch idempotency
```
Indexes:
```sql
CREATE INDEX idx_je_queue ON job_execution (tenant_name, service_context, job_name, status, created_at, id);
CREATE INDEX idx_je_flow  ON job_execution (flow_execution_id, status);
```

---

## 2. Where the policy value comes from (resolution)

Precedence, highest first:
1. **`task.concurrencyPolicy`** in the flow DAG JSON (`flow_definition.flow.tasks[].concurrencyPolicy`).
2. **`job_definition.parameters.concurrency`** (a default for that job).
3. **`SKIP`** — the safe fallback.

Resolved once at dispatch and **stored on `job_execution.concurrency_policy`** so the
completion/sweep paths don't need to re-read the flow.

Validated at `POST /flows`: value ∈ {SKIP, QUEUE, PARALLEL}; unknown → 400.

---

## 3. Statuses & transitions (concurrency-relevant)

```
PENDING  -- row created, decision not yet applied (transient, within the dispatch tx)
QUEUED   -- QUEUE policy, lock was held → waiting for its turn
RUNNING  -- holds the lock (SKIP/QUEUE) or lock-free (PARALLEL); dispatched to a worker
SKIPPED  -- SKIP policy, lock was held → terminal, never dispatched
SUCCESS | FAILED | TIMED_OUT -- terminal (see retry/SLA specs)
```
Every transition **out of RUNNING** deletes the lock; **SKIPPED** never held one.
QUEUED → RUNNING happens only via promotion (§5).

---

## 4. Dispatch algorithm (orchestrator, one job) — transaction **Tx-D**

Inputs: `flow_execution_id (FE)`, `task_id (T)`, `job_name (J)`, `service_context (SC)`,
`tenant (TN)`, resolved `policy (P)`, `attempt (A)`, `parameters`, `report_to`.

All steps 1–5 run in **one transaction Tx-D**; Kafka publish happens after commit via the
outbox (so the state change and the intent-to-dispatch are atomic).

```
-- 1. Idempotent row creation (dedupes duplicate flow-dispatch delivery)
INSERT INTO job_execution
    (flow_execution_id, task_id, job_name, service_context, tenant_name,
     concurrency_policy, status, attempt, max_attempts, created_at)
VALUES (FE, T, J, SC, TN, P, 'PENDING', A, <max>, now())
ON CONFLICT (flow_execution_id, task_id, attempt) DO NOTHING
RETURNING id;                                        -- JE
-- no row returned ⇒ already dispatched ⇒ COMMIT, ack, STOP.

-- 2. Branch on policy
if P == PARALLEL:
    UPDATE job_execution SET status='RUNNING', started_at=now(),
           deadline=now()+<timeout> WHERE id=JE;
    dispatch = true                                   -- no lock

if P in (SKIP, QUEUE):
    if P == QUEUE:
        -- FIFO guard: never jump an existing waiter for this key
        exists_waiter = SELECT 1 FROM job_execution
            WHERE tenant_name=TN AND service_context=SC AND job_name=J
              AND status='QUEUED' LIMIT 1;
        if exists_waiter:
            UPDATE job_execution SET status='QUEUED' WHERE id=JE;
            dispatch = false
            goto commit
    -- try to take the lock (atomic)
    INSERT INTO execution_lock (tenant_name, service_context, job_name,
                                flow_execution_id, job_execution_id)
    VALUES (TN, SC, J, FE, JE)
    ON CONFLICT (tenant_name, service_context, job_name) DO NOTHING
    RETURNING id;                                     -- LOCK?
    if LOCK acquired:
        UPDATE job_execution SET status='RUNNING', started_at=now(),
               deadline=now()+<timeout> WHERE id=JE;
        dispatch = true
    else:                                             -- lock held by someone
        if P == SKIP:
            UPDATE job_execution SET status='SKIPPED', ended_at=now() WHERE id=JE;
            dispatch = false
        if P == QUEUE:
            UPDATE job_execution SET status='QUEUED' WHERE id=JE;
            dispatch = false

-- 3. If dispatching, enqueue the outbox row IN THE SAME TX
if dispatch:
    INSERT INTO outbox (topic, msg_key, tenant_name, payload)
    VALUES ('job-dispatch-'||SC, J, TN, <JobExecutionRequested JE>);

-- 4. COMMIT Tx-D
-- 5. Outbox publisher loop later sends it to Kafka (at-least-once).
```

Notes:
- The **lock INSERT and the status UPDATE are in the same tx** → a job is RUNNING **iff** it
  holds the lock. No window where it's RUNNING without the lock or vice-versa.
- PARALLEL never touches `execution_lock`.

---

## 5. Completion / release / promotion (orchestrator, §F5) — transaction **Tx-C**

On a job reaching a terminal (or retry-pending) state, in **one transaction Tx-C**:

```
-- 1. Apply terminal status (idempotent: only if not already terminal)
UPDATE job_execution
   SET status=<S>, output=?, error_message=?, ended_at=now(), updated_at=now()
 WHERE id=JE AND status IN ('RUNNING','QUEUED');
-- (retry case: status set to 'FAILED' with next_retry_at; lock still released below)

-- 2. Release the lock (idempotent) and learn the key
DELETE FROM execution_lock WHERE job_execution_id=JE
RETURNING tenant_name, service_context, job_name;     -- K = (TN,SC,J); may be 0 rows (PARALLEL/SKIPPED)

-- 3. Promote the next QUEUED job for K (only if a lock existed / was freed)
if K present:
    SELECT id, flow_execution_id FROM job_execution
     WHERE tenant_name=TN AND service_context=SC AND job_name=J AND status='QUEUED'
     ORDER BY created_at, id
     FOR UPDATE SKIP LOCKED
     LIMIT 1;                                          -- JQ (oldest waiter)
    if JQ:
        INSERT INTO execution_lock (tenant_name,service_context,job_name,
                                    flow_execution_id, job_execution_id)
        VALUES (TN,SC,J, JQ.flow_execution_id, JQ.id)
        ON CONFLICT (tenant_name,service_context,job_name) DO NOTHING
        RETURNING id;                                  -- won?
        if won:
            UPDATE job_execution SET status='RUNNING', started_at=now(),
                   deadline=now()+<timeout> WHERE id=JQ.id;
            INSERT INTO outbox ('job-dispatch-'||SC, J, TN, <JobExecutionRequested JQ>);
        -- if not won: another tx grabbed the lock; JQ stays QUEUED, promoted next release.

-- 4. Advance the SAME flow's DAG (only on SUCCESS of JE): dispatch tasks whose
--    predecessors are now all SUCCESS (each via Tx-D). Distinct from step 3.

-- 5. Close the flow if no PENDING|QUEUED|RUNNING and no retry-pending remain for FE.

-- 6. COMMIT Tx-C  (SELECT flow_execution FOR UPDATE at the top serializes closure)
```

**Two independent mechanisms in Tx-C — do not conflate:**
- **Step 3 (promotion)** operates on the **concurrency domain K** — the next waiter may be
  in a *different* flow. It's how QUEUE drains.
- **Step 4 (DAG advance)** operates on the **same flow FE** — downstream tasks.

---

## 6. FIFO guarantee for QUEUE

Order is `ORDER BY created_at, id` over `status='QUEUED'` rows for key K. FIFO holds
because:
- On arrival, a QUEUE job that finds an existing waiter **appends** (Tx-D step 2 FIFO
  guard) — it can't jump the line even if it momentarily sees the lock free.
- On release, promotion always picks the **oldest** waiter.
- Ties on `created_at` broken by `id` (deterministic).

Best-effort caveat: a PARALLEL or SKIP job for the same key doesn't queue, so QUEUE FIFO is
"FIFO among QUEUE jobs," which is the intended semantics.

---

## 7. Interaction with retry / timeout / crash

- **Retry (F6):** on FAILED-with-retries-left, Tx-C still **releases the lock** and promotes
  the queue. The retry re-dispatches via Tx-D with `attempt+1` and **re-acquires the lock
  from scratch** (so it queues behind anything that started meanwhile — correct).
- **SLA timeout (F7):** the sweep sets RUNNING→TIMED_OUT, which runs the **same Tx-C**
  (release + promote). A late report is ignored by the terminal guard (Tx-C step 1 WHERE).
- **Crash (F8):** heartbeat sweep sets RUNNING→FAILED(WORKER_LOST) via Tx-C → lock released
  + queue promoted. So a crashed holder never wedges its queue.
- **Backstop promotion sweep:** every N s, for any key that has QUEUED rows but **no lock
  row**, run promotion (covers a promotion missed due to a crash between DELETE lock and
  promote). Query:
  ```sql
  SELECT DISTINCT tenant_name, service_context, job_name FROM job_execution je
   WHERE status='QUEUED'
     AND NOT EXISTS (SELECT 1 FROM execution_lock l
                     WHERE l.tenant_name=je.tenant_name AND l.service_context=je.service_context
                       AND l.job_name=je.job_name);
  -- then promote oldest per key (§5 step 3)
  ```

---

## 8. Race conditions & why each is safe

| Race | Outcome | Why safe |
|---|---|---|
| Two dispatchers dispatch same task | one row, one dispatch | `UNIQUE(flow_execution_id, task_id, attempt)` + `DO NOTHING` |
| Two jobs race for the lock | exactly one RUNNING | `UNIQUE` on lock key + `ON CONFLICT DO NOTHING` (atomic) |
| Completion releases while a fresh dispatch acquires | one holds, other QUEUEs/SKIPs | both do `INSERT ... ON CONFLICT`; one wins |
| Two completions promote same key | at most one promoted to RUNNING | `FOR UPDATE SKIP LOCKED` picks distinct rows; lock `UNIQUE` admits one |
| New QUEUE arrival vs older waiter | older runs first | Tx-D FIFO guard appends behind existing QUEUED |
| Promotion lost (crash between delete-lock & promote) | recovered | backstop promotion sweep (§7) |
| Duplicate `job.execution.completed` | single effect | Tx-C step 1 `WHERE status IN ('RUNNING','QUEUED')` |
| Holder crashes | lock freed, queue drains | crash sweep runs Tx-C |
| Multi-pod dispatch/promote | consistent | all mutations are single-tx atomic INSERT/UPDATE with unique guards |

---

## 9. Worked timelines

Assume job `daily-settlement` (SC=`settlement-service`); a first instance A is RUNNING
(holds the lock). A second fire arrives → instance B.

**SKIP**
```
t0  A RUNNING (lock held by A)
t1  B dispatched → INSERT lock CONFLICT → B = SKIPPED (no dispatch)
t2  A completes → lock released; no QUEUED waiters → nothing to promote
```
**QUEUE**
```
t0  A RUNNING (lock held)
t1  B dispatched → CONFLICT → B = QUEUED
t2  C dispatched → sees QUEUED waiter (B) → C = QUEUED (behind B)
t3  A completes → release lock → promote oldest (B) → B RUNNING + dispatched
t4  B completes → release → promote C → C RUNNING
```
**PARALLEL**
```
t0  A RUNNING (no lock)
t1  B dispatched → RUNNING (no lock) — A and B run together
```

---

## 10. Config

```
# concurrency defaults & bounds
bankos.jobs.concurrency.default=SKIP           # when neither task nor job_definition sets it
bankos.jobs.queue.max-depth=0                  # 0 = unbounded; >0 skips/rejects beyond depth
bankos.jobs.sweep.interval=30                  # also runs the backstop promotion sweep
```

---

## 11. Scope knob (decision)

Default lock key = `(tenant_name, service_context, job_name)` → **one run of a job across
the whole tenant**. Alternative scopes, by changing the lock key + the promotion/queue
predicates consistently:

| Scope | Lock key | Meaning |
|---|---|---|
| **global-per-job** (default) | tenant, service_context, job_name | never run the same job twice at once, anywhere |
| per-flow-definition | + flow_definition_id | same job may run in two *different* flows at once, but not within one flow-def |
| per-flow-execution | + flow_execution_id | only guards re-entrancy within a single run (weakest) |

Pick one; everything in §1/§4/§5 uses that key uniformly.

---

## 12. Open sub-decisions
- **Scope** (§11) — confirm global-per-job.
- **QUEUE bound** — unbounded, or cap `queue.max-depth` (and then SKIP or reject overflow)?
- **PENDING status** — keep as an explicit transient, or set the final status directly in
  Tx-D (skip PENDING)? (PENDING only ever exists inside Tx-D, so it's optional.)
- **Starvation** — with continuous arrivals a QUEUE never idles; acceptable, or add a max
  wait → TIMED_OUT_QUEUED?
