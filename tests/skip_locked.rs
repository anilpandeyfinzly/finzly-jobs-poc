//! Multi-pod claim safety.
//!
//! Simulates several pods (concurrent tasks/connections) all polling the same
//! table with `SELECT ... FOR UPDATE SKIP LOCKED`. Asserts each due row is
//! claimed by exactly one pod — no double-pick, none dropped.
//!
//! Requires a local Postgres. Defaults to the finzly-postgres container;
//! override with TEST_DATABASE_URL.
//!
//! Run: cargo test --test skip_locked -- --nocapture

use std::collections::HashMap;
use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

fn db_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/finzly".to_string())
}

const SEED: i32 = 500; // due rows to claim
const WORKERS: usize = 8; // simulated pods
const BATCH: i64 = 7; // rows claimed per poll

#[tokio::test]
async fn skip_locked_claims_are_disjoint() {
    let pool = PgPoolOptions::new()
        .max_connections((WORKERS as u32) + 2)
        .connect(&db_url())
        .await
        .expect("connect to local postgres (is the finzly-postgres container up?)");
    let pool = Arc::new(pool);

    // Self-contained table so we never touch app data.
    sqlx::query("DROP TABLE IF EXISTS skip_locked_test")
        .execute(&*pool)
        .await
        .unwrap();
    sqlx::query(
        r#"CREATE TABLE skip_locked_test (
               id             INT PRIMARY KEY,
               next_fire_time TIMESTAMPTZ NOT NULL,
               claimed_by     TEXT
           )"#,
    )
    .execute(&*pool)
    .await
    .unwrap();

    // Seed SEED due rows (next_fire_time in the past).
    for id in 0..SEED {
        sqlx::query(
            "INSERT INTO skip_locked_test (id, next_fire_time) \
             VALUES ($1, now() - INTERVAL '1 second')",
        )
        .bind(id)
        .execute(&*pool)
        .await
        .unwrap();
    }

    // Each worker = one pod: loop claiming batches until nothing is left.
    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut mine: Vec<i32> = Vec::new();
            loop {
                let mut tx = pool.begin().await.unwrap();
                let rows = sqlx::query(
                    r#"SELECT id FROM skip_locked_test
                       WHERE claimed_by IS NULL AND next_fire_time <= now()
                       ORDER BY id
                       FOR UPDATE SKIP LOCKED
                       LIMIT $1"#,
                )
                .bind(BATCH)
                .fetch_all(&mut *tx)
                .await
                .unwrap();

                if rows.is_empty() {
                    tx.commit().await.unwrap();
                    break;
                }

                for row in &rows {
                    let id: i32 = row.get("id");
                    sqlx::query("UPDATE skip_locked_test SET claimed_by = $1 WHERE id = $2")
                        .bind(format!("pod-{w}"))
                        .bind(id)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    mine.push(id);
                }
                tx.commit().await.unwrap();
            }
            mine
        }));
    }

    // Collect what each pod claimed.
    let mut all: Vec<i32> = Vec::new();
    let mut per_pod: Vec<usize> = Vec::new();
    for h in handles {
        let claimed = h.await.unwrap();
        per_pod.push(claimed.len());
        all.extend(claimed);
    }
    println!("claims per pod: {per_pod:?} (total {})", all.len());

    // 1) No id claimed by more than one pod.
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for id in &all {
        *counts.entry(*id).or_default() += 1;
    }
    let dups: Vec<i32> = counts
        .iter()
        .filter(|&(_, &c)| c > 1)
        .map(|(&id, _)| id)
        .collect();
    assert!(dups.is_empty(), "rows claimed by >1 pod: {dups:?}");

    // 2) Exactly the seeded set was claimed, none dropped.
    assert_eq!(all.len(), SEED as usize, "claimed count != seeded count");

    // 3) DB agrees: nothing left unclaimed, distinct ids == SEED.
    let unclaimed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM skip_locked_test WHERE claimed_by IS NULL")
            .fetch_one(&*pool)
            .await
            .unwrap();
    assert_eq!(unclaimed, 0, "some rows never claimed");

    let distinct: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT id) FROM skip_locked_test WHERE claimed_by IS NOT NULL",
    )
    .fetch_one(&*pool)
    .await
    .unwrap();
    assert_eq!(distinct, SEED as i64);

    sqlx::query("DROP TABLE skip_locked_test")
        .execute(&*pool)
        .await
        .unwrap();

    println!("OK: {SEED} rows claimed exactly once across {WORKERS} pods, zero double-picks");
}
