use apalis_core::{
    error::{AbortError, BoxDynError},
    layers::{Layer, Service},
    task::{Parts, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use futures::{FutureExt, future::BoxFuture};
use serde::Serialize;
use sqlx::MySqlPool;
use ulid::Ulid;

use crate::{ClaimGeneration, MySqlContext, MySqlTask};

const ACK_SQL: &str = r#"
UPDATE jobs
SET status = ?,
    attempts = ?,
    last_result = ?,
    done_at = NOW(3),
    attempt_exited_at = NOW(3),
    terminal_origin = CASE WHEN ? IN ('Done', 'Killed') THEN 'ack' ELSE 'unknown' END,
    run_at = CASE
        WHEN ? = 'Failed' THEN TIMESTAMPADD(
            SECOND,
            LEAST(CAST(POW(2, GREATEST(? - 1, 0)) * 10 AS UNSIGNED), 3600),
            NOW(3)
        )
        ELSE run_at
    END
WHERE id = ?
  AND lock_by = ?
  AND claim_generation = ?
"#;

const LOCK_SQL: &str = r#"
UPDATE jobs
SET status = 'Running', lock_at = NOW(3), lock_by = ?
WHERE id = ?
  -- Poll storage pre-assigns the worker; shared storage assigns it here.
  AND (lock_by = ? OR lock_by IS NULL)
  AND claim_generation = ?
  AND (
      status IN ('Queued', 'Pending')
      OR (status = 'Failed' AND attempts < max_attempts)
  )
"#;

async fn acknowledge_claim(
    pool: &MySqlPool,
    task_id: String,
    worker_id: String,
    generation: ClaimGeneration,
    status: String,
    attempt: i32,
    result: String,
) -> Result<(), sqlx::Error> {
    let res = sqlx::query(ACK_SQL)
        .bind(&status)
        .bind(attempt)
        .bind(result)
        .bind(&status)
        .bind(&status)
        .bind(attempt)
        .bind(&task_id)
        .bind(&worker_id)
        .bind(generation.0)
        .execute(pool)
        .await?;

    if res.rows_affected() == 0 {
        log::warn!(
            "ack CAS rejected for task {task_id}, worker {worker_id}, claim_generation {}",
            generation.0
        );
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct MySqlAck {
    pool: MySqlPool,
}
impl MySqlAck {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

impl<Res: Serialize + 'static> Acknowledge<Res, MySqlContext, Ulid> for MySqlAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<MySqlContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id;
        let worker_id = parts.ctx.lock_by().clone();
        let generation = parts.data.get::<ClaimGeneration>().copied();

        // Workflows need special handling to serialize the response correctly
        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()));

        let status = calculate_status(parts, res);
        parts.status.store(status.clone());
        let attempt = parts.attempt.current() as i32;
        let pool = self.pool.clone();
        let res = response.map_err(|e| sqlx::Error::Decode(e.into()));
        let status = status.to_string();
        async move {
            let task_id = task_id
                .ok_or(sqlx::Error::ColumnNotFound("TASK_ID_FOR_ACK".to_owned()))?
                .to_string();
            let worker_id =
                worker_id.ok_or(sqlx::Error::ColumnNotFound("WORKER_ID_LOCK_BY".to_owned()))?;
            let res_ok = res?;
            let generation = generation.ok_or_else(|| {
                sqlx::Error::ColumnNotFound("CLAIM_GENERATION_FOR_ACK".to_owned())
            })?;
            acknowledge_claim(
                &pool, task_id, worker_id, generation, status, attempt, res_ok,
            )
            .await
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    async fn test_pool() -> Option<MySqlPool> {
        let url = std::env::var("SCHEDULER_ISOLATED_URL")
            .or_else(|_| std::env::var("SCHEDULER_ISOLATED_DATABASE_URL"))
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok()?;
        Some(
            MySqlPool::connect(&url)
                .await
                .expect("connect isolated mysql"),
        )
    }

    async fn insert_claim(pool: &MySqlPool, id: &str, worker: &str, generation: u64) {
        sqlx::query(
            "INSERT INTO workers \
             (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES (?, 'p3-test', 'P3Test', '', NOW(), NOW()) \
             ON DUPLICATE KEY UPDATE last_seen=NOW()",
        )
        .bind(worker)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO jobs \
             (id, job_type, status, attempts, max_attempts, run_at, priority, job, \
              lock_by, lock_at, claim_generation) \
             VALUES (?, 'p3-test', 'Running', 0, 3, NOW(), 0, '{}', ?, NOW(), ?)",
        )
        .bind(id)
        .bind(worker)
        .bind(generation)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn p3_stale_ack_cannot_overwrite_new_claim_even_with_same_worker_name() {
        let Some(pool) = test_pool().await else {
            eprintln!("SKIP: isolated mysql is not configured");
            return;
        };
        let id = Ulid::new().to_string();
        let worker = format!("{}-p3", Ulid::new());
        insert_claim(&pool, &id, &worker, 1).await;

        sqlx::query(
            "UPDATE jobs SET status='Running', claim_generation=2, last_result=NULL WHERE id=?",
        )
        .bind(&id)
        .execute(&pool)
        .await
        .unwrap();

        let err = acknowledge_claim(
            &pool,
            id.clone(),
            worker.clone(),
            ClaimGeneration(1),
            "Done".to_owned(),
            1,
            r#"{"Ok":null}"#.to_owned(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, sqlx::Error::RowNotFound));

        let row = sqlx::query(
            "SELECT status, claim_generation, last_result, terminal_origin FROM jobs WHERE id=?",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "Running");
        assert_eq!(row.get::<u64, _>("claim_generation"), 2);
        assert_eq!(row.get::<Option<String>, _>("last_result"), None);
        assert_eq!(row.get::<String, _>("terminal_origin"), "unknown");

        sqlx::query("DELETE FROM jobs WHERE id=?")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM workers WHERE id=?")
            .bind(&worker)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn p3_failed_ack_sets_backoff_and_exit_evidence_atomically() {
        let Some(pool) = test_pool().await else {
            eprintln!("SKIP: isolated mysql is not configured");
            return;
        };
        let id = Ulid::new().to_string();
        let worker = format!("{}-p3", Ulid::new());
        insert_claim(&pool, &id, &worker, 7).await;

        acknowledge_claim(
            &pool,
            id.clone(),
            worker.clone(),
            ClaimGeneration(7),
            "Failed".to_owned(),
            1,
            r#"{"Err":"boom"}"#.to_owned(),
        )
        .await
        .unwrap();

        let row = sqlx::query(
            "SELECT status, attempts, terminal_origin, attempt_exited_at IS NOT NULL AS exited, \
                    TIMESTAMPDIFF(SECOND, NOW(3), run_at) AS delay_secs \
             FROM jobs WHERE id=?",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "Failed");
        assert_eq!(row.get::<i32, _>("attempts"), 1);
        assert_eq!(row.get::<String, _>("terminal_origin"), "unknown");
        assert_eq!(row.get::<i64, _>("exited"), 1);
        assert!((8..=10).contains(&row.get::<i64, _>("delay_secs")));

        sqlx::query("DELETE FROM jobs WHERE id=?")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM workers WHERE id=?")
            .bind(&worker)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn p3_lock_and_done_ack_require_same_generation_and_record_origin() {
        let Some(pool) = test_pool().await else {
            eprintln!("SKIP: isolated mysql is not configured");
            return;
        };
        let id = Ulid::new().to_string();
        let parsed_id = id.parse::<Ulid>().unwrap();
        let worker = format!("{}-p3", Ulid::new());
        insert_claim(&pool, &id, &worker, 11).await;
        sqlx::query("UPDATE jobs SET status='Queued' WHERE id=?")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();

        let stale = lock_task(&pool, &parsed_id, &worker, ClaimGeneration(10))
            .await
            .unwrap_err();
        assert!(matches!(stale, sqlx::Error::RowNotFound));
        lock_task(&pool, &parsed_id, &worker, ClaimGeneration(11))
            .await
            .unwrap();
        acknowledge_claim(
            &pool,
            id.clone(),
            worker.clone(),
            ClaimGeneration(11),
            "Done".to_owned(),
            1,
            r#"{"Ok":null}"#.to_owned(),
        )
        .await
        .unwrap();

        let row = sqlx::query(
            "SELECT status, terminal_origin, attempt_exited_at IS NOT NULL AS exited \
             FROM jobs WHERE id=?",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("status"), "Done");
        assert_eq!(row.get::<String, _>("terminal_origin"), "ack");
        assert_eq!(row.get::<i64, _>("exited"), 1);

        sqlx::query("DELETE FROM jobs WHERE id=?")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM workers WHERE id=?")
            .bind(&worker)
            .execute(&pool)
            .await
            .unwrap();
    }
}

pub(crate) fn calculate_status<Res>(
    parts: &Parts<MySqlContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match &res {
        Ok(_) => Status::Done,
        Err(e) => match e {
            _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
            _ => Status::Failed,
        },
    }
}

pub(crate) async fn lock_task(
    pool: &MySqlPool,
    task_id: &Ulid,
    worker_id: &str,
    generation: ClaimGeneration,
) -> Result<(), sqlx::Error> {
    let task_id = task_id.to_string();
    let res = sqlx::query(LOCK_SQL)
        .bind(worker_id)
        .bind(&task_id)
        .bind(worker_id)
        .bind(generation.0)
        .execute(pool)
        .await?;

    if res.rows_affected() == 0 {
        log::warn!(
            "lock CAS rejected for task {task_id}, worker {worker_id}, claim_generation {}",
            generation.0
        );
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct LockTaskLayer {
    pool: MySqlPool,
}

impl LockTaskLayer {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService {
            inner,
            pool: self.pool.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LockTaskService<S> {
    inner: S,
    pool: MySqlPool,
}

impl<S, Args> Service<MySqlTask<Args>> for LockTaskService<S>
where
    S: Service<MySqlTask<Args>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, mut req: MySqlTask<Args>) -> Self::Future {
        let pool = self.pool.clone();
        let worker_id = req
            .parts
            .data
            .get::<WorkerContext>()
            .map(|w| w.name().to_owned())
            .unwrap();
        let parts = &req.parts;
        let task_id = match &parts.task_id {
            Some(id) => *id.inner(),
            None => {
                return async {
                    Err(sqlx::Error::ColumnNotFound("TASK_ID_FOR_LOCK".to_owned()).into())
                }
                .boxed();
            }
        };
        let generation = match parts.data.get::<ClaimGeneration>().copied() {
            Some(generation) => generation,
            None => {
                return async {
                    Err(sqlx::Error::ColumnNotFound("CLAIM_GENERATION_FOR_LOCK".to_owned()).into())
                }
                .boxed();
            }
        };
        req.parts.ctx = req.parts.ctx.with_lock_by(Some(worker_id.clone()));
        let fut = self.inner.call(req);
        async move {
            lock_task(&pool, &task_id, &worker_id, generation).await?;

            fut.await.map_err(|e| e.into())
        }
        .boxed()
    }
}
