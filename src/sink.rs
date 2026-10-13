use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_sql::{DateTime, DateTimeExt};
use futures::{
    FutureExt, Sink,
    future::{BoxFuture, Shared},
};
use sqlx::MySqlPool;
use ulid::Ulid;

use crate::{CompactType, Config, MySqlStorage, MySqlTask};

type FlushFuture = BoxFuture<'static, Result<(), Arc<sqlx::Error>>>;

/// Insert one already-encoded task using the caller's transaction.
///
/// The caller owns commit/rollback, so its run/outbox state and the queue row
/// can become visible atomically.
pub async fn push_task_in_transaction(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    cfg: &Config,
    task: MySqlTask<CompactType>,
) -> Result<(), sqlx::Error> {
    let id = task
        .parts
        .task_id
        .map(|id| id.to_string())
        .unwrap_or(Ulid::new().to_string());
    let run_at = <DateTime as DateTimeExt>::from_unix_timestamp(task.parts.run_at as i64);
    let max_attempts = task.parts.ctx.max_attempts();
    let priority = task.parts.ctx.priority();
    let args = task.args;
    let job_type = cfg.queue().to_string();
    let meta = serde_json::to_string(&task.parts.ctx.meta()).unwrap_or_default();
    let idempotency_key = task.parts.idempotency_key;

    sqlx::query(include_str!("../queries/task/sink.sql"))
        .bind(args)
        .bind(id)
        .bind(job_type)
        .bind(max_attempts)
        .bind(run_at)
        .bind(priority)
        .bind(meta)
        .bind(idempotency_key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// JSON-encode and insert one typed task using the caller's transaction.
///
/// This matches [`MySqlStorage::new`] / `new_in_queue`, whose default codec is
/// JSON, while keeping commit control with the run/outbox transaction.
pub async fn push_json_task_in_transaction<Args: serde::Serialize>(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    cfg: &Config,
    task: MySqlTask<Args>,
) -> Result<(), sqlx::Error> {
    let task = task.try_map(|args| {
        serde_json::to_vec(&args).map_err(|error| sqlx::Error::Encode(error.into()))
    })?;
    push_task_in_transaction(tx, cfg, task).await
}

/// Sink for pushing tasks into the mysql backend
#[pin_project::pin_project]
#[derive(Debug)]
pub struct MySqlSink<Args, Compact, Codec> {
    pool: MySqlPool,
    config: Config,
    buffer: Vec<MySqlTask<Compact>>,
    #[pin]
    flush_future: Option<Shared<FlushFuture>>,
    _marker: std::marker::PhantomData<(Args, Codec)>,
}

impl<Args, Compact, Codec> Clone for MySqlSink<Args, Compact, Codec> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Push a batch of tasks into the database
pub async fn push_tasks(
    pool: MySqlPool,
    cfg: Config,
    buffer: Vec<MySqlTask<CompactType>>,
) -> Result<(), Arc<sqlx::Error>> {
    let mut tx = pool.begin().await?;
    for task in buffer {
        push_task_in_transaction(&mut tx, &cfg, task).await?;
    }
    tx.commit().await?;

    Ok(())
}

impl<Args, Compact, Codec> MySqlSink<Args, Compact, Codec> {
    /// Create a new MySqlSink
    #[must_use]
    pub fn new(pool: &MySqlPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            _marker: std::marker::PhantomData,
            flush_future: None,
        }
    }
}

impl<Args, Encode, Fetcher> Sink<MySqlTask<CompactType>> for MySqlStorage<Args, Encode, Fetcher>
where
    Args: Send + Sync + 'static,
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: MySqlTask<CompactType>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.project().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        // If there's no existing future and buffer is empty, we're done
        if this.sink.flush_future.is_none() && this.sink.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.sink.flush_future.is_none() && !this.sink.buffer.is_empty() {
            let pool = this.pool.clone();
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.sink.buffer);
            let sink_fut = push_tasks(pool, config, buffer);
            this.sink.flush_future = Some((Box::pin(sink_fut) as FlushFuture).shared());
        }

        // Poll the existing future
        if let Some(mut fut) = this.sink.flush_future.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(Arc::into_inner(e).unwrap()))
                }
                Poll::Pending => {
                    // Future is still pending, put it back and return Pending
                    this.sink.flush_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            // No future and no work to do
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apalis_core::task::{builder::TaskBuilder, task_id::TaskId};
    use apalis_sql::ext::TaskBuilderExt;

    #[tokio::test]
    async fn p3_caller_rollback_hides_job_run_and_outbox_together() {
        let Some(url) = std::env::var("SCHEDULER_ISOLATED_URL")
            .or_else(|_| std::env::var("SCHEDULER_ISOLATED_DATABASE_URL"))
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok()
        else {
            eprintln!("SKIP: isolated mysql is not configured");
            return;
        };
        let pool = MySqlPool::connect(&url).await.unwrap();
        let run_ulid = Ulid::new();
        let run_id = run_ulid.to_string();
        let event_id = Ulid::new().to_string();
        let queue = format!("p3-tx-{}", Ulid::new());
        let config = Config::new(&queue);

        let task: MySqlTask<serde_json::Value> =
            TaskBuilder::<_, crate::MySqlContext, Ulid>::new(serde_json::json!({"p3": true}))
                .priority(3)
                .max_attempts(4)
                .with_task_id(TaskId::new(run_ulid))
                .with_idempotency_key(format!("p3:{run_id}"))
                .build();

        let mut tx = pool.begin().await.unwrap();
        sqlx::query(
            "INSERT INTO scheduled_run \
             (run_id, handler, queue, source, engine_version, \
              dispatch_spec_schema_version, dispatch_spec, dispatch_spec_fingerprint, \
              manual_request_id, state) \
             VALUES (?, 'p3-test', ?, 'manual', 'v2', 1, '{}', REPEAT('0', 64), ?, 'Dispatching')",
        )
        .bind(&run_id)
        .bind(&queue)
        .bind(format!("p3-request-{run_id}"))
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO scheduled_outbox (event_id, run_id, state) VALUES (?, ?, 'Claimed')",
        )
        .bind(&event_id)
        .bind(&run_id)
        .execute(&mut *tx)
        .await
        .unwrap();
        push_json_task_in_transaction(&mut tx, &config, task)
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        for table in ["jobs", "scheduled_run", "scheduled_outbox"] {
            let sql = format!(
                "SELECT COUNT(*) FROM {table} WHERE {} = ?",
                if table == "scheduled_outbox" {
                    "run_id"
                } else {
                    "id"
                }
            );
            let sql = if table == "scheduled_run" {
                sql.replace("WHERE id =", "WHERE run_id =")
            } else {
                sql
            };
            let count: i64 = sqlx::query_scalar(&sql)
                .bind(&run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "{table} escaped caller rollback");
        }
    }
}
