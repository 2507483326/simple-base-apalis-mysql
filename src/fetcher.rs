use std::{
    collections::{HashMap, HashSet, VecDeque},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
    task::{Context, Poll},
};

use apalis_core::{
    backend::poll_strategy::{PollContext, PollStrategyExt},
    task::Task,
    worker::context::WorkerContext,
};
use apalis_sql::{DateTime, DateTimeExt, TaskRow, config::Config};
use futures::{FutureExt, future::BoxFuture, stream::Stream};
use pin_project::pin_project;
use sqlx::{MySql, MySqlPool, Pool};
use ulid::Ulid;

use crate::{ClaimGeneration, CompactType, MySqlContext, MySqlTask, from_row::MySqlTaskRow};

/// Fetch the next batch of tasks from the mysql backend
pub async fn fetch_next(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
) -> Result<Vec<Task<CompactType, MySqlContext, Ulid>>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let lock_at = DateTime::now();
    let job_type = config.queue().to_string();
    let buffer_size = config.buffer_size() as i32;
    let worker = worker.name().clone();

    let rows = sqlx::query_as::<_, MySqlTaskRow>(include_str!("../queries/backend/fetch_next.sql"))
        .bind(job_type)
        .bind(lock_at)
        .bind(buffer_size)
        .fetch_all(&mut *tx)
        .await?;

    if rows.is_empty() {
        tx.commit().await?;
        return Ok(Vec::new());
    }

    let ids: HashSet<_> = rows.iter().filter_map(|r| r.id.clone()).collect();

    let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let update_query = format!(
        "UPDATE jobs SET status = 'Queued', lock_by = ?, lock_at = ?, \
         claim_generation = claim_generation + 1 WHERE id IN ({})",
        placeholders
    );

    let mut query = sqlx::query(&update_query).bind(&worker).bind(lock_at);

    for id in &ids {
        query = query.bind(id);
    }

    query.execute(&mut *tx).await?;

    // Read the incremented tokens in the same transaction. `MySqlTaskRow` is
    // deliberately left compatible with the upstream query helpers; the
    // generation is runtime-only data needed by lock/ack middleware.
    let generation_query = format!(
        "SELECT id, claim_generation FROM jobs WHERE id IN ({})",
        placeholders
    );
    let mut query = sqlx::query(&generation_query);
    for id in &ids {
        query = query.bind(id);
    }
    let generations: HashMap<String, u64> = query
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| {
            use sqlx::Row;
            Ok((row.try_get("id")?, row.try_get("claim_generation")?))
        })
        .collect::<Result<_, sqlx::Error>>()?;

    // Convert rows to tasks
    let res = rows
        .into_iter()
        .map(|r| {
            let id =
                r.id.clone()
                    .ok_or_else(|| sqlx::Error::Protocol("Missing id".into()))?;
            let mut row: TaskRow = r.try_into()?;
            row.lock_by = Some(worker.clone());
            let mut task = row
                .try_into_task_compact()
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            let generation = generations
                .get(&id)
                .copied()
                .ok_or_else(|| sqlx::Error::Protocol("Missing claim_generation".into()))?;
            task.parts.data.insert(ClaimGeneration(generation));
            Ok(task)
        })
        .collect();

    tx.commit().await?;
    res
}

enum StreamState {
    Ready,
    Delay,
    Fetch(BoxFuture<'static, Result<Vec<MySqlTask<CompactType>>, sqlx::Error>>),
    Buffered(VecDeque<MySqlTask<CompactType>>),
    Empty,
}

/// Dispatcher for fetching tasks from a MySQL backend via [MySqlPollFetcher]
#[derive(Clone, Debug)]
pub struct MySqlFetcher;
/// Polling-based fetcher for retrieving tasks from a MySQL backend
#[pin_project]
pub struct MySqlPollFetcher<Compact, Decode> {
    pool: MySqlPool,
    config: Config,
    wrk: WorkerContext,
    _marker: PhantomData<(Compact, Decode)>,
    #[pin]
    state: StreamState,

    #[pin]
    delay_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>>,

    prev_count: Arc<AtomicUsize>,
}

impl<Compact, Decode> std::fmt::Debug for MySqlPollFetcher<Compact, Decode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlPollFetcher")
            .field("pool", &self.pool)
            .field("config", &self.config)
            .field("wrk", &self.wrk)
            .field("_marker", &self._marker)
            .field("prev_count", &self.prev_count)
            .finish()
    }
}

impl<Compact, Decode> Clone for MySqlPollFetcher<Compact, Decode> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Decode> MySqlPollFetcher<CompactType, Decode> {
    /// Create a new MySqlPollFetcher
    #[must_use]
    pub fn new(pool: &Pool<MySql>, config: &Config, wrk: &WorkerContext) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Decode> Stream for MySqlPollFetcher<CompactType, Decode> {
    type Item = Result<Option<MySqlTask<CompactType>>, sqlx::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.delay_stream.is_none() {
            let strategy = this
                .config
                .poll_strategy()
                .clone()
                .build_stream(&PollContext::new(this.wrk.clone(), this.prev_count.clone()));
            this.delay_stream = Some(Box::pin(strategy));
        }

        loop {
            match this.state {
                StreamState::Ready => {
                    let stream =
                        fetch_next(this.pool.clone(), this.config.clone(), this.wrk.clone());
                    this.state = StreamState::Fetch(stream.boxed());
                }
                StreamState::Delay => {
                    if let Some(delay_stream) = this.delay_stream.as_mut() {
                        match delay_stream.as_mut().poll_next(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Some(_)) => {
                                this.state = StreamState::Ready;
                            }
                            Poll::Ready(None) => {
                                this.state = StreamState::Empty;
                                return Poll::Ready(None);
                            }
                        }
                    } else {
                        this.state = StreamState::Empty;
                        return Poll::Ready(None);
                    }
                }

                StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(item) => match item {
                        Ok(requests) => {
                            if requests.is_empty() {
                                this.state = StreamState::Delay;
                            } else {
                                let mut buffer = VecDeque::new();
                                for request in requests {
                                    buffer.push_back(request);
                                }

                                this.state = StreamState::Buffered(buffer);
                            }
                        }
                        Err(e) => {
                            this.state = StreamState::Empty;
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                },

                StreamState::Buffered(ref mut buffer) => {
                    if let Some(request) = buffer.pop_front() {
                        // Yield the next buffered item
                        if buffer.is_empty() {
                            // Buffer is now empty, transition to ready for next fetch
                            this.state = StreamState::Ready;
                        }
                        return Poll::Ready(Some(Ok(Some(request))));
                    } else {
                        // Buffer is empty, transition to ready
                        this.state = StreamState::Ready;
                    }
                }

                StreamState::Empty => return Poll::Ready(None),
            }
        }
    }
}

impl<Compact, Decode> MySqlPollFetcher<Compact, Decode> {
    /// Take pending tasks from the fetcher
    pub fn take_pending(&mut self) -> VecDeque<MySqlTask<Vec<u8>>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    #[tokio::test]
    async fn p3_fetch_increments_and_attaches_claim_generation() {
        let Some(url) = std::env::var("SCHEDULER_ISOLATED_URL")
            .or_else(|_| std::env::var("SCHEDULER_ISOLATED_DATABASE_URL"))
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok()
        else {
            eprintln!("SKIP: isolated mysql is not configured");
            return;
        };
        let pool = MySqlPool::connect(&url).await.unwrap();
        let id = Ulid::new().to_string();
        let queue = format!("p3-fetch-{}", Ulid::new());
        let worker_name = format!("{}-p3", Ulid::new());

        sqlx::query(
            "INSERT INTO workers \
             (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES (?, ?, 'P3Test', '', NOW(), NOW())",
        )
        .bind(&worker_name)
        .bind(&queue)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO jobs \
             (id, job_type, status, attempts, max_attempts, run_at, priority, job, \
              claim_generation) \
             VALUES (?, ?, 'Pending', 0, 3, NOW(), 0, '{}', 0)",
        )
        .bind(&id)
        .bind(&queue)
        .execute(&pool)
        .await
        .unwrap();

        let tasks = fetch_next(
            pool.clone(),
            Config::new(&queue),
            WorkerContext::new::<()>(&worker_name),
        )
        .await
        .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].parts.data.get::<ClaimGeneration>(),
            Some(&ClaimGeneration(1))
        );

        let row = sqlx::query("SELECT status, lock_by, claim_generation FROM jobs WHERE id=?")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("status"), "Queued");
        assert_eq!(row.get::<String, _>("lock_by"), worker_name);
        assert_eq!(row.get::<u64, _>("claim_generation"), 1);

        sqlx::query("DELETE FROM jobs WHERE id=?")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM workers WHERE id=?")
            .bind(&worker_name)
            .execute(&pool)
            .await
            .unwrap();
    }
}
