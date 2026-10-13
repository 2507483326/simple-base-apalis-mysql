use apalis_core::worker::context::WorkerContext;
use sqlx::MySqlPool;

use crate::Config;

/// Register a new worker in the database
pub async fn register_worker(
    pool: MySqlPool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    let worker_id = worker.name().to_owned();
    let queue = config.queue().to_string();
    let layers = worker.get_service().to_owned();
    let keep_alive = config.keep_alive().as_secs() as i64;
    let res = sqlx::query(include_str!("../../queries/backend/register_worker.sql"))
        .bind(&worker_id)
        .bind(queue)
        .bind(storage_type)
        .bind(layers)
        .bind(&worker_id)
        .bind(keep_alive)
        .execute(&pool)
        .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "WORKER_ALREADY_EXISTS",
        )));
    }
    Ok(())
}
