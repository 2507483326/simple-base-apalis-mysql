use apalis_core::backend::{BackendExt, Metrics, Statistic};
use ulid::Ulid;

use crate::{CompactType, MySqlContext, MySqlStorage};

#[derive(sqlx::FromRow)]
struct StatisticRow {
    /// The priority of the statistic (lower number means higher priority)
    pub priority: i64,
    /// The statistics type
    pub r#type: String,
    /// Overall statistics of the backend
    pub statistic: String,
    /// The value of the statistic
    pub value: Option<f64>,
}

impl<Args, D, F> Metrics for MySqlStorage<Args, D, F>
where
    Self: BackendExt<
            Context = MySqlContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = sqlx::Error,
        >,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.pool.clone();
        async move {
            let rec = sqlx::query_as::<_, StatisticRow>(include_str!(
                "../../queries/backend/overview.sql"
            ))
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| Statistic {
                priority: Some(r.priority as u64),
                stat_type: super::stat_type_from_string(&r.r#type),
                title: r.statistic,
                value: r.value.unwrap_or_default().to_string(),
            })
            .collect();
            Ok(rec)
        }
    }
    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let queue_id = self.config().queue().to_string();
        async move {
            let mut query = sqlx::query_as::<_, StatisticRow>(include_str!(
                "../../queries/backend/overview_by_queue.sql"
            ));
            // The upstream statement uses the queue in each UNION branch.
            for _ in 0..27 {
                query = query.bind(&queue_id);
            }
            let rec = query
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(|r| Statistic {
                    priority: Some(r.priority as u64),
                    stat_type: super::stat_type_from_string(&r.r#type),
                    title: r.statistic,
                    value: r.value.unwrap_or_default().to_string(),
                })
                .collect();
            Ok(rec)
        }
    }
}
