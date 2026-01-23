use anyhow::{Result, anyhow};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;
use tracing::info;

/// Encapsulates a PostgreSQL connection pool.
/// Internally this is an async, thread-safe connection pool that can be shared across tasks.
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Creates a new Database instance asynchronously.
    ///
    /// # Arguments
    ///
    /// * `database_url` - PostgreSQL connection string
    ///
    /// # Example
    ///
    /// ```rust
    /// let db = Database::new("postgres://user:pass@localhost/db").await?;
    /// let pool = db.pool();
    /// ```
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await?;

        info!("Connected to Postgres");

        Ok(Self { pool })
    }

    /// Checks if a table exists in the PostgreSQL database.
    /// Returns an error if the table does not exist.
    pub async fn check_table(&self, table_name: &str) -> Result<()> {
        // Query returns a single row with a boolean
        let table_exists: (bool,) = sqlx::query_as(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = $1
            )
            "#,
        )
        .bind(table_name)
        .fetch_one(&self.pool)
        .await?;

        if !table_exists.0 {
            return Err(anyhow!(
                "Table '{}' does not exist in the database",
                table_name
            ));
        }

        info!("Table {} OK", table_name);

        Ok(())
    }

    /// Returns a reference to the internal connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
