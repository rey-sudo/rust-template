use anyhow::Result;
use chrono::{TimeZone, Timelike, Utc};
use pulsar::{Pulsar, TokioExecutor, producer};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_retry::{
    Retry,
    strategy::{ExponentialBackoff, jitter},
};
use tracing::{error, info, warn};

use crate::{
    application::clients::client::{AnyClient, create_client},
    common::{candle::Candle, tick::Tick},
    config::Config,
    infrastructure::database::Database,
};

/// Commands sent from dispatcher to symbol worker
pub enum SymbolCommand {
    Tick(Tick),
    Shutdown,
}

/// Converts a Unix timestamp in milliseconds to the start of its minute.
/// Sets seconds and nanoseconds to zero, returning a timestamp aligned
/// to the 1-minute boundary.
///
/// # Example
/// ```
/// let ts = 1_682_000_123_456; // 2023-04-01 12:34:56.789 UTC
/// let minute_ts = ts_to_minute(ts);
/// assert_eq!(minute_ts, 1_682_000_120_000); // 2023-04-01 12:34:00.000 UTC
/// ```
fn ts_to_minute(ts_millis: i64) -> i64 {
    let dt: chrono::DateTime<Utc> = Utc
        .timestamp_millis_opt(ts_millis)
        .single()
        .expect("invalid tick timestamp");

    dt.with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
        .timestamp_millis()
}

/// Spawns a dedicated asynchronous worker responsible for consuming and processing
/// market data ticks for a single trading symbol.
///
/// This worker acts as an isolated, single-threaded actor with exclusive ownership
/// of the symbol’s state. Its responsibilities include:
///
/// - Performing an initial backfill of historical 1-minute OHLCV candles.
///   Client as data provider.
/// - Maintaining the currently active 1-minute candle entirely in memory.
/// - Consuming ticks sequentially from an MPSC channel to ensure deterministic
///   candle construction with no concurrency races.
/// - Detecting minute boundaries and, on rollover:
///   - Closing the current candle,
///   - Persisting it to the database,
///   - Publishing the closed candle to Pulsar,
///   - Initializing and publishing the next live candle.
/// - Publishing live (in-progress) candle updates on every tick.
/// - Handling graceful shutdown via an explicit `Shutdown` command.
///
/// Error handling semantics:
/// - Any unrecoverable error (backfill, database, Pulsar, or client failure)
///   causes the worker task to terminate.
/// - Failure results in a clean exit, allowing the dispatcher to observe, log, and
///   optionally respawn the worker.
///
/// Design guarantees:
/// - Exactly one worker exists per symbol at any time.
/// - All tick processing for a symbol is strictly ordered.
/// - No shared mutable state exists across workers.
/// - Partial candles are never persisted; only fully closed 1-minute candles
///   are written to durable storage.
///
/// Returns a `JoinHandle` that resolves to `Result<()>`, allowing the caller
/// to await task completion and surface any fatal worker errors.
pub fn spawn_symbol_worker(
    symbol: String,
    mut rx: mpsc::Receiver<SymbolCommand>,
    db: Database,
    pulsar: Pulsar<TokioExecutor>,
    config: Config,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        info!("Symbol worker started for {}", symbol);
        // Create a concrete market data client (e.g. Binance, Databento) based on runtime
        // configuration `client_id`. The worker depends only on the `AnyClient` trait, keeping it
        // fully decoupled from specific data provider implementations.
        let client: Box<dyn AnyClient> = create_client(&config)?;

        let _last_closed_minute: i64 =
            backfill_and_init(&symbol, &db.pool(), client.as_ref()).await?;

        // Create a non-persistent Pulsar producer for publishing live (in-progress)
        // 1-minute candles. Messages sent on this topic represent the current state
        // of the active candle and are not intended for durable storage.
        let mut live_producer: pulsar::Producer<TokioExecutor> = pulsar
            .producer()
            .with_topic("non-persistent://public/market-data/ohlcv-1m-live")
            .build()
            .await?;

        // Create a non-persistent Pulsar producer for publishing closed (finalized)
        // 1-minute candles. Messages on this topic are emitted exactly once per
        // completed minute and represent immutable OHLCV data.
        let mut closed_producer: pulsar::Producer<TokioExecutor> = pulsar
            .producer()
            .with_topic("non-persistent://public/market-data/ohlcv-1m-closed")
            .build()
            .await?;

        // Holds the currently active 1-minute candle for this symbol.
        // Starts as `None` because no ticks have been processed yet.
        // Will be updated on each incoming tick and replaced when a new minute begins.
        let mut current_candle: Option<Candle> = None;

        // Main event loop of the symbol worker.
        // Continuously receives commands from the dispatcher via the channel.
        //
        // Commands can be:
        // - `SymbolCommand::Tick(tick)` → process an incoming market tick
        // - `SymbolCommand::Shutdown` → stop the worker gracefully
        //
        // Tick processing logic:
        // 1. Convert tick timestamp to the start of its minute.
        // 2. Update the current candle if it's the same minute.
        // 3. If the minute has rolled over, persist and publish the closed candle,
        //    then start a new candle for the new minute.
        // 4. If this is the first tick ever, initialize the first candle.
        //
        // Shutdown handling:
        // - Logs a message and exits the loop, allowing the worker to stop cleanly.
        while let Some(cmd) = rx.recv().await {
            match cmd {
                SymbolCommand::Tick(tick) => {
                    let tick_minute_ts: i64 = ts_to_minute(tick.ts);

                    match &mut current_candle {
                        // Same minute → update candle
                        // Specific case
                        Some(candle) if candle.open_time == tick_minute_ts => {
                            candle.update(&tick);
                            publish_live(&mut live_producer, &symbol, candle).await?;
                        }

                        // Minute rollover → close current, start new
                        // General case
                        Some(candle) => {
                            persist_closed(&db.pool(), &symbol, candle).await?;
                            publish_closed(&mut closed_producer, &symbol, candle).await?;

                            let new_candle: Candle = Candle::new(&symbol, &tick, tick_minute_ts);

                            publish_live(&mut live_producer, &symbol, &new_candle).await?;
                            current_candle = Some(new_candle);
                        }

                        // First tick ever for this symbol
                        // Base case
                        None => {
                            let candle: Candle = Candle::new(&symbol, &tick, tick_minute_ts);

                            publish_live(&mut live_producer, &symbol, &candle).await?;
                            current_candle = Some(candle);
                        }
                    }
                }

                SymbolCommand::Shutdown => {
                    info!("Shutdown signal for symbol {}", symbol);
                    break;
                }
            }
        }

        info!("Symbol worker stopped for {}", symbol);
        Ok(())
    })
}

/// Backfill and initialize 1m candles for a given symbol.
/// Returns the timestamp (Unix ms) of the last closed candle.
pub async fn backfill_and_init(
    symbol: &str,
    db: &sqlx::Pool<sqlx::Postgres>,
    client: &dyn AnyClient,
) -> Result<i64> {
    info!("Backfill/init for {}", symbol);

    // Query the database for the most recent closed 1-minute candle for this symbol.
    // If present, its `close_time` is used as the starting point for historical
    // backfill; otherwise, backfill will start from the provider’s earliest data.
    let last_close_time: Option<i64> = query_last_closed(symbol, db).await?;

    let start_ms: Option<i64> = last_close_time;
    let end_ms: i64 = Utc::now().timestamp_millis();

    // Fetch candles from the client (Binance, etc.)
    let candles: Vec<Candle> = client
        .fetch_ohlcv_1m(symbol, start_ms, Some(end_ms), 1000) // max 1000 per request
        .await?;

    if candles.is_empty() {
        warn!("No candles fetched for {}", symbol);
        // fallback: return last close if exists, otherwise current time
        return Ok(last_close_time.unwrap_or(end_ms));
    }

    info!("Fetched {} candles", candles.len());

    // Persist candles in DB
    persist_candle_history(symbol, &candles, db).await?;

    // Return the timestamp of the last closed candle
    let last_close_ms: i64 = candles.last().expect("checked non-empty").close_time;

    Ok(last_close_ms)
}

/// Queries the database for the most recent closed 1-minute candle for a symbol.
/// Returns the `close_time` (Unix ms) of the latest persisted candle, or `None`
/// if the symbol has no historical data stored yet.
async fn query_last_closed(symbol: &str, db: &sqlx::Pool<sqlx::Postgres>) -> Result<Option<i64>> {
    let last_close: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT close_time
        FROM ohlcv_1m
        WHERE symbol = $1
        ORDER BY close_time DESC
        LIMIT 1
        "#,
    )
    .bind(symbol)
    .fetch_optional(db)
    .await?;

    Ok(last_close)
}

/// Persists a batch of historical 1-minute candles to the database.
///
/// This function is used during the initial backfill phase to store
/// already-closed candles fetched from an external data provider.
/// Inserts are performed idempotently using `ON CONFLICT DO NOTHING`,
/// allowing safe re-execution without creating duplicates.
///
/// Database writes are executed inside a single transaction and wrapped
/// in a bounded retry strategy to tolerate transient failures. If all
/// retries are exhausted, the error is propagated to the caller.
///
/// Only fully closed candles are expected as input; live or partial
/// candles must not be passed to this function.
pub async fn persist_candle_history(
    symbol: &str,
    candles: &[Candle],
    db: &sqlx::Pool<sqlx::Postgres>,
) -> Result<()> {
    if candles.is_empty() {
        return Ok(());
    }

    let retry_strategy = ExponentialBackoff::from_millis(200).map(jitter).take(3);

    Retry::spawn(retry_strategy, || async {
        let mut tx: sqlx::Transaction<'_, sqlx::Postgres> = db.begin().await?;

        for candle in candles {
            sqlx::query(
                r#"
                INSERT INTO ohlcv_1m (
                    symbol,
                    open_time,
                    close_time,
                    open,
                    high,
                    low,
                    close,
                    volume
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, open_time) DO NOTHING
                "#,
            )
            .bind(symbol)
            .bind(candle.open_time)
            .bind(candle.close_time)
            .bind(candle.open)
            .bind(candle.high)
            .bind(candle.low)
            .bind(candle.close)
            .bind(candle.volume)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|e: anyhow::Error| {
        error!(
            symbol = %symbol,
            candle_count = candles.len(),
            error = %e,
            "Failed to persist candle history after retries"
        );
        e
    })?;

    info!("Persisted {} 1m candles for {}", candles.len(), symbol);

    Ok(())
}

/// Persists a finalized (closed) 1-minute candle to the database.
/// This function is idempotent: if the candle already exists,
/// the insert is ignored to safely handle replays or retries.
/// Any database error is propagated to the caller, allowing the
/// worker to fail fast on unrecoverable persistence failures.
async fn persist_closed(
    db: &sqlx::Pool<sqlx::Postgres>,
    symbol: &str,
    candle: &Candle,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO ohlcv_1m (
            symbol,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (symbol, open_time) DO NOTHING
        "#,
    )
    .bind(symbol)
    .bind(candle.open_time)
    .bind(candle.close_time)
    .bind(candle.open)
    .bind(candle.high)
    .bind(candle.low)
    .bind(candle.close)
    .bind(candle.volume)
    .execute(db)
    .await?;

    info!("Persisted CLOSED candle {} @ {}", symbol, candle.close_time);

    Ok(())
}

/// Publishes the current (still open) 1-minute candle with the live producer.
/// This is called on every tick update and emits non-finalized candle data
/// for real-time consumers.
async fn publish_live(
    producer: &mut producer::Producer<TokioExecutor>,
    symbol: &str,
    candle: &Candle,
) -> Result<()> {
    let fut: producer::SendFuture = producer.send_non_blocking(candle.clone()).await?;
    fut.await?;
    info!("Publish LIVE candle {} @ {}", symbol, candle.close);
    Ok(())
}

/// Publishes a finalized (closed) 1-minute candle with the closed producer.
/// This is called exactly once per candle, after it has been fully formed
/// and persisted.
async fn publish_closed(
    producer: &mut producer::Producer<TokioExecutor>,
    symbol: &str,
    candle: &Candle,
) -> Result<()> {
    let fut: producer::SendFuture = producer.send_non_blocking(candle.clone()).await?;
    fut.await?;
    info!("Publish CLOSED candle {} @ {}", symbol, candle.close_time);
    Ok(())
}
