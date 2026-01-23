use crate::{
    application::symbol_worker::{SymbolCommand, spawn_symbol_worker},
    common::tick::Tick,
    config::Config,
    infrastructure::{
        database::Database,
        pulsar::{PulsarClient, tick_consumer::TickConsumer},
    },
};
use anyhow::Result;
use futures_util::TryStreamExt;
use pulsar::consumer::Message;
use std::collections::HashMap;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Starts the main dispatcher loop
/// - Receives ticks from Pulsar
/// - Routes them to symbol workers
/// - Handles shutdown signal
pub async fn start_dispatcher(
    mut tick_consumer: TickConsumer,
    db: Database,
    pulsar_client: PulsarClient,
    config: Config,
) -> Result<()> {
    // Map of active symbol workers.
    // Each symbol is associated with a dedicated mpsc channel used
    // to route ticks to its corresponding worker task.
    let mut workers: HashMap<String, mpsc::Sender<SymbolCommand>> = HashMap::new();

    // Join handles for all spawned symbol workers, used to ensure
    // a graceful shutdown and proper error propagation.
    let mut worker_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    info!("Waiting for ticks...");

    loop {
        select! {
            result = tick_consumer.inner_mut().try_next() => {
                let maybe_msg: Option<Message<Tick>> = result?;

                let Some(msg) = maybe_msg else {
                    warn!("Pulsar stream closed");
                    break;
                };

                let tick = match msg.deserialize() {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("Failed to deserialize tick: {:?}", e);
                        tick_consumer.inner_mut().ack(&msg).await?;
                        continue;
                    }
                };

                let symbol = tick.symbol.clone();

                // Determine which symbol worker should receive this tick.
                // If a worker for this symbol already exists, reuse its mpsc sender.
                // Otherwise, lazily initialize a new worker for the symbol.
                let sender = if let Some(tx) = workers.get(&symbol) {
                    // A worker for this symbol is already running.
                    // Clone the sender to route the tick to the existing worker.
                    tx.clone()
                } else {
                    // No worker exists yet for this symbol.

                    // Enforce an upper bound on the number of concurrent symbol workers.
                    // This protects the service from unbounded memory and task growth
                    if workers.len() >= config.max_symbols {
                        warn!("MAX_SYMBOLS reached, dropping tick for {}", symbol);
                        tick_consumer.inner_mut().ack(&msg).await?;
                        continue;
                    }

                    info!("Initializing symbol {}", symbol);

                    // Create a bounded channel for this symbol worker.
                    // All ticks for this symbol will be routed through this channel.
                    let (tx, rx) = mpsc::channel(1024);

                    // Spawn a dedicated async task responsible for:
                    // - Backfilling historical 1m candles (on first tick)
                    // - Maintaining the current live 1m candle in memory
                    // - Persisting closed candles to PostgreSQL
                    // - Publishing live and closed candles to Pulsar
                    let handle = spawn_symbol_worker(
                        symbol.clone(),
                        rx, //Multiple producer, single consumer for this worker rx does not need a clone.
                        db.clone(),
                        pulsar_client.inner().clone(),
                        config.clone(),
                    );

                    // Cache the worker channel for this symbol to enable fast tick dispatch
                    // and enforce a single worker per symbol.
                    workers.insert(symbol.clone(), tx.clone());
                    // Store the JoinHandle so the main task can later await the worker,
                    // ensuring a graceful shutdown and surfacing any worker errors.
                    worker_handles.push(handle);

                    tx //Created sender
                };

                // Send the tick to the symbol worker.
                // If the channel is closed, it means the worker has exited or crashed,
                // so we remove the symbol from the active workers map to allow re-initialization.
                if sender.send(SymbolCommand::Tick(tick)).await.is_err() {
                    warn!("Worker for {} dropped, removing", symbol);
                    workers.remove(&symbol);
                }

                tick_consumer.inner_mut().ack(&msg).await?;
            }

            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    info!("Shutting down symbol workers");

    // Notify workers
    for (_, tx) in workers {
        let _ = tx.send(SymbolCommand::Shutdown).await;
    }

    // Await workers
    for handle in worker_handles {
        if let Err(e) = handle.await? {
            warn!("Worker exited with error: {:?}", e);
        }
    }

    info!("service-ingest-truth stopped cleanly");

    Ok(())
}
