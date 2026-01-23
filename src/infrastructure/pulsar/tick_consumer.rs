use anyhow::Result;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};

use crate::common::tick::Tick;

/// Encapsulates a Pulsar consumer for a specific topic and subscription
pub struct TickConsumer {
    consumer: Consumer<Tick, TokioExecutor>,
}

impl TickConsumer {
    /// Creates a new TickConsumer asynchronously
    ///
    /// # Arguments
    ///
    /// * `pulsar_client` - The shared Pulsar client
    /// * `topic` - Topic to subscribe to
    /// * `subscription` - Subscription name
    /// * `consumer_name` - Optional consumer name
    /// * `sub_type` - Subscription type (KeyShared, Exclusive, etc.)
    ///
    /// # Example
    ///
    /// ```rust
    /// let tick_consumer = TickConsumer::new(
    ///     &pulsar_client,
    ///     "persistent://public/market-data/ticks",
    ///     "service-ingest-truth",
    ///     "consumer-1",
    ///     SubType::KeyShared
    /// ).await?;
    /// ```
    /// Configures the Pulsar consumer used to receive the market ticks produced by `service-ingest`.
    /// All pods share the same subscription name (`service-ingest-truth`); Each consumer is
    /// assigned a unique name derived from (POD_NAME) for observability only.

    /// Key_Shared guarantees that all messages with the same key (symbol)
    /// are delivered to a single consumer at a time. If a consumer crashes
    /// or disconnects, Pulsar automatically reassigns the key to another
    /// active consumer, resuming from the last acknowledged message.
    pub async fn new(
        pulsar_client: &Pulsar<TokioExecutor>,
        consumer_name: &str,
    ) -> Result<Self> {
        let consumer: Consumer<Tick, _> = pulsar_client
            .consumer()
            .with_topic("persistent://public/market-data/ticks")
            .with_subscription("service-ingest-truth")
            .with_subscription_type(SubType::KeyShared)
            .with_consumer_name(consumer_name)
            .build()
            .await?;

        Ok(Self { consumer })
    }

    /// Returns a mutable reference to the internal consumer
    /// Useful to call `try_next()` or ack messages
    pub fn inner_mut(&mut self) -> &mut Consumer<Tick, TokioExecutor> {
        &mut self.consumer
    }
}
