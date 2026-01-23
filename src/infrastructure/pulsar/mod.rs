pub mod tick_consumer;

use anyhow::Result;
use pulsar::{Pulsar, TokioExecutor};

/// Encapsulates a Pulsar client for async message processing
#[derive(Clone)]
pub struct PulsarClient {
    inner: Pulsar<TokioExecutor>,
}

impl PulsarClient {
    /// Creates a new Pulsar client asynchronously
    ///
    /// # Arguments
    ///
    /// * `url` - Pulsar broker URL
    ///
    /// # Example
    ///
    /// ```rust
    /// let pulsar_client = PulsarClient::new("pulsar://127.0.0.1:6650").await?;
    /// ```
    /// Initializes the Apache Pulsar client using the Tokio runtime.
    /// - Uses the builder pattern to configure the broker URL and async executor
    /// - Establishes the connection asynchronously
    /// - Fails fast if the connection to the broker cannot be established
    pub async fn new(url: &str) -> Result<Self> {
        let client: Pulsar<TokioExecutor> = Pulsar::builder(url, TokioExecutor)
            .build()
            .await?;

        Ok(Self { inner: client })
    }

    /// Returns a reference to the internal Pulsar client
    pub fn inner(&self) -> &Pulsar<TokioExecutor> {
        &self.inner
    }

    /// Returns a mutable reference to the internal Pulsar client
    /// Useful if you want to build consumers or producers
    pub fn inner_mut(&mut self) -> &mut Pulsar<TokioExecutor> {
        &mut self.inner
    }
}
