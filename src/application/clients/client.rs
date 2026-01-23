use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::str::FromStr;



use crate::application::clients::binance::Binance;
use crate::common::candle::Candle;
use crate::config::Config;

/// Data provider Client enum
/// ```
/// Client::Binance
/// Client::Databento
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Client {
    Binance,
    Databento,
}

impl FromStr for Client {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "binance" => Ok(Client::Binance),
            "databento" => Ok(Client::Databento),
            other => Err(anyhow::anyhow!(
                "Unknown CLIENT_ID '{}'. Supported: binance, databento",
                other
            )),
        }
    }
}

/// Abstraction over market data providers capable of supplying historical
/// 1-minute OHLCV candles.
///
/// This trait defines a uniform interface that hides provider-specific APIs,
/// authentication mechanisms, pagination behavior, and rate-limiting details
/// from the rest of the system. Consumers depend only on this contract, not on
/// concrete data source implementations.
///
/// All timestamps are expressed as Unix milliseconds. Returned candles must
/// represent closed, finalized 1-minute intervals and be ordered
/// chronologically by `open_time`.
///
/// Implementations must be safe to use inside asynchronous tasks and executors
/// (`Send + Sync`), allowing them to be moved across threads and referenced
/// safely within async contexts.
#[async_trait]
pub trait AnyClient: Send + Sync {
    /// Fetches historical 1-minute OHLCV candles for a trading symbol.
    ///
    /// # Parameters
    /// - `symbol`: Trading symbol identifier (e.g. "BTCUSDT").
    /// - `start_time`: Optional inclusive start timestamp (Unix ms) indicating
    ///   where the historical query should begin.
    /// - `end_time`: Optional inclusive end timestamp (Unix ms) indicating
    ///   where the historical query should stop.
    /// - `limit`: Maximum number of 1-minute candles to return in a single call.

    async fn fetch_ohlcv_1m(
        &self,
        symbol: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u16,
    ) -> Result<Vec<Candle>>;
}

/// Factory function to create the appropriate client based on config
pub fn create_client(config: &Config) -> Result<Box<dyn AnyClient>> {
    match &config.client_id {
        Client::Binance => {
            let client: Binance = Binance::new();
            Ok(Box::new(client))
        }
        Client::Databento => {
            // TODO: Implement Databento client later
            Err(anyhow::anyhow!("Databento client not implemented yet"))
        }
    }
}
