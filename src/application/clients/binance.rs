/*
 * BLACKER
 * Copyright (C) 2026  Juan José Caballero Rey
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use reqwest::Response;
use serde_json::Value;
use tokio_retry::{
    Retry,
    strategy::{ExponentialBackoff, jitter},
};
use tracing::warn;

use crate::{application::clients::client::AnyClient, common::candle::Candle};

/// Binance REST client for historical market data.
/// Responsibilities:
/// - Fetch closed OHLCV candles
/// - Normalize Binance-specific formats
/// - Never return partial candles
#[derive(Clone)]
pub struct Binance {
    http: reqwest::Client,
    base_url: String,
}

impl Binance {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: "https://api.binance.com".to_string(),
        }
    }
}

#[async_trait]
impl AnyClient for Binance {
    /// Fetch 1-minute OHLCV candles from spot Binance.
    ///
    /// - `symbol`: Trading pair (e.g. "BTCUSDT").
    /// - `start_time`: Optional UNIX timestamp in milliseconds (inclusive).
    /// - `end_time`: Optional UNIX timestamp in milliseconds (inclusive).
    /// - `limit`: Maximum number of candles to return (max 1000).
    ///
    /// If no time range is provided, the most recent candles are returned.
    async fn fetch_ohlcv_1m(
        &self,
        symbol: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u16,
    ) -> Result<Vec<Candle>> {
        let retry_strategy = ExponentialBackoff::from_millis(200).map(jitter).take(3);

        let resp: Response = Retry::spawn(retry_strategy, || async {
            // Build the base HTTP GET request to the Binance Klines endpoint.
            // We start with the required query parameters that are always present:
            // - symbol: trading pair (e.g. BTCUSDT)
            // - interval: fixed to 1m candles (source of truth)
            // - limit: maximum number of klines to return in a single request
            //
            // The RequestBuilder is immutable, so we store it in a mutable variable
            // to allow conditional extension (e.g. adding startTime / endTime later).
            let mut req: reqwest::RequestBuilder = self
                .http
                .get(format!("{}/api/v3/klines", self.base_url))
                .query(&[
                    ("symbol", symbol),
                    ("interval", "1m"),
                    ("limit", &limit.to_string()),
                ]);

            // If a start timestamp is provided, constrain the query to candles
            // starting at or after this UNIX millisecond timestamp.
            if let Some(start) = start_time {
                req = req.query(&[("startTime", start.to_string())]);
            }

            // If an end timestamp is provided, constrain the query to candles
            // ending at or before this UNIX millisecond timestamp.
            if let Some(end) = end_time {
                req = req.query(&[("endTime", end.to_string())]);
            }

            // Execute the HTTP request.
            let resp: Response = req.send().await?;

            match resp.error_for_status() {
                Ok(resp) => Ok(resp),
                Err(err) => {
                    warn!(
                        symbol = symbol,
                        error = %err,
                        "Binance returned non-success status"
                    );
                    Err(err)
                }
            }
        })
        .await?;

        // Parse the response body into raw Binance kline arrays.
        let data: Vec<Vec<Value>> = resp.json().await?;

        // Pre-allocate storage for parsed candles.
        let mut candles: Vec<Candle> = Vec::with_capacity(data.len());

        for k in data {
            // [
            //   0: open_time (UNIX ms),
            //   1: open_price (string),
            //   2: high_price (string),
            //   3: low_price  (string),
            //   4: close_price (string),
            //   5: volume (string),
            //   6: close_time (UNIX ms),
            //   7: quote_asset_volume (string),
            //   8: number_of_trades (number),
            //   9: taker_buy_base_volume (string),
            //  10: taker_buy_quote_volume (string),
            //  11: unused (ignore)
            // ]

            if k.len() < 7 {
                return Err(anyhow!("Invalid kline payload from Binance")); // Add validator
            }

            let open_time: i64 = k[0].as_i64().unwrap();
            let close_time: i64 = k[6].as_i64().unwrap();

            candles.push(Candle {
                symbol: symbol.to_string(),
                open_time,
                close_time,
                open: k[1].as_str().unwrap().parse()?,
                high: k[2].as_str().unwrap().parse()?,
                low: k[3].as_str().unwrap().parse()?,
                close: k[4].as_str().unwrap().parse()?,
                volume: k[5].as_str().unwrap().parse()?,
            });
        }

        Ok(candles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tracing::info;

    #[tokio::test]
    async fn fetch_ohlcv_1m_from_binance() {
        tracing_subscriber::fmt::init();

        let now_ms: i64 = Utc::now().timestamp_millis();
        let five_minutes_ago_ms: i64 = now_ms - 5 * 60 * 1_000;

        let client: Binance = Binance::new();

        let candles: Vec<Candle> = client
            .fetch_ohlcv_1m("BTCUSDT", Some(five_minutes_ago_ms), Some(now_ms), 5)
            .await
            .expect("failed to fetch klines");

        assert!(!candles.is_empty());

        let c: &Candle = &candles[0];

        info!("{:?}", c);
        info!("Candles length {}", candles.len());

        // Basic structural assertions
        assert_eq!(c.symbol, "BTCUSDT");
        assert!(c.open > 0.0);
        assert!(c.high >= c.low);
        assert!(c.volume >= 0.0);

        // Time sanity
        assert!(c.close_time > c.open_time);
        assert!(c.open_time < Utc::now().timestamp_millis());
    }
}
