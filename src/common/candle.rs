use pulsar::{SerializeMessage, producer};

use crate::common::tick::Tick;

/// Represents a finalized or in-progress 1-minute OHLCV candle.
/// All timestamps are expressed as Unix milliseconds, where
/// `open_time` is aligned to the start of the minute and
/// `close_time` marks the inclusive end of the interval.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Candle {
    pub symbol: String,

    /// Inclusive start time of the candle (Unix ms, minute-aligned)
    pub open_time: i64,
    /// Inclusive end time of the candle (Unix ms)
    pub close_time: i64,

    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl Candle {
    /// Create a new 1-minute candle from the first tick of the minute
    pub fn new(symbol: &str, tick: &Tick, minute_ts: i64) -> Self {
        Self {
            symbol: symbol.to_string(),

            // Minute boundaries
            open_time: minute_ts,
            close_time: minute_ts + 60_000 - 1, // inclusive close

            open: tick.price,
            high: tick.price,
            low: tick.price,
            close: tick.price,
            volume: tick.quantity,
        }
    }

    /// Update candle with an incoming tick
    pub fn update(&mut self, tick: &Tick) {
        self.high = self.high.max(tick.price);
        self.low = self.low.min(tick.price);
        self.close = tick.price;
        self.volume += tick.quantity;
    }
}

impl SerializeMessage for Candle {
    fn serialize_message(candle: Self) -> Result<producer::Message, pulsar::Error> {
        let payload: Vec<u8> = serde_json::to_vec(&candle)
            .map_err(|e: serde_json::Error| pulsar::Error::Custom(e.to_string()))?;

        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}
