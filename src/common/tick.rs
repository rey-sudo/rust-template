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

use pulsar::{DeserializeMessage, Payload};
use serde::{Deserialize, Serialize};

use crate::application::clients::client::Client;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {

    pub exchange: Client,


    pub symbol: String,


    pub price: f64,


    pub quantity: f64,


    pub side: Side,

    /// Timestamp Unix millis
    pub ts: i64,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}


impl DeserializeMessage for Tick {
    type Output = Result<Tick, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}