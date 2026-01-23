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

use crate::application::clients::client::Client;



/// Microservice configuration struct
/// ```
/// let config: Config = Config::from_env()?;
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    pub consumer_name: String,
    pub client_id: Client,
    pub database_url: String,
    pub pulsar_url: String,
    pub max_symbols: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let consumer_name: String = std::env::var("POD_NAME")
            .unwrap_or_else(|_| format!("service-ingest-truth-{}", std::process::id()));

        let client_id_raw: String =
            std::env::var("CLIENT_ID").map_err(|_| anyhow::anyhow!("CLIENT_ID is not set"))?;

        let database_url: String = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL is not set"))?;

        let pulsar_url: String =
            std::env::var("PULSAR_URL").map_err(|_| anyhow::anyhow!("PULSAR_URL is not set"))?;

        let max_symbols_raw: String =
            std::env::var("MAX_SYMBOLS").map_err(|_| anyhow::anyhow!("MAX_SYMBOLS is not set"))?;

        //==========================================================================================

        let client_id: Client = client_id_raw.parse()?;

        let max_symbols: usize = max_symbols_raw
            .parse()
            .map_err(|_| anyhow::anyhow!("MAX_SYMBOLS must be a positive integer"))?;

        if max_symbols == 0 {
            return Err(anyhow::anyhow!("MAX_SYMBOLS must be > 0"));
        }

        Ok(Self {
            client_id,
            consumer_name,
            database_url,
            pulsar_url,
            max_symbols,
        })
    }
}
