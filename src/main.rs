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


use anyhow::{Result};

use service_ingest_base::{
    application::{
        consumer_worker::start_dispatcher,
    },
    config::Config,
    infrastructure::{
        bootstrap,
        database::{Database},
        pulsar::{PulsarClient, tick_consumer::TickConsumer},
    },
};

#[tokio::main]
async fn main() -> Result<()> {
    bootstrap::run()?;

    let config: Config = bootstrap::get_config()?;

    let db: Database = Database::new(&config.database_url).await?;

    db.check_table("ohlcv_1m").await?;

    let pulsar_client: PulsarClient = PulsarClient::new(&config.pulsar_url).await?;

    let tick_consumer: TickConsumer =
        TickConsumer::new(&pulsar_client.inner(), &config.consumer_name).await?;

    start_dispatcher(tick_consumer, db, pulsar_client, config).await?;

    Ok(())
}
