use anyhow::Result;
use dotenvy::from_filename;
use rustls::crypto::ring;
use tracing::info;

use crate::config::Config;

pub fn run() -> Result<()> {
    from_filename(".env.local").ok();

    // Initialize rustls crypto backend (ring).
    // Required for all TLS connections (WebSockets, HTTPS, Pulsar).
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Initialize tracing subscriber for structured, async-safe logging.
    // Enables info!, warn!, error! logs across the entire service.
    tracing_subscriber::fmt::init();

    info!("Bootstrap finished");

    Ok(())
}

pub fn get_config() -> Result<Config> {
    let config: Config = Config::from_env()?;

    info!(
        "Client {:?} | MAX_SYMBOLS={}",
        config.client_id, config.max_symbols
    );

    Ok(config)
}
