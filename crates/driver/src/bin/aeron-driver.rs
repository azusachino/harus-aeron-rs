//! Driver process entry point.

use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    tracing::info!("Starting aeron-driver");

    let config = Configuration::default();
    let mut driver = MediaDriver::launch(config)?;

    tracing::info!("Media driver launched successfully");

    // Single step verification for driver binary start
    let _ = driver.do_work()?;

    Ok(())
}
