//! Driver process entry point.

use std::io::{Read, Write};

use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    tracing::info!("Starting aeron-driver");

    let config = Configuration::default();
    let mut driver = MediaDriver::launch(config)?;

    tracing::info!("Media driver launched successfully");

    // Single step verification for driver binary start
    let _ = driver.do_work()?;

    if std::env::args().any(|arg| arg == "--hold") {
        println!("{}", driver.cnc_path().display());
        std::io::stdout().flush()?;
        let mut release = String::new();
        std::io::stdin().read_to_string(&mut release)?;
    }

    Ok(())
}
