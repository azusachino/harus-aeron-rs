//! Driver process entry point.

use std::io::{Read, Write};
use std::sync::mpsc;

use aeron_driver::configuration::Configuration;
use aeron_driver::driver::MediaDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    tracing::info!("Starting aeron-driver");
    let configuration = Configuration::load();

    if std::env::args().any(|arg| arg == "--once") {
        let mut driver = MediaDriver::launch(configuration.clone())?;
        tracing::info!("Media driver launched successfully for one duty cycle");
        let _ = driver.do_work()?;
        return Ok(());
    }

    let mut driver = MediaDriver::start(configuration)?;
    tracing::info!(aeron_dir = %driver.aeron_dir().display(), "Media driver started");
    println!("{}", driver.cnc_path().display());

    {
        std::io::stdout().flush()?;
        let (release_sender, release_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let mut release = String::new();
            let _ = std::io::stdin().read_to_string(&mut release);
            let _ = release_sender.send(());
        });
        while release_receiver.try_recv().is_err() && driver.is_running() {
            std::thread::park_timeout(std::time::Duration::from_millis(50));
        }
    }

    driver.stop()?;

    Ok(())
}
