//! Cross-process probe: Rust client against a vendored Java MediaDriver.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use aeron_client::cnc_file::{CNC_FILE, CncFile};
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

const STREAM_ID: i32 = 4_242;
const CLIENT_ID: i64 = 9_001;
const PAYLOAD: &[u8] = b"rust-client-through-java-driver";
const TIMEOUT: Duration = Duration::from_secs(15);

fn run(directory: PathBuf) -> aeron_client::error::Result<()> {
    let cnc = CncFile::map_existing(directory.join(CNC_FILE))?;
    let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer())
        .map_err(|error| aeron_client::error::AeronError::IllegalState(error.to_string()))?;
    let mut conductor = ClientConductor::with_cnc_file(DriverProxy::new(ring, CLIENT_ID), cnc)?;
    let publication_registration = conductor.add_publication("aeron:ipc", STREAM_ID)?;
    let subscription_registration = conductor.add_subscription("aeron:ipc", STREAM_ID)?;
    let deadline = Instant::now() + TIMEOUT;

    while (conductor.publication(publication_registration).is_none()
        || conductor.subscription(subscription_registration).is_none())
        && Instant::now() < deadline
    {
        conductor.do_work()?;
        std::thread::yield_now();
    }

    if conductor.publication(publication_registration).is_none()
        || conductor.subscription(subscription_registration).is_none()
    {
        return Err(aeron_client::error::AeronError::DriverTimeout(
            TIMEOUT.as_millis() as u64,
        ));
    }

    let mut payload = PAYLOAD.to_vec();
    let source = AtomicBuffer::new(&mut payload);
    let mut offered = false;
    let mut received = Vec::new();
    while Instant::now() < deadline {
        conductor.do_work()?;
        if !offered {
            if let Some(publication) = conductor.publication(publication_registration) {
                let position = publication.offer(&source, 0, PAYLOAD.len())?;
                offered = position > 0;
            }
        }
        if let Some(subscription) = conductor.subscription_mut(subscription_registration) {
            subscription.poll(
                |buffer, offset, length, _header| {
                    received.resize(length, 0);
                    buffer.get_bytes(offset, &mut received);
                },
                10,
            );
        }
        if received == PAYLOAD {
            println!("Rust client IPC round-trip through Java driver passed");
            return Ok(());
        }
        std::thread::yield_now();
    }

    Err(aeron_client::error::AeronError::IllegalState(format!(
        "Rust client probe timed out: offered={offered}, received={received:?}"
    )))
}

fn main() {
    let directory = env::args_os().nth(1).map(PathBuf::from).unwrap_or_else(|| {
        eprintln!("usage: rust_client_probe <aeron-directory>");
        std::process::exit(2);
    });
    if let Err(error) = run(directory) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
