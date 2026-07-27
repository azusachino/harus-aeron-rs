//! Cross-process Rust client probe used by the driver interop smoke harness.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use aeron_client::cnc_file::{CNC_FILE, CncFile};
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::error::{AeronError, Result};
use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

const STREAM_ID: i32 = 4_243;
const IPC_CLIENT_ID: i64 = 9_101;
const UDP_PUBLISHER_CLIENT_ID: i64 = 9_102;
const UDP_SUBSCRIBER_CLIENT_ID: i64 = 9_103;
const PAYLOAD: &[u8] = b"rust-client-through-rust-driver";
const TIMEOUT: Duration = Duration::from_secs(15);

fn client(directory: PathBuf, client_id: i64) -> Result<ClientConductor<'static>> {
    let cnc = CncFile::map_existing(directory.join(CNC_FILE))?;
    let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer())
        .map_err(|error| AeronError::IllegalState(error.to_string()))?;
    ClientConductor::with_cnc_file(DriverProxy::new(ring, client_id), cnc)
}

fn run_ipc(directory: PathBuf) -> Result<()> {
    let mut conductor = client(directory, IPC_CLIENT_ID)?;
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
        return Err(AeronError::DriverTimeout(TIMEOUT.as_millis() as u64));
    }

    let mut payload = PAYLOAD.to_vec();
    let source = AtomicBuffer::new(&mut payload);
    let mut offered = false;
    let mut received = Vec::new();
    while Instant::now() < deadline {
        conductor.do_work()?;
        if !offered {
            if let Some(publication) = conductor.publication(publication_registration) {
                offered = publication.offer(&source, 0, PAYLOAD.len())? > 0;
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
            println!("Rust multi-process IPC round-trip passed");
            return Ok(());
        }
        std::thread::yield_now();
    }

    Err(AeronError::IllegalState(format!(
        "Rust IPC probe timed out: offered={offered}, received={received:?}"
    )))
}

fn run_udp_publisher(directory: PathBuf, endpoint: &str) -> Result<()> {
    let mut conductor = client(directory, UDP_PUBLISHER_CLIENT_ID)?;
    let channel = format!("aeron:udp?endpoint={endpoint}");
    let registration = conductor.add_publication(&channel, STREAM_ID)?;
    let deadline = Instant::now() + TIMEOUT;
    let mut payload = PAYLOAD.to_vec();
    let source = AtomicBuffer::new(&mut payload);
    let mut offered = false;

    while Instant::now() < deadline {
        conductor.do_work()?;
        if let Some(publication) = conductor.publication(registration) {
            let position = publication.offer(&source, 0, PAYLOAD.len())?;
            offered |= position > 0;
            if offered {
                // Keep the datagram in flight long enough for the independent subscriber
                // process to complete its SETUP/SM handshake and poll the payload.
                std::thread::sleep(Duration::from_millis(100));
                println!("Rust multi-process UDP publisher passed");
                return Ok(());
            }
        }
        std::thread::yield_now();
    }

    Err(AeronError::IllegalState(format!(
        "Rust UDP publisher timed out: offered={offered}"
    )))
}

fn run_udp_subscriber(directory: PathBuf, endpoint: &str) -> Result<()> {
    let mut conductor = client(directory, UDP_SUBSCRIBER_CLIENT_ID)?;
    let channel = format!("aeron:udp?endpoint={endpoint}");
    let registration = conductor.add_subscription(&channel, STREAM_ID)?;
    let deadline = Instant::now() + TIMEOUT;
    let mut received = Vec::new();

    while Instant::now() < deadline {
        conductor.do_work()?;
        if let Some(subscription) = conductor.subscription_mut(registration) {
            subscription.poll(
                |buffer, offset, length, _header| {
                    received.resize(length, 0);
                    buffer.get_bytes(offset, &mut received);
                },
                10,
            );
        }
        if received == PAYLOAD {
            println!("Rust multi-process UDP subscriber passed");
            return Ok(());
        }
        std::thread::yield_now();
    }

    Err(AeronError::IllegalState(format!(
        "Rust UDP subscriber timed out: received={received:?}"
    )))
}

fn run() -> Result<()> {
    let mut args = env::args_os();
    let _program = args.next();
    let mode = args
        .next()
        .ok_or_else(|| AeronError::IllegalState(usage()))?;
    let directory = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| AeronError::IllegalState(usage()))?;
    let mode = mode
        .to_str()
        .ok_or_else(|| AeronError::IllegalState(usage()))?;

    match mode {
        "ipc" if args.next().is_none() => run_ipc(directory),
        "udp-publisher" | "udp-subscriber" => {
            let endpoint = args
                .next()
                .and_then(|endpoint| endpoint.into_string().ok())
                .ok_or_else(|| AeronError::IllegalState(usage()))?;
            if args.next().is_some() {
                return Err(AeronError::IllegalState(usage()));
            }
            if mode == "udp-publisher" {
                run_udp_publisher(directory, &endpoint)
            } else {
                run_udp_subscriber(directory, &endpoint)
            }
        }
        _ => Err(AeronError::IllegalState(usage())),
    }
}

fn usage() -> String {
    "usage: rust_process_probe <ipc|udp-publisher|udp-subscriber> <aeron-directory> [endpoint]"
        .to_string()
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
