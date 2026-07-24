//! MediaDriver main orchestrator launching Conductor, Sender, and Receiver agents.
//!
//! Mirrors `io.aeron.driver.MediaDriver.java`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aeron_client::cnc_file::{CncFile, DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH};

use crate::conductor::DriverConductor;
use crate::configuration::Configuration;
use crate::error::Result;
use crate::receiver::Receiver;
use crate::sender::Sender;

/// Default client liveness timeout (10 seconds) before the conductor reclaims a client's
/// resources. A proper `aeron.client.liveness.timeout` config knob lands with the lifecycle
/// task; this default keeps the CnC metadata honest in the meantime.
const CLIENT_LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;

static DRIVER_INSTANCE: AtomicU64 = AtomicU64::new(0);

pub struct MediaDriver {
    configuration: Configuration,
    aeron_dir: PathBuf,
    cnc_path: PathBuf,
    conductor: DriverConductor,
    sender: Sender,
    receiver: Receiver,
}

impl MediaDriver {
    /// Launch a driver: create and map a fresh CnC file, then construct the conductor bound
    /// to it plus the sender/receiver agents.
    ///
    /// The CnC file is placed in a unique temporary path; the Aeron-directory lifecycle (a
    /// stable `aeron.dir/cnc.dat`, agent threads, clean shutdown) is handled by the driver
    /// lifecycle task.
    pub fn launch(configuration: Configuration) -> Result<Self> {
        let instance = DRIVER_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let aeron_dir =
            std::env::temp_dir().join(format!("aeron-driver-{}-{}", std::process::id(), instance));
        std::fs::create_dir_all(&aeron_dir).map_err(|e| {
            crate::error::DriverError::IllegalState(format!("create aeron dir: {e}"))
        })?;
        let cnc_path = aeron_dir.join(aeron_client::cnc_file::CNC_FILE);

        let cnc = CncFile::create_new(
            &cnc_path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            CLIENT_LIVENESS_TIMEOUT_NS,
        )?;

        let conductor = DriverConductor::new(configuration.clone(), cnc, aeron_dir.clone())?;
        let sender = Sender::new();
        let receiver = Receiver::new();

        Ok(Self {
            configuration,
            aeron_dir,
            cnc_path,
            conductor,
            sender,
            receiver,
        })
    }

    /// Run one duty cycle across all agents, returning the total work done.
    pub fn do_work(&mut self) -> Result<usize> {
        let now_ns = now_ns();
        let conductor_work = self.conductor.do_work(now_ns)?;
        let sender_work = self.sender.do_work()?;
        let receiver_work = self.receiver.do_work()?;

        Ok(conductor_work + sender_work + receiver_work)
    }

    /// The mapped CnC file — clients connect by mapping the same path.
    pub fn cnc(&self) -> &aeron_client::cnc_file::CncFile {
        self.conductor.cnc()
    }

    pub fn cnc_path(&self) -> &std::path::Path {
        &self.cnc_path
    }

    /// Directory holding this driver's CnC file and per-publication log files.
    pub fn aeron_dir(&self) -> &std::path::Path {
        &self.aeron_dir
    }

    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    pub fn conductor(&self) -> &DriverConductor {
        &self.conductor
    }

    pub fn conductor_mut(&mut self) -> &mut DriverConductor {
        &mut self.conductor
    }

    pub fn sender(&self) -> &Sender {
        &self.sender
    }

    pub fn sender_mut(&mut self) -> &mut Sender {
        &mut self.sender
    }

    pub fn receiver(&self) -> &Receiver {
        &self.receiver
    }

    pub fn receiver_mut(&mut self) -> &mut Receiver {
        &mut self.receiver
    }
}

impl Drop for MediaDriver {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.aeron_dir);
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aeron_client::driver_proxy::DriverProxy;
    use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

    #[test]
    fn should_launch_media_driver_and_process_a_client_command() {
        let config = Configuration::default();
        let mut driver = MediaDriver::launch(config).unwrap();

        // A client maps the same CnC and sends an add-subscription command.
        let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).unwrap();
        let proxy = DriverProxy::new(ring, 99);
        proxy.add_subscription("aeron:ipc", 1001).unwrap();

        let work = driver.do_work().unwrap();
        assert_eq!(work, 1);
        assert_eq!(driver.conductor().subscriptions().len(), 1);
    }
}
