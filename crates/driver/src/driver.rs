//! MediaDriver main orchestrator launching Conductor, Sender, and Receiver agents.
//!
//! Mirrors `io.aeron.driver.MediaDriver.java`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aeron_client::cnc_file::CncFile;

use crate::conductor::DriverConductor;
use crate::configuration::Configuration;
use crate::error::Result;
use crate::receiver::Receiver;
use crate::sender::Sender;

static DRIVER_INSTANCE: AtomicU64 = AtomicU64::new(0);

pub struct MediaDriver {
    configuration: Configuration,
    aeron_dir: PathBuf,
    cnc_path: PathBuf,
    conductor: DriverConductor,
    sender: Sender,
    receiver: Receiver,
    remove_aeron_dir_on_drop: bool,
    clock_origin: Instant,
    clock_origin_epoch_ns: i64,
}

/// A MediaDriver running its conductor, sender, and receiver duty cycle on a dedicated thread.
pub struct RunningMediaDriver {
    aeron_dir: PathBuf,
    stop: Arc<AtomicBool>,
    error: Arc<Mutex<Option<crate::error::DriverError>>>,
    join_handle: Option<JoinHandle<()>>,
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
        Self::launch_in_with_cleanup(configuration, aeron_dir, true)
    }

    /// Launch a driver in an explicitly managed Aeron directory.
    ///
    /// The directory must not already contain `cnc.dat`; this prevents one driver from
    /// silently taking over another driver's shared-memory control plane. The caller owns
    /// cleanup of an explicitly supplied directory.
    pub fn launch_in(configuration: Configuration, aeron_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::launch_in_with_cleanup(configuration, aeron_dir.into(), false)
    }

    fn launch_in_with_cleanup(
        configuration: Configuration,
        aeron_dir: PathBuf,
        remove_aeron_dir_on_drop: bool,
    ) -> Result<Self> {
        configuration.validate()?;
        std::fs::create_dir_all(&aeron_dir).map_err(|e| {
            crate::error::DriverError::IllegalState(format!("create aeron dir: {e}"))
        })?;
        let cnc_path = aeron_dir.join(aeron_client::cnc_file::CNC_FILE);
        if cnc_path.exists() {
            return Err(crate::error::DriverError::IllegalState(format!(
                "aeron directory is already in use: {}",
                aeron_dir.display()
            )));
        }

        let cnc = CncFile::create_new_with_regions_and_page_size(
            &cnc_path,
            configuration.conductor_buffer_length,
            configuration.to_clients_buffer_length,
            configuration.counters_metadata_buffer_length,
            configuration.counters_values_buffer_length,
            configuration.error_buffer_length,
            configuration.client_liveness_timeout_ns,
            configuration.file_page_size,
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
            remove_aeron_dir_on_drop,
            clock_origin: Instant::now(),
            clock_origin_epoch_ns: epoch_ns(),
        })
    }

    /// Start a temporary-directory driver on a dedicated duty-cycle thread.
    pub fn start(configuration: Configuration) -> Result<RunningMediaDriver> {
        let instance = DRIVER_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let aeron_dir =
            std::env::temp_dir().join(format!("aeron-driver-{}-{}", std::process::id(), instance));
        Self::start_in_with_cleanup(configuration, aeron_dir, true)
    }

    /// Start a driver on a dedicated duty-cycle thread in an explicit directory.
    pub fn start_in(
        configuration: Configuration,
        aeron_dir: impl Into<PathBuf>,
    ) -> Result<RunningMediaDriver> {
        Self::start_in_with_cleanup(configuration, aeron_dir.into(), false)
    }

    fn start_in_with_cleanup(
        configuration: Configuration,
        aeron_dir: PathBuf,
        remove_aeron_dir_on_drop: bool,
    ) -> Result<RunningMediaDriver> {
        let driver = Self::launch_in_with_cleanup(
            configuration,
            aeron_dir.clone(),
            remove_aeron_dir_on_drop,
        )?;
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));
        let thread_stop = Arc::clone(&stop);
        let thread_error = Arc::clone(&error);
        let join_handle = thread::Builder::new()
            .name("aeron-driver".to_string())
            .spawn(move || {
                let mut driver = driver;
                while !thread_stop.load(Ordering::Acquire) {
                    if let Err(driver_error) = driver.do_work() {
                        *thread_error.lock().expect("driver error lock poisoned") =
                            Some(driver_error);
                        break;
                    }
                    if driver.conductor().termination_requested() {
                        thread_stop.store(true, Ordering::Release);
                    }
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            })
            .map_err(|e| crate::error::DriverError::IllegalState(format!("start driver: {e}")))?;

        Ok(RunningMediaDriver {
            aeron_dir,
            stop,
            error,
            join_handle: Some(join_handle),
        })
    }

    /// Run one duty cycle across all agents, returning the total work done.
    pub fn do_work(&mut self) -> Result<usize> {
        let now_ns = self
            .clock_origin_epoch_ns
            .saturating_add(self.clock_origin.elapsed().as_nanos().min(i64::MAX as u128) as i64);
        let conductor_work = self.conductor.do_work(now_ns)?;
        // Adopt any endpoints the conductor created this cycle into the send/receive agents.
        for publication in self.conductor.take_new_network_publications() {
            self.sender.add_network_publication(publication);
        }
        for endpoint in self.conductor.take_new_receive_endpoints() {
            self.receiver.add_receive_channel_endpoint(endpoint);
        }
        for channel in self.conductor.take_removed_receive_channels() {
            self.receiver.remove_receive_channel_endpoint(&channel);
        }
        for registration_id in self.conductor.take_removed_network_publications() {
            self.sender.remove_network_publication(registration_id);
        }
        for (registration_id, update) in self.conductor.take_destination_updates() {
            // The conductor validates the registration and channel before queuing this update.
            // A publication can still disappear in the same duty cycle, so silently dropping a
            // stale update matches the asynchronous Sender ownership boundary.
            self.sender.update_destination(registration_id, update);
        }
        let sender_work = self.sender.do_work(now_ns)?;
        for publication in self.sender.network_publications() {
            self.conductor.sync_network_publication_limit(
                publication.registration_id(),
                publication.publication_limit(),
            )?;
        }
        let receiver_work = self.receiver.do_work_at(now_ns)?;
        // Hand received SETUP/DATA frames to the conductor for image creation + reassembly.
        let mut dispatch_work = 0;
        for frame in self.receiver.take_received_frames() {
            let dispatch_channel = self
                .conductor
                .resolve_receive_channel(&frame.channel)
                .to_owned();
            self.conductor.on_received_frame_with_status(
                &frame.bytes,
                frame.source,
                now_ns,
                Some(&dispatch_channel),
                frame.receiver_status,
            )?;
            if self.conductor.take_setup_elicitation(
                &dispatch_channel,
                frame.session_id,
                frame.stream_id,
                frame.source,
                now_ns,
            ) {
                self.receiver.send_setup_eliciting_status(
                    frame.endpoint_index,
                    frame.session_id,
                    frame.stream_id,
                    frame.source,
                )?;
            }
            dispatch_work += 1;
        }

        // Loss is detected image-side; route each due NAK back through the receiver's endpoint.
        let mut nak_work = 0;
        for nak in self.conductor.poll_image_loss(now_ns) {
            nak_work += self.receiver.send_nak_on_channel(
                &nak.channel,
                nak.session_id,
                nak.stream_id,
                nak.term_id,
                nak.term_offset,
                nak.length,
                nak.source,
            )?;
        }

        Ok(conductor_work + sender_work + receiver_work + dispatch_work + nak_work)
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
        if self.remove_aeron_dir_on_drop {
            let _ = std::fs::remove_dir_all(&self.aeron_dir);
        }
    }
}

impl RunningMediaDriver {
    pub fn aeron_dir(&self) -> &std::path::Path {
        &self.aeron_dir
    }

    pub fn cnc_path(&self) -> PathBuf {
        self.aeron_dir.join(aeron_client::cnc_file::CNC_FILE)
    }

    pub fn is_running(&self) -> bool {
        self.join_handle
            .as_ref()
            .is_some_and(|join_handle| !join_handle.is_finished())
    }

    /// Stop the duty-cycle thread and return any error observed while it was running.
    pub fn stop(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.join().map_err(|_| {
                crate::error::DriverError::IllegalState("driver thread panicked".to_string())
            })?;
        }

        if let Some(error) = self
            .error
            .lock()
            .expect("driver error lock poisoned")
            .take()
        {
            return Err(error);
        }

        Ok(())
    }
}

impl Drop for RunningMediaDriver {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn epoch_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aeron_client::cnc_file::CncFile;
    use aeron_client::conductor::{ClientConductor, RegistrationState};
    use aeron_client::driver_proxy::DriverProxy;
    use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;
    use std::net::UdpSocket;
    use std::time::{Duration, Instant};

    #[test]
    fn should_launch_media_driver_and_process_a_client_command() {
        let config = Configuration::default();
        let mut driver = MediaDriver::launch(config).unwrap();

        assert!(driver.cnc().counters_metadata_length() > 0);
        assert!(driver.cnc().counters_values_length() > 0);
        let control_counter_offset = 43 * aeron_client::cnc_file::COUNTER_METADATA_LENGTH;
        let metadata = driver.cnc().counters_metadata_buffer();
        assert_eq!(
            metadata.get_i32(control_counter_offset),
            aeron_client::cnc_file::COUNTER_RECORD_ALLOCATED
        );
        assert_eq!(metadata.get_i32(control_counter_offset + 4), 8);
        assert_eq!(
            driver
                .cnc()
                .counters_values_buffer()
                .get_i64(43 * aeron_client::cnc_file::COUNTER_VALUE_LENGTH),
            aeron_client::counters::CONTROL_PROTOCOL_SEMANTIC_VERSION
        );

        // A client maps the same CnC and sends an add-subscription command.
        let ring = ManyToOneRingBuffer::new(driver.cnc().to_driver_buffer()).unwrap();
        let proxy = DriverProxy::new(ring, 99);
        proxy.add_subscription("aeron:ipc", 1001).unwrap();

        let work = driver.do_work().unwrap();
        assert_eq!(work, 1);
        assert_eq!(driver.conductor().subscriptions().len(), 1);
    }

    #[test]
    fn should_publish_configured_client_liveness_timeout_in_cnc_metadata() {
        let timeout_ns = 123_456_789;
        let driver = MediaDriver::launch(Configuration {
            client_liveness_timeout_ns: timeout_ns,
            ..Configuration::default()
        })
        .unwrap();

        assert_eq!(driver.cnc().client_liveness_timeout_ns(), timeout_ns);
    }

    #[test]
    fn should_publish_configured_cnc_geometry_and_file_page_size() {
        let config = Configuration {
            conductor_buffer_length: aeron_client::cnc_file::DEFAULT_TO_DRIVER_LENGTH + (1 << 20),
            to_clients_buffer_length: aeron_client::cnc_file::DEFAULT_TO_CLIENTS_LENGTH + (1 << 20),
            counters_values_buffer_length: 2 * 1024 * 1024,
            error_buffer_length: 2 * 1024 * 1024,
            file_page_size: 8192,
            ..Configuration::default()
        };
        let driver = MediaDriver::launch(config.clone()).unwrap();

        assert_eq!(
            driver.cnc().to_driver_buffer().capacity(),
            config.conductor_buffer_length
        );
        assert_eq!(
            driver.cnc().to_clients_buffer().capacity(),
            config.to_clients_buffer_length
        );
        assert_eq!(
            driver.cnc().counters_values_length(),
            config.counters_values_buffer_length
        );
        assert_eq!(driver.cnc().error_log_length(), config.error_buffer_length);
        assert_eq!(driver.cnc().file_page_size(), config.file_page_size);
    }

    #[test]
    fn should_reject_invalid_configuration_before_creating_cnc() {
        let directory = std::env::temp_dir().join(format!(
            "aeron-driver-invalid-config-{}",
            DRIVER_INSTANCE.fetch_add(1, Ordering::Relaxed)
        ));
        let result = MediaDriver::launch_in(
            Configuration {
                mtu_length: 1_500,
                ..Configuration::default()
            },
            &directory,
        );
        assert!(matches!(
            result,
            Err(crate::error::DriverError::IllegalArgument(message))
                if message.contains("mtu-length")
        ));
        assert!(!directory.exists());
    }

    #[test]
    fn should_start_and_stop_a_managed_driver_thread() {
        let mut driver = MediaDriver::start(Configuration::default()).unwrap();
        assert!(
            driver
                .aeron_dir()
                .join(aeron_client::cnc_file::CNC_FILE)
                .exists()
        );
        driver.stop().unwrap();
        assert!(!driver.aeron_dir().exists());
    }

    #[test]
    fn should_reject_launch_in_a_directory_with_existing_cnc() {
        let directory = std::env::temp_dir().join(format!(
            "aeron-driver-existing-cnc-{}",
            DRIVER_INSTANCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(aeron_client::cnc_file::CNC_FILE), []).unwrap();

        let result = MediaDriver::launch_in(Configuration::default(), &directory);
        assert!(matches!(
            result,
            Err(crate::error::DriverError::IllegalState(message)) if message.contains("already in use")
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn should_execute_async_commands_in_order_on_background_driver() {
        // This ports the runtime portion of vendor MediaDriverTest. Duty-cycle tracker and
        // print-configuration cases are Java Context APIs with no Rust equivalent yet.
        let mut driver = MediaDriver::start(Configuration::default()).unwrap();
        let cnc = CncFile::map_existing(driver.cnc_path()).unwrap();
        let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
        let mut client =
            ClientConductor::with_cnc_file(DriverProxy::new(ring, 42), cnc.clone()).unwrap();
        let endpoint_port = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let channel = format!("aeron:udp?endpoint=127.0.0.1:{endpoint_port}");

        let first = client.add_exclusive_publication(&channel, 1111).unwrap();
        client.remove_publication(first).unwrap();
        let second = client.add_exclusive_publication(&channel, 1111).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            client.do_work().unwrap();
            if client.registration_state(first) == Some(RegistrationState::Closed)
                && client.publication(second).is_some()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(
            client.registration_state(first),
            Some(RegistrationState::Closed)
        );
        assert!(client.publication(second).is_some());
        driver.stop().unwrap();
    }

    #[test]
    fn should_stop_a_managed_driver_on_terminate_command() {
        let mut driver = MediaDriver::start(Configuration {
            allow_termination: true,
            ..Configuration::default()
        })
        .unwrap();
        let cnc = CncFile::map_existing(driver.cnc_path()).unwrap();
        let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
        DriverProxy::new(ring, 7).terminate_driver(&[]).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while driver.is_running() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }

        assert!(!driver.is_running());
        driver.stop().unwrap();
    }

    #[test]
    fn should_deny_terminate_command_by_default() {
        let mut driver = MediaDriver::start(Configuration::default()).unwrap();
        let cnc = CncFile::map_existing(driver.cnc_path()).unwrap();
        let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
        DriverProxy::new(ring, 8).terminate_driver(&[]).unwrap();

        std::thread::sleep(Duration::from_millis(25));
        assert!(driver.is_running());
        driver.stop().unwrap();
    }
}
