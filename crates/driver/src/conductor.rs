//! DriverConductor command processor and resource manager.
//!
//! Mirrors `io.aeron.driver.DriverConductor.java`. Polls the to-driver ring buffer of the
//! CnC file, decodes the client command flyweights that `aeron_client::driver_proxy` writes,
//! maintains publication/subscription registrations and client liveness, and publishes
//! response events on the to-clients broadcast buffer.

use std::collections::HashMap;

use agrona::AtomicBuffer;
use agrona::BroadcastTransmitter;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

use aeron_client::cnc_file::CncFile;
use aeron_client::command::{
    PublicationMessageFlyweight, RemoveMessageFlyweight, SubscriptionMessageFlyweight,
};
use aeron_client::driver_events::{
    ON_OPERATION_SUCCESS, ON_SUBSCRIPTION_READY, OperationSucceededFlyweight,
    SubscriptionReadyFlyweight,
};
use aeron_client::driver_proxy::{
    ADD_EXCLUSIVE_PUBLICATION, ADD_PUBLICATION, ADD_SUBSCRIPTION, CLIENT_CLOSE, CLIENT_KEEPALIVE,
    REMOVE_PUBLICATION, REMOVE_SUBSCRIPTION,
};

use crate::configuration::Configuration;
use crate::error::{DriverError, Result};

#[derive(Debug, Clone)]
pub struct PublicationEntry {
    pub registration_id: i64,
    pub correlation_id: i64,
    pub client_id: i64,
    pub stream_id: i32,
    pub channel: String,
    pub is_exclusive: bool,
}

#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub registration_id: i64,
    pub client_id: i64,
    pub stream_id: i32,
    pub channel: String,
}

/// A decoded client command, extracted from a ring-buffer record so it can be processed
/// after the (immutable) ring-buffer read borrow is released.
enum Command {
    AddPublication {
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: String,
        exclusive: bool,
    },
    AddSubscription {
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: String,
    },
    RemovePublication {
        correlation_id: i64,
        registration_id: i64,
    },
    RemoveSubscription {
        correlation_id: i64,
        registration_id: i64,
    },
    Keepalive {
        client_id: i64,
    },
    ClientClose {
        client_id: i64,
    },
}

pub struct DriverConductor {
    cnc: CncFile,
    to_driver: ManyToOneRingBuffer<'static>,
    to_clients: BroadcastTransmitter<'static>,
    configuration: Configuration,
    client_liveness_timeout_ns: i64,
    next_registration_id: i64,
    publications: HashMap<i64, PublicationEntry>,
    subscriptions: HashMap<i64, SubscriptionEntry>,
    /// client id -> last observed keepalive timestamp (ns).
    clients: HashMap<i64, i64>,
}

impl DriverConductor {
    /// Construct a conductor bound to a mapped CnC file.
    pub fn new(configuration: Configuration, cnc: CncFile) -> Result<Self> {
        let to_driver = ManyToOneRingBuffer::new(cnc.to_driver_buffer())
            .map_err(|e| DriverError::IllegalState(format!("to-driver ring buffer: {e}")))?;
        let to_clients = BroadcastTransmitter::new(cnc.to_clients_buffer())
            .map_err(|e| DriverError::IllegalState(format!("to-clients broadcast: {e}")))?;
        let client_liveness_timeout_ns = cnc.client_liveness_timeout_ns();

        Ok(Self {
            cnc,
            to_driver,
            to_clients,
            configuration,
            client_liveness_timeout_ns,
            next_registration_id: 1,
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            clients: HashMap::new(),
        })
    }

    /// One conductor duty cycle: drain and dispatch client commands, then sweep liveness.
    pub fn do_work(&mut self, now_ns: i64) -> Result<usize> {
        // Decode records into owned commands first; the ring-buffer read borrow cannot be
        // held while we mutate conductor state.
        let mut commands: Vec<Command> = Vec::new();
        let read = self.to_driver.read(|msg_type, buffer, offset, _length| {
            if let Some(cmd) = decode_command(msg_type, buffer, offset) {
                commands.push(cmd);
            }
        });

        let mut work = read;
        for command in commands {
            self.dispatch(command, now_ns)?;
        }

        work += self.sweep_timed_out_clients(now_ns);
        Ok(work)
    }

    fn dispatch(&mut self, command: Command, now_ns: i64) -> Result<()> {
        match command {
            Command::AddPublication {
                client_id,
                correlation_id,
                stream_id,
                channel,
                exclusive,
            } => {
                self.touch_client(client_id, now_ns);
                let registration_id = self.next_id();
                self.publications.insert(
                    registration_id,
                    PublicationEntry {
                        registration_id,
                        correlation_id,
                        client_id,
                        stream_id,
                        channel,
                        is_exclusive: exclusive,
                    },
                );
                // PUBLICATION_READY (with the shared log file) is emitted once the log buffer
                // is allocated — see the IPC/network publication tasks.
                Ok(())
            }
            Command::AddSubscription {
                client_id,
                correlation_id,
                stream_id,
                channel,
            } => {
                self.touch_client(client_id, now_ns);
                let registration_id = self.next_id();
                self.subscriptions.insert(
                    registration_id,
                    SubscriptionEntry {
                        registration_id,
                        client_id,
                        stream_id,
                        channel,
                    },
                );
                self.on_subscription_ready(correlation_id)
            }
            Command::RemovePublication {
                correlation_id,
                registration_id,
            } => {
                self.publications.remove(&registration_id);
                self.on_operation_success(correlation_id)
            }
            Command::RemoveSubscription {
                correlation_id,
                registration_id,
            } => {
                self.subscriptions.remove(&registration_id);
                self.on_operation_success(correlation_id)
            }
            Command::Keepalive { client_id } => {
                self.touch_client(client_id, now_ns);
                Ok(())
            }
            Command::ClientClose { client_id } => {
                self.remove_client(client_id);
                Ok(())
            }
        }
    }

    fn next_id(&mut self) -> i64 {
        let id = self.next_registration_id;
        self.next_registration_id += 1;
        id
    }

    fn touch_client(&mut self, client_id: i64, now_ns: i64) {
        self.clients.insert(client_id, now_ns);
    }

    fn remove_client(&mut self, client_id: i64) {
        self.clients.remove(&client_id);
        self.publications.retain(|_, p| p.client_id != client_id);
        self.subscriptions.retain(|_, s| s.client_id != client_id);
    }

    fn sweep_timed_out_clients(&mut self, now_ns: i64) -> usize {
        let timeout = self.client_liveness_timeout_ns;
        if timeout <= 0 {
            return 0;
        }
        let expired: Vec<i64> = self
            .clients
            .iter()
            .filter(|&(_, &last)| now_ns - last > timeout)
            .map(|(&id, _)| id)
            .collect();
        for client_id in &expired {
            self.remove_client(*client_id);
        }
        expired.len()
    }

    fn on_subscription_ready(&self, correlation_id: i64) -> Result<()> {
        let mut scratch = [0u8; 12];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        let flyweight = SubscriptionReadyFlyweight::wrap(buf, 0);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_channel_status_counter_id(-1);
        self.transmit(ON_SUBSCRIPTION_READY, &buf, len)
    }

    fn on_operation_success(&self, correlation_id: i64) -> Result<()> {
        let mut scratch = [0u8; 8];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        let flyweight = OperationSucceededFlyweight::wrap(buf, 0);
        flyweight.set_correlation_id(correlation_id);
        self.transmit(ON_OPERATION_SUCCESS, &buf, len)
    }

    fn transmit(&self, msg_type_id: i32, buffer: &AtomicBuffer, length: usize) -> Result<()> {
        self.to_clients
            .transmit(msg_type_id, buffer, 0, length)
            .map_err(|e| DriverError::IllegalState(format!("broadcast transmit: {e}")))
    }

    pub fn publications(&self) -> &HashMap<i64, PublicationEntry> {
        &self.publications
    }

    pub fn subscriptions(&self) -> &HashMap<i64, SubscriptionEntry> {
        &self.subscriptions
    }

    pub fn active_client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    pub fn cnc(&self) -> &CncFile {
        &self.cnc
    }
}

fn decode_command(msg_type: i32, buffer: &AtomicBuffer, offset: usize) -> Option<Command> {
    match msg_type {
        ADD_PUBLICATION | ADD_EXCLUSIVE_PUBLICATION => {
            let fw = PublicationMessageFlyweight::new(*buffer, offset);
            Some(Command::AddPublication {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                stream_id: fw.stream_id(),
                channel: fw.channel(),
                exclusive: msg_type == ADD_EXCLUSIVE_PUBLICATION,
            })
        }
        ADD_SUBSCRIPTION => {
            let fw = SubscriptionMessageFlyweight::new(*buffer, offset);
            Some(Command::AddSubscription {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                stream_id: fw.stream_id(),
                channel: fw.channel(),
            })
        }
        REMOVE_PUBLICATION => {
            let fw = RemoveMessageFlyweight::new(*buffer, offset);
            Some(Command::RemovePublication {
                correlation_id: fw.correlation_id(),
                registration_id: fw.registration_id(),
            })
        }
        REMOVE_SUBSCRIPTION => {
            let fw = RemoveMessageFlyweight::new(*buffer, offset);
            Some(Command::RemoveSubscription {
                correlation_id: fw.correlation_id(),
                registration_id: fw.registration_id(),
            })
        }
        CLIENT_KEEPALIVE => {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(*buffer, offset);
            Some(Command::Keepalive {
                client_id: fw.client_id(),
            })
        }
        CLIENT_CLOSE => {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(*buffer, offset);
            Some(Command::ClientClose {
                client_id: fw.client_id(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aeron_client::cnc_file::{DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH};
    use aeron_client::driver_events::ON_SUBSCRIPTION_READY;
    use aeron_client::driver_proxy::DriverProxy;
    use agrona::BroadcastReceiver;
    use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

    const CLIENT_ID: i64 = 42;
    const LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;

    fn temp_cnc(tag: &str) -> (CncFile, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "aeron-conductor-{}-{}-{:?}.dat",
            tag,
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let cnc = CncFile::create_new(
            &path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            LIVENESS_TIMEOUT_NS,
        )
        .unwrap();
        (cnc, path)
    }

    fn proxy(cnc: &CncFile) -> DriverProxy<'static> {
        let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
        DriverProxy::new(ring, CLIENT_ID)
    }

    #[test]
    fn registers_publication_and_subscription_from_commands() {
        let (cnc, path) = temp_cnc("register");
        let mut conductor = DriverConductor::new(Configuration::default(), cnc.clone()).unwrap();

        let client = proxy(&cnc);
        client.add_publication("aeron:ipc", 1001).unwrap();
        client.add_subscription("aeron:ipc", 1001).unwrap();

        let work = conductor.do_work(1_000).unwrap();
        assert_eq!(work, 2, "two commands processed");
        assert_eq!(conductor.publications().len(), 1);
        assert_eq!(conductor.subscriptions().len(), 1);
        assert_eq!(conductor.active_client_count(), 1);

        let pubs: Vec<_> = conductor.publications().values().collect();
        assert_eq!(pubs[0].stream_id, 1001);
        assert_eq!(pubs[0].channel, "aeron:ipc");
        assert_eq!(pubs[0].client_id, CLIENT_ID);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn publishes_subscription_ready_event_to_clients() {
        let (cnc, path) = temp_cnc("subready");
        let mut conductor = DriverConductor::new(Configuration::default(), cnc.clone()).unwrap();

        // Client subscribes to events first (broadcast delivers only after listening).
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();

        let client = proxy(&cnc);
        let correlation_id = client.add_subscription("aeron:ipc", 7).unwrap();
        conductor.do_work(1_000).unwrap();

        let mut seen_type = 0i32;
        let mut seen_correlation = 0i64;
        let got = events
            .receive_next(|msg_type, buf, offset, _len| {
                seen_type = msg_type;
                seen_correlation = SubscriptionReadyFlyweight::wrap(*buf, offset).correlation_id();
            })
            .unwrap();

        assert!(got);
        assert_eq!(seen_type, ON_SUBSCRIPTION_READY);
        assert_eq!(seen_correlation, correlation_id);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn remove_publishes_operation_success_and_drops_registration() {
        let (cnc, path) = temp_cnc("remove");
        let mut conductor = DriverConductor::new(Configuration::default(), cnc.clone()).unwrap();

        let client = proxy(&cnc);
        let sub_correlation = client.add_subscription("aeron:ipc", 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let registration_id = *conductor.subscriptions().keys().next().unwrap();

        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let remove_correlation = client.remove_subscription(registration_id).unwrap();
        conductor.do_work(2_000).unwrap();

        assert_eq!(conductor.subscriptions().len(), 0);
        assert_ne!(sub_correlation, remove_correlation);

        let mut seen_type = 0i32;
        let mut seen_correlation = 0i64;
        let got = events
            .receive_next(|msg_type, buf, offset, _len| {
                seen_type = msg_type;
                seen_correlation = OperationSucceededFlyweight::wrap(*buf, offset).correlation_id();
            })
            .unwrap();
        assert!(got);
        assert_eq!(seen_type, ON_OPERATION_SUCCESS);
        assert_eq!(seen_correlation, remove_correlation);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn sweeps_clients_that_stop_sending_keepalives() {
        let (cnc, path) = temp_cnc("liveness");
        let mut conductor = DriverConductor::new(Configuration::default(), cnc.clone()).unwrap();

        let client = proxy(&cnc);
        client.add_publication("aeron:ipc", 1001).unwrap();
        conductor.do_work(1_000).unwrap();
        assert_eq!(conductor.active_client_count(), 1);
        assert_eq!(conductor.publications().len(), 1);

        // Advance well past the liveness timeout with no keepalive.
        let swept = conductor.do_work(1_000 + LIVENESS_TIMEOUT_NS + 1);
        assert_eq!(swept.unwrap(), 1, "one client timed out");
        assert_eq!(conductor.active_client_count(), 0);
        assert_eq!(
            conductor.publications().len(),
            0,
            "timed-out client's resources removed"
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }
}
