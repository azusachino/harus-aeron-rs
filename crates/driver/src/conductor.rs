//! DriverConductor command processor and resource manager.
//!
//! Mirrors `io.aeron.driver.DriverConductor.java`. Polls the to-driver ring buffer of the
//! CnC file, decodes the client command flyweights that `aeron_client::driver_proxy` writes,
//! maintains publication/subscription registrations and client liveness, and publishes
//! response events on the to-clients broadcast buffer.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use agrona::AtomicBuffer;
use agrona::BroadcastTransmitter;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

use aeron_client::cnc_file::{CncFile, DEFAULT_FILE_PAGE_SIZE};
use aeron_client::command::{
    PublicationMessageFlyweight, RemoveMessageFlyweight, SubscriptionMessageFlyweight,
};
use aeron_client::driver_events::{
    ImageBuffersReadyFlyweight, ON_AVAILABLE_IMAGE, ON_EXCLUSIVE_PUBLICATION_READY,
    ON_OPERATION_SUCCESS, ON_PUBLICATION_READY, ON_SUBSCRIPTION_READY, OperationSucceededFlyweight,
    PublicationBuffersReadyFlyweight, SubscriptionReadyFlyweight,
};
use aeron_client::driver_proxy::{
    ADD_EXCLUSIVE_PUBLICATION, ADD_PUBLICATION, ADD_SUBSCRIPTION, CLIENT_CLOSE, CLIENT_KEEPALIVE,
    REMOVE_PUBLICATION, REMOVE_SUBSCRIPTION,
};
use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    FRAME_ALIGNMENT, align, set_initial_term_id, set_mtu_length, store_default_frame_header,
};
use aeron_client::protocol::{
    CURRENT_VERSION, DATA_HEADER_LENGTH, DATA_SESSION_ID_FIELD_OFFSET, DATA_TERM_ID_FIELD_OFFSET,
    DATA_TERM_OFFSET_FIELD_OFFSET, DataHeaderFlyweight, FRAME_LENGTH_FIELD_OFFSET, HDR_TYPE_DATA,
    HDR_TYPE_SETUP, SetupFlyweight, TYPE_FIELD_OFFSET,
};

use crate::configuration::Configuration;
use crate::error::{DriverError, Result};
use crate::media::receive_channel_endpoint::ReceiveChannelEndpoint;
use crate::media::send_channel_endpoint::SendChannelEndpoint;
use crate::media::udp_channel::UdpChannel;
use crate::media::udp_channel_transport::UdpChannelTransport;
use crate::network_publication::NetworkPublication;
use crate::publication_image::PublicationImage;

/// Source identity reported for IPC images — matches upstream's `aeron:ipc` channel identity.
const IPC_SOURCE_IDENTITY: &str = "aeron:ipc";

/// Source identity reported for network images.
const UDP_SOURCE_IDENTITY: &str = "aeron:udp";

/// Returns `true` for an IPC channel (`aeron:ipc`, optionally with parameters).
fn is_ipc_channel(channel: &str) -> bool {
    channel == "aeron:ipc" || channel.starts_with("aeron:ipc?")
}

/// Returns `true` for a UDP (network) channel.
fn is_udp_channel(channel: &str) -> bool {
    channel.starts_with("aeron:udp")
}

/// Round up to the next 4-byte boundary (variable-length command fields are SBE-aligned).
fn align4(value: usize) -> usize {
    (value + 3) & !3
}

#[derive(Debug, Clone)]
pub struct PublicationEntry {
    pub registration_id: i64,
    pub correlation_id: i64,
    pub client_id: i64,
    pub stream_id: i32,
    pub channel: String,
    pub is_exclusive: bool,
    pub is_ipc: bool,
    pub session_id: i32,
    /// Counter id of the publication limit position (driver-assigned).
    pub publication_limit_counter_id: i32,
    /// Absolute path of the shared log file backing this publication; `None` until a log
    /// buffer is allocated (network publications get theirs in the Sender data-path task).
    pub log_file_name: Option<String>,
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
    /// Directory that holds this driver's `publications/` and `images/` log files.
    aeron_dir: PathBuf,
    client_liveness_timeout_ns: i64,
    next_registration_id: i64,
    next_session_id: i32,
    next_counter_id: i32,
    publications: HashMap<i64, PublicationEntry>,
    subscriptions: HashMap<i64, SubscriptionEntry>,
    /// Registration id -> shared log buffer handle, kept mapped for the publication's life so
    /// that matching IPC images can hand subscribers the very same log (zero-copy IPC).
    log_buffers: HashMap<i64, LogBuffers>,
    /// Counter id -> position counter (publication limit / subscriber position). These stand
    /// in for the shared CnC counters region (not yet laid out); an in-process client shares
    /// these `Arc`s via [`DriverConductor::counters`] until that region lands.
    counters: HashMap<i32, Arc<AtomicI64>>,
    /// Network publications created this cycle, drained by the driver into the Sender.
    new_network_publications: Vec<NetworkPublication>,
    /// Receive endpoints created this cycle, drained by the driver into the Receiver.
    new_receive_endpoints: Vec<ReceiveChannelEndpoint>,
    /// Reassembly state for network images, keyed by session id.
    images: HashMap<i32, PublicationImage>,
    /// client id -> last observed keepalive timestamp (ns).
    clients: HashMap<i64, i64>,
}

impl DriverConductor {
    /// Construct a conductor bound to a mapped CnC file, storing its log files under `aeron_dir`.
    pub fn new(configuration: Configuration, cnc: CncFile, aeron_dir: PathBuf) -> Result<Self> {
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
            aeron_dir,
            client_liveness_timeout_ns,
            next_registration_id: 1,
            next_session_id: 1,
            next_counter_id: 1,
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            log_buffers: HashMap::new(),
            counters: HashMap::new(),
            new_network_publications: Vec::new(),
            new_receive_endpoints: Vec::new(),
            images: HashMap::new(),
            clients: HashMap::new(),
        })
    }

    /// Drain network publications created since the last call so the Sender can adopt them.
    pub fn take_new_network_publications(&mut self) -> Vec<NetworkPublication> {
        std::mem::take(&mut self.new_network_publications)
    }

    /// Drain receive endpoints created since the last call so the Receiver can adopt them.
    pub fn take_new_receive_endpoints(&mut self) -> Vec<ReceiveChannelEndpoint> {
        std::mem::take(&mut self.new_receive_endpoints)
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
                self.on_add_publication(client_id, correlation_id, stream_id, channel, exclusive)
            }
            Command::AddSubscription {
                client_id,
                correlation_id,
                stream_id,
                channel,
            } => {
                self.touch_client(client_id, now_ns);
                // The subscription's registration id is the client's correlation id — that is
                // the id the client keys its subscription by and the driver echoes back in
                // AVAILABLE_IMAGE (upstream SubscriptionLink semantics).
                let registration_id = correlation_id;
                self.subscriptions.insert(
                    registration_id,
                    SubscriptionEntry {
                        registration_id,
                        client_id,
                        stream_id,
                        channel: channel.clone(),
                    },
                );
                self.on_subscription_ready(correlation_id)?;
                if is_ipc_channel(&channel) {
                    self.link_ipc_subscription(registration_id, stream_id)?;
                } else if is_udp_channel(&channel) {
                    self.add_receive_endpoint(&channel)?;
                }
                Ok(())
            }
            Command::RemovePublication {
                correlation_id,
                registration_id,
            } => {
                if let Some(entry) = self.publications.remove(&registration_id) {
                    self.counters.remove(&entry.publication_limit_counter_id);
                }
                self.log_buffers.remove(&registration_id);
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

    fn next_session_id(&mut self) -> i32 {
        let id = self.next_session_id;
        self.next_session_id += 1;
        id
    }

    fn next_counter_id(&mut self) -> i32 {
        let id = self.next_counter_id;
        self.next_counter_id += 1;
        id
    }

    /// Register a publication and, for IPC, allocate its shared log buffer, notify the client
    /// with PUBLICATION_READY, then connect any subscription already waiting on the stream.
    fn on_add_publication(
        &mut self,
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: String,
        exclusive: bool,
    ) -> Result<()> {
        // The publication's registration id is the client's correlation id (single-publisher
        // path); the client keys its publication by the registration id echoed back here.
        let registration_id = correlation_id;
        let is_ipc = is_ipc_channel(&channel);
        let is_udp = is_udp_channel(&channel);
        let session_id = self.next_session_id();
        let publication_limit_counter_id = self.next_counter_id();
        // Publication limit starts at 0 (no window). IPC opens it when a subscriber connects;
        // UDP opens it at creation (real Status-Message flow control lands with task-8).
        self.counters
            .insert(publication_limit_counter_id, Arc::new(AtomicI64::new(0)));

        let log_file_name = if is_ipc {
            let term_length = self.configuration.ipc_term_buffer_length as i32;
            let mtu = self.configuration.ipc_mtu_length as i32;
            let (name, buffers) = self.allocate_log_buffers(
                registration_id,
                session_id,
                stream_id,
                term_length,
                mtu,
            )?;
            self.log_buffers.insert(registration_id, buffers);
            Some(name)
        } else if is_udp {
            let term_length = self.configuration.publication_term_buffer_length as i32;
            let mtu = self.configuration.mtu_length as i32;
            let (name, buffers) = self.allocate_log_buffers(
                registration_id,
                session_id,
                stream_id,
                term_length,
                mtu,
            )?;
            self.log_buffers.insert(registration_id, buffers.clone());
            self.create_network_publication(
                registration_id,
                session_id,
                stream_id,
                &channel,
                buffers,
                mtu,
            )?;
            self.open_publication_window(
                publication_limit_counter_id,
                self.configuration.publication_term_buffer_length,
            );
            Some(name)
        } else {
            None
        };

        self.publications.insert(
            registration_id,
            PublicationEntry {
                registration_id,
                correlation_id,
                client_id,
                stream_id,
                channel,
                is_exclusive: exclusive,
                is_ipc,
                session_id,
                publication_limit_counter_id,
                log_file_name: log_file_name.clone(),
            },
        );

        if let Some(log_file_name) = log_file_name {
            self.on_publication_ready(
                correlation_id,
                registration_id,
                session_id,
                stream_id,
                publication_limit_counter_id,
                &log_file_name,
                exclusive,
            )?;
            if is_ipc {
                self.link_ipc_publication(
                    stream_id,
                    session_id,
                    publication_limit_counter_id,
                    &log_file_name,
                )?;
            }
        }
        Ok(())
    }

    /// Bind a send channel endpoint for `channel` and queue a network publication over the
    /// shared log for the Sender to transmit.
    fn create_network_publication(
        &mut self,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        buffers: LogBuffers,
        mtu: i32,
    ) -> Result<()> {
        let udp_channel = UdpChannel::parse(channel)
            .map_err(|e| DriverError::IllegalState(format!("parse udp channel: {e}")))?;
        let destination = udp_channel.remote_data;
        let transport =
            UdpChannelTransport::bind(udp_channel.clone(), udp_channel.local_data, None, 0, 0)?;
        let endpoint = SendChannelEndpoint::new(udp_channel, transport);
        self.new_network_publications.push(NetworkPublication::new(
            registration_id,
            buffers,
            endpoint,
            destination,
            session_id,
            stream_id,
            0,
            mtu as usize,
        ));
        Ok(())
    }

    /// Bind a receive channel endpoint for a UDP subscription and queue it for the Receiver.
    fn add_receive_endpoint(&mut self, channel: &str) -> Result<()> {
        let udp_channel = UdpChannel::parse(channel)
            .map_err(|e| DriverError::IllegalState(format!("parse udp channel: {e}")))?;
        // Bind to the endpoint address so the publisher's frames arrive here.
        let bind = udp_channel.remote_data;
        let transport = UdpChannelTransport::bind(udp_channel.clone(), bind, None, 0, 0)?;
        self.new_receive_endpoints
            .push(ReceiveChannelEndpoint::new(udp_channel, transport));
        Ok(())
    }

    /// Dispatch one frame received off the wire: SETUP creates an image, DATA rebuilds it.
    pub fn on_received_frame(&mut self, frame: &[u8]) -> Result<()> {
        if frame.len() < DATA_HEADER_LENGTH {
            return Ok(());
        }
        let mut bytes = frame.to_vec();
        let buf = AtomicBuffer::new(&mut bytes);
        let frame_type = buf.get_i16(TYPE_FIELD_OFFSET) as u16;
        match frame_type {
            HDR_TYPE_SETUP => self.on_setup(&buf),
            HDR_TYPE_DATA => {
                self.on_data(&buf, frame.len());
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// A SETUP frame: allocate the image log for a matching UDP subscription and announce it.
    fn on_setup(&mut self, buf: &AtomicBuffer) -> Result<()> {
        let setup = SetupFlyweight::new(*buf);
        let session_id = setup.session_id();
        let stream_id = setup.stream_id();
        if self.images.contains_key(&session_id) {
            return Ok(());
        }

        let subscriber: Option<i64> = self
            .subscriptions
            .values()
            .find(|s| is_udp_channel(&s.channel) && s.stream_id == stream_id)
            .map(|s| s.registration_id);
        let Some(subscription_registration_id) = subscriber else {
            return Ok(());
        };

        let initial_term_id = setup.initial_term_id();
        let term_length = setup.term_length();
        let mtu_length = setup.mtu_length();
        let (log_file_name, buffers) = self.allocate_image_log_buffers(
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            mtu_length,
        )?;

        self.images.insert(
            session_id,
            PublicationImage::new(
                session_id,
                stream_id,
                initial_term_id,
                buffers,
                log_file_name.clone(),
            ),
        );
        self.on_available_image(
            session_id,
            stream_id,
            subscription_registration_id,
            &log_file_name,
            UDP_SOURCE_IDENTITY,
        )
    }

    /// A DATA frame (possibly several concatenated frames): rebuild each into its image.
    fn on_data(&mut self, buf: &AtomicBuffer, length: usize) {
        let session_id = buf.get_i32(DATA_SESSION_ID_FIELD_OFFSET);
        let Some(image) = self.images.get(&session_id) else {
            return;
        };
        let mut offset = 0usize;
        while offset + DATA_HEADER_LENGTH <= length {
            let frame_length = buf.get_i32(offset + FRAME_LENGTH_FIELD_OFFSET);
            if frame_length <= 0 {
                break;
            }
            let aligned = align(frame_length, FRAME_ALIGNMENT) as usize;
            let term_id = buf.get_i32(offset + DATA_TERM_ID_FIELD_OFFSET);
            let term_offset = buf.get_i32(offset + DATA_TERM_OFFSET_FIELD_OFFSET) as usize;

            let mut frame_bytes = vec![0u8; aligned.min(length - offset)];
            buf.get_bytes(offset, &mut frame_bytes);
            let frame_length = frame_bytes.len();
            let frame = AtomicBuffer::new(&mut frame_bytes);
            image.on_data_frame(term_id, term_offset, &frame, frame_length);

            offset += aligned;
        }
    }

    /// Create and map a fresh image log under `aeron_dir/images/` for a received stream.
    fn allocate_image_log_buffers(
        &self,
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        term_length: i32,
        mtu_length: i32,
    ) -> Result<(String, LogBuffers)> {
        let dir = self.aeron_dir.join("images");
        std::fs::create_dir_all(&dir)
            .map_err(|e| DriverError::IllegalState(format!("create images dir: {e}")))?;
        let path = dir.join(format!("{session_id}-{stream_id}.logbuffer"));

        let buffers = LogBuffers::map_new(&path, term_length, DEFAULT_FILE_PAGE_SIZE)
            .map_err(|e| DriverError::IllegalState(format!("map image log buffer: {e}")))?;

        let meta = buffers.meta_data_buffer();
        set_initial_term_id(&meta, initial_term_id);
        set_mtu_length(&meta, mtu_length);

        Ok((path.to_string_lossy().into_owned(), buffers))
    }

    /// Create and map a fresh IPC log file under `aeron_dir/publications/`, initialising its
    /// metadata so publishers and subscribers see a valid, shared log.
    fn allocate_log_buffers(
        &self,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        term_length: i32,
        mtu_length: i32,
    ) -> Result<(String, LogBuffers)> {
        let dir = self.aeron_dir.join("publications");
        std::fs::create_dir_all(&dir)
            .map_err(|e| DriverError::IllegalState(format!("create publications dir: {e}")))?;
        let path = dir.join(format!("{registration_id}.logbuffer"));

        let buffers = LogBuffers::map_new(&path, term_length, DEFAULT_FILE_PAGE_SIZE)
            .map_err(|e| DriverError::IllegalState(format!("map log buffer: {e}")))?;

        // Fresh logs start at initial term id 0; record the negotiated MTU so appenders frame
        // consistently with what the client (and the network wire) will read.
        let meta = buffers.meta_data_buffer();
        set_initial_term_id(&meta, 0);
        set_mtu_length(&meta, mtu_length);

        // Seed the default frame header (DATA type + session/stream ids) so the publisher's
        // HeaderWriter stamps real data frames — without this every frame reads as padding.
        let mut header_bytes = [0u8; DATA_HEADER_LENGTH];
        let header_buf = AtomicBuffer::new(&mut header_bytes);
        let header = DataHeaderFlyweight::new(header_buf);
        header.set_version(CURRENT_VERSION);
        header.set_header_type(HDR_TYPE_DATA);
        header.set_session_id(session_id);
        header.set_stream_id(stream_id);
        store_default_frame_header(&meta, &header_buf);

        let name = path.to_string_lossy().into_owned();
        Ok((name, buffers))
    }

    /// Connect a newly added IPC subscription to every matching existing IPC publication.
    fn link_ipc_subscription(
        &mut self,
        subscription_registration_id: i64,
        stream_id: i32,
    ) -> Result<()> {
        let matches: Vec<(i32, i32, String)> = self
            .publications
            .values()
            .filter(|p| p.is_ipc && p.stream_id == stream_id)
            .filter_map(|p| {
                p.log_file_name
                    .clone()
                    .map(|name| (p.session_id, p.publication_limit_counter_id, name))
            })
            .collect();
        for (session_id, publication_limit_counter_id, log_file_name) in matches {
            self.open_publication_window(
                publication_limit_counter_id,
                self.configuration.ipc_term_buffer_length,
            );
            self.on_available_image(
                session_id,
                stream_id,
                subscription_registration_id,
                &log_file_name,
                IPC_SOURCE_IDENTITY,
            )?;
        }
        Ok(())
    }

    /// Connect a newly added IPC publication to every matching existing IPC subscription.
    fn link_ipc_publication(
        &mut self,
        stream_id: i32,
        session_id: i32,
        publication_limit_counter_id: i32,
        log_file_name: &str,
    ) -> Result<()> {
        let subscribers: Vec<i64> = self
            .subscriptions
            .values()
            .filter(|s| is_ipc_channel(&s.channel) && s.stream_id == stream_id)
            .map(|s| s.registration_id)
            .collect();
        for subscription_registration_id in subscribers {
            self.open_publication_window(
                publication_limit_counter_id,
                self.configuration.ipc_term_buffer_length,
            );
            self.on_available_image(
                session_id,
                stream_id,
                subscription_registration_id,
                log_file_name,
                IPC_SOURCE_IDENTITY,
            )?;
        }
        Ok(())
    }

    /// Open a publication's flow-control window to half a term (the initial window).
    fn open_publication_window(&self, publication_limit_counter_id: i32, term_length: usize) {
        if let Some(limit) = self.counters.get(&publication_limit_counter_id) {
            limit.store((term_length / 2) as i64, Ordering::Release);
        }
    }

    /// Publication limit counter for the given id, shared with an in-process client.
    pub fn counters(&self) -> &HashMap<i32, Arc<AtomicI64>> {
        &self.counters
    }

    fn touch_client(&mut self, client_id: i64, now_ns: i64) {
        self.clients.insert(client_id, now_ns);
    }

    fn remove_client(&mut self, client_id: i64) {
        self.clients.remove(&client_id);
        let removed: Vec<i64> = self
            .publications
            .values()
            .filter(|p| p.client_id == client_id)
            .map(|p| p.registration_id)
            .collect();
        for registration_id in removed {
            if let Some(entry) = self.publications.remove(&registration_id) {
                self.counters.remove(&entry.publication_limit_counter_id);
            }
            self.log_buffers.remove(&registration_id);
        }
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

    #[allow(clippy::too_many_arguments)]
    fn on_publication_ready(
        &mut self,
        correlation_id: i64,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        publication_limit_counter_id: i32,
        log_file_name: &str,
        exclusive: bool,
    ) -> Result<()> {
        // 32-byte fixed body + length-prefixed log file name.
        let mut scratch = vec![0u8; 32 + 4 + log_file_name.len()];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        let fw = PublicationBuffersReadyFlyweight::wrap(buf, 0);
        fw.set_correlation_id(correlation_id);
        fw.set_registration_id(registration_id);
        fw.set_session_id(session_id);
        fw.set_stream_id(stream_id);
        fw.set_publication_limit_counter_id(publication_limit_counter_id);
        fw.set_channel_status_counter_id(-1);
        fw.set_log_file_name(log_file_name);
        let msg_type = if exclusive {
            ON_EXCLUSIVE_PUBLICATION_READY
        } else {
            ON_PUBLICATION_READY
        };
        self.transmit(msg_type, &buf, len)
    }

    fn on_available_image(
        &mut self,
        session_id: i32,
        stream_id: i32,
        subscription_registration_id: i64,
        log_file_name: &str,
        source_identity: &str,
    ) -> Result<()> {
        let correlation_id = self.next_id();
        let subscriber_position_id = self.next_counter_id();
        // The subscriber owns its position counter (starts at 0).
        self.counters
            .insert(subscriber_position_id, Arc::new(AtomicI64::new(0)));
        // 28-byte fixed body + length-prefixed log file name + length-prefixed source identity.
        let name_field = 4 + align4(log_file_name.len());
        let source_field = 4 + source_identity.len();
        let mut scratch = vec![0u8; 28 + name_field + source_field];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        let fw = ImageBuffersReadyFlyweight::wrap(buf, 0);
        fw.set_correlation_id(correlation_id);
        fw.set_session_id(session_id);
        fw.set_stream_id(stream_id);
        fw.set_subscription_registration_id(subscription_registration_id);
        fw.set_subscriber_position_id(subscriber_position_id);
        fw.set_log_file_name(log_file_name);
        fw.set_source_identity(source_identity);
        self.transmit(ON_AVAILABLE_IMAGE, &buf, len)
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

    /// A tiny IPC term length keeps test log files fast to map (64 KiB terms, not 64 MiB).
    fn small_config() -> Configuration {
        Configuration {
            ipc_term_buffer_length: aeron_client::logbuffer::TERM_MIN_LENGTH as usize,
            ..Configuration::default()
        }
    }

    /// A per-test scratch directory for the driver's log files, removed on drop.
    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn temp_dir(tag: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!(
            "aeron-conductor-dir-{}-{}-{:?}",
            tag,
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        TempDir(path)
    }

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
        let dir = temp_dir("register");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

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
        let dir = temp_dir("subready");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

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
        let dir = temp_dir("remove");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

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
        let dir = temp_dir("liveness");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

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

    #[test]
    fn ipc_publication_allocates_shared_log_and_emits_publication_ready() {
        let (cnc, path) = temp_cnc("ipcpub");
        let dir = temp_dir("ipcpub");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        let correlation_id = client.add_publication("aeron:ipc", 1001).unwrap();
        conductor.do_work(1_000).unwrap();

        // The publication carries a real, mappable log file.
        let entry = conductor.publications().values().next().unwrap().clone();
        assert!(entry.is_ipc);
        let log_file_name = entry.log_file_name.clone().expect("ipc log allocated");
        let mapped = LogBuffers::map_existing(&log_file_name).expect("shared log mappable");
        assert_eq!(mapped.term_length(), small_config().ipc_term_buffer_length);

        // And PUBLICATION_READY is delivered to the client with the same log file name.
        let mut seen_type = 0i32;
        let mut seen_name = String::new();
        let mut seen_session = 0i32;
        let got = events
            .receive_next(|msg_type, buf, offset, _len| {
                seen_type = msg_type;
                let fw = PublicationBuffersReadyFlyweight::wrap(*buf, offset);
                seen_session = fw.session_id();
                seen_name = fw.log_file_name().unwrap();
            })
            .unwrap();
        assert!(got);
        assert_eq!(seen_type, ON_PUBLICATION_READY);
        assert_eq!(seen_name, log_file_name);
        assert_eq!(seen_session, entry.session_id);
        assert_eq!(correlation_id, entry.correlation_id);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn ipc_publication_and_subscription_connect_with_available_image() {
        let (cnc, path) = temp_cnc("ipcimage");
        let dir = temp_dir("ipcimage");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();

        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        // Subscription first, publication second — the image links on the later add.
        let sub_correlation = client.add_subscription("aeron:ipc", 7).unwrap();
        client.add_publication("aeron:ipc", 7).unwrap();
        conductor.do_work(1_000).unwrap();

        let pub_log = conductor
            .publications()
            .values()
            .next()
            .unwrap()
            .log_file_name
            .clone()
            .unwrap();

        // Drain events, capturing the AVAILABLE_IMAGE.
        let mut image_name = None;
        let mut image_sub_reg = 0i64;
        let mut image_source = String::new();
        while events
            .receive_next(|msg_type, buf, offset, _len| {
                if msg_type == ON_AVAILABLE_IMAGE {
                    let fw = ImageBuffersReadyFlyweight::wrap(*buf, offset);
                    image_name = Some(fw.log_file_name().unwrap());
                    image_sub_reg = fw.subscription_registration_id();
                    image_source = fw.source_identity().unwrap();
                }
            })
            .unwrap()
        {}

        assert_eq!(image_name.as_deref(), Some(pub_log.as_str()));
        assert_eq!(image_sub_reg, sub_correlation);
        assert_eq!(image_source, "aeron:ipc");

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }
}
