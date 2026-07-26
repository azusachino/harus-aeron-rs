//! DriverConductor command processor and resource manager.
//!
//! Mirrors `io.aeron.driver.DriverConductor.java`. Polls the to-driver ring buffer of the
//! CnC file, decodes the client command flyweights that `aeron_client::driver_proxy` writes,
//! maintains publication/subscription registrations and client liveness, and publishes
//! response events on the to-clients broadcast buffer.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use agrona::AtomicBuffer;
use agrona::BroadcastTransmitter;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

use aeron_client::cnc_file::{CncFile, DEFAULT_FILE_PAGE_SIZE};
use aeron_client::command::{
    CounterMessageFlyweight, PublicationMessageFlyweight, RemoveMessageFlyweight,
    SubscriptionMessageFlyweight, TerminateDriverFlyweight,
};
use aeron_client::counters::{
    CONTROL_PROTOCOL_SEMANTIC_VERSION, DRIVER_PUBLISHER_LIMIT_TYPE_ID,
    DRIVER_SUBSCRIBER_POSITION_TYPE_ID, DRIVER_SYSTEM_COUNTER_TYPE_ID,
    SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION,
};
use aeron_client::driver_events::{
    ClientTimeoutFlyweight, ErrorResponseFlyweight, ImageBuffersReadyFlyweight, ON_AVAILABLE_IMAGE,
    ON_CLIENT_TIMEOUT, ON_COUNTER_READY, ON_ERROR, ON_EXCLUSIVE_PUBLICATION_READY,
    ON_NEXT_AVAILABLE_SESSION_ID, ON_OPERATION_SUCCESS, ON_PUBLICATION_READY,
    ON_SUBSCRIPTION_READY, ON_UNAVAILABLE_COUNTER, ON_UNAVAILABLE_IMAGE,
    OperationSucceededFlyweight, PublicationBuffersReadyFlyweight, SubscriptionReadyFlyweight,
};
use aeron_client::driver_proxy::{
    ADD_COUNTER, ADD_EXCLUSIVE_PUBLICATION, ADD_PUBLICATION, ADD_SUBSCRIPTION, CLIENT_CLOSE,
    CLIENT_KEEPALIVE, GET_NEXT_AVAILABLE_SESSION_ID, REMOVE_COUNTER, REMOVE_PUBLICATION,
    REMOVE_SUBSCRIPTION, TERMINATE_DRIVER,
};
use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    FRAME_ALIGNMENT, align, set_initial_term_id, set_mtu_length, store_default_frame_header,
};
use aeron_client::protocol::{
    CURRENT_VERSION, DATA_HEADER_LENGTH, DATA_SESSION_ID_FIELD_OFFSET, DATA_STREAM_ID_FIELD_OFFSET,
    DATA_TERM_ID_FIELD_OFFSET, DATA_TERM_OFFSET_FIELD_OFFSET, DataHeaderFlyweight, EOS_FLAG,
    FLAGS_FIELD_OFFSET, FRAME_LENGTH_FIELD_OFFSET, HDR_TYPE_DATA, HDR_TYPE_SETUP, SetupFlyweight,
    TYPE_FIELD_OFFSET,
};
use aeron_client::uri::{ChannelUri, RELIABLE_PARAM_NAME, SESSION_ID_PARAM_NAME};

use crate::configuration::Configuration;
use crate::error::{DriverError, Result};
use crate::flow_control::{
    MULTICAST_RRWM_DEFAULT, UNICAST_RRWM_DEFAULT, retransmit_receiver_window_multiple,
};
use crate::media::receive_channel_endpoint::ReceiveChannelEndpoint;
use crate::media::send_channel_endpoint::SendChannelEndpoint;
use crate::media::udp_channel::UdpChannel;
use crate::media::udp_channel_transport::UdpChannelTransport;
use crate::network_publication::NetworkPublication;
use crate::publication_image::{ImageLifecycleEvent, PublicationImage};
use crate::publication_params::{DummyPublicationLookup, PublicationParams};
use crate::receiver::ReceiverStatus;

/// Source identity reported for IPC images — matches upstream's `aeron:ipc` channel identity.
const IPC_SOURCE_IDENTITY: &str = "aeron:ipc";

/// Source identity reported for network images.
const UDP_SOURCE_IDENTITY: &str = "aeron:udp";

const IMAGE_LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;
/// Matches `Configuration.STATUS_MESSAGE_TIMEOUT_DEFAULT_NS` in vendored Aeron; paces the image
/// DRAINING->LINGER release window.
const STATUS_MESSAGE_TIMEOUT_NS: i64 = 200_000_000;
/// Matches `Configuration.PENDING_SETUPS_TIMEOUT_NS` in vendored Aeron.
const PENDING_SETUPS_TIMEOUT_NS: i64 = 1_000_000_000;

type NetworkImageKey = (String, i32, i32, SocketAddr);

/// A NAK an image wants sent upstream, tagged with the endpoint identity needed to route it.
///
/// Loss detection is image-driven (upstream `PublicationImage`), but the receive endpoint that
/// must transmit the NAK lives in the `Receiver`; the driver ferries these across that boundary.
#[derive(Debug, Clone)]
pub struct PendingNak {
    pub channel: String,
    pub session_id: i32,
    pub stream_id: i32,
    pub source: SocketAddr,
    pub term_id: i32,
    pub term_offset: i32,
    pub length: i32,
}
type NetworkSessionKey = NetworkImageKey;

/// Receive-side session states from Aeron's `DataPacketDispatcher`.
///
/// Reference: `vendor/aeron/aeron-driver/src/main/java/io/aeron/driver/DataPacketDispatcher.java`
/// (`SessionState`, `onDataPacket`, and `onSetupMessage`). This is the Rust equivalent of
/// Aeron's `DataPacketDispatcher`; the dispatcher is intentionally kept in the conductor so
/// endpoint ownership, image lifecycle, and CnC events share one state machine. This state is
/// intentionally separate from `images`: DATA may arrive before SETUP, and cooldown/no-interest
/// sessions must not create an image just because a later SETUP is received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkSessionState {
    PendingSetupFrame,
    InitInProgress,
    Active,
    OnCoolDown,
    NoInterest,
}

/// Returns `true` for an IPC channel (`aeron:ipc`, optionally with parameters).
fn is_ipc_channel(channel: &str) -> bool {
    channel == "aeron:ipc" || channel.starts_with("aeron:ipc?")
}

/// Returns `true` for a UDP (network) channel.
fn is_udp_channel(channel: &str) -> bool {
    channel.starts_with("aeron:udp")
}

/// Return the transport identity and options that affect subscription dispatch.
///
/// Reliability is a subscription property, while `session-id` narrows the interest within an
/// otherwise shared transport. Removing both from the identity lets the conductor reject the
/// same-session reliability conflict without treating subscriptions for different sessions as
/// conflicting.
fn subscription_options(channel: &str) -> Result<(String, Option<i32>, bool)> {
    let mut uri = ChannelUri::parse(channel)
        .map_err(|error| DriverError::InvalidChannel(format!("parse channel: {error}")))?;
    let session_id = uri
        .get(SESSION_ID_PARAM_NAME)
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel(format!("invalid session-id in channel: {value}"))
            })
        })
        .transpose()?;
    let reliable = uri
        .get(RELIABLE_PARAM_NAME)
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                DriverError::InvalidChannel(format!("invalid reliable value in channel: {value}"))
            })
        })
        .transpose()?
        .unwrap_or(true);
    uri.remove(RELIABLE_PARAM_NAME);
    uri.remove(SESSION_ID_PARAM_NAME);
    Ok((uri.to_string(), session_id, reliable))
}

fn subscription_conflicts(
    existing: &SubscriptionEntry,
    identity: &str,
    stream_id: i32,
    session_id: Option<i32>,
    reliable: bool,
) -> bool {
    subscription_options(&existing.channel)
        .is_ok_and(|(existing_identity, _, _)| existing_identity == identity)
        && existing.stream_id == stream_id
        && existing.reliable != reliable
        && match (existing.session_id, session_id) {
            (None, None) => true,
            (Some(existing), Some(candidate)) => existing == candidate,
            _ => false,
        }
}

/// Return the publication channel identity and an optional explicitly requested session id.
/// Session ids are part of publication identity only for conflict validation; the transport
/// channel itself remains reusable across sessions.
fn publication_options(channel: &str) -> Result<(String, Option<i32>)> {
    let mut uri = ChannelUri::parse(channel)
        .map_err(|error| DriverError::InvalidChannel(format!("parse channel: {error}")))?;
    let session_id = uri
        .get(SESSION_ID_PARAM_NAME)
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel(format!("invalid session-id in channel: {value}"))
            })
        })
        .transpose()?;
    uri.remove(SESSION_ID_PARAM_NAME);
    Ok((uri.to_string(), session_id))
}

/// Round up to the next 4-byte boundary (variable-length command fields are SBE-aligned).
fn align4(value: usize) -> usize {
    (value + 3) & !3
}

fn stable_channel_tag(channel: &str) -> String {
    let mut hasher = DefaultHasher::new();
    channel.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
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
    /// Session-specific subscription interest, or `None` for wildcard/all sessions.
    pub session_id: Option<i32>,
    /// Whether this subscription expects reliable delivery; defaults to true in Aeron URI
    /// semantics.
    pub reliable: bool,
}

#[derive(Debug)]
struct ReceiveChannelReference {
    references: usize,
    /// The Receiver identity of the endpoint that was actually bound. Wildcard ports can
    /// receive a unique canonical suffix each time their URI is parsed.
    canonical_form: String,
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
    AddCounter {
        client_id: i64,
        correlation_id: i64,
        type_id: i32,
        key: Vec<u8>,
        label: String,
    },
    RemovePublication {
        correlation_id: i64,
        registration_id: i64,
    },
    RemoveSubscription {
        correlation_id: i64,
        registration_id: i64,
    },
    RemoveCounter {
        correlation_id: i64,
        registration_id: i64,
    },
    GetNextAvailableSessionId {
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
    },
    Keepalive {
        client_id: i64,
    },
    ClientClose {
        client_id: i64,
    },
    TerminateDriver {
        client_id: i64,
        correlation_id: i64,
    },
    Unknown {
        correlation_id: i64,
        message_type: i32,
    },
}

impl Command {
    fn correlation_id(&self) -> Option<i64> {
        match self {
            Self::AddPublication { correlation_id, .. }
            | Self::AddSubscription { correlation_id, .. }
            | Self::AddCounter { correlation_id, .. }
            | Self::RemovePublication { correlation_id, .. }
            | Self::RemoveSubscription { correlation_id, .. }
            | Self::RemoveCounter { correlation_id, .. }
            | Self::GetNextAvailableSessionId { correlation_id, .. }
            | Self::TerminateDriver { correlation_id, .. }
            | Self::Unknown { correlation_id, .. } => Some(*correlation_id),
            Self::Keepalive { .. } | Self::ClientClose { .. } => None,
        }
    }
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
    /// Client counter registration id -> allocated counter id and owner.
    client_counters: HashMap<i64, (i64, i32)>,
    /// Subscriber-position counter id keyed by the subscription and image notification that own it.
    subscriber_counters: HashMap<(i64, i64), i32>,
    /// Network publications created this cycle, drained by the driver into the Sender.
    new_network_publications: Vec<NetworkPublication>,
    /// Receive endpoints created this cycle, drained by the driver into the Receiver.
    new_receive_endpoints: Vec<ReceiveChannelEndpoint>,
    /// Channel identities whose subscription interest was removed this cycle.
    removed_receive_channels: Vec<String>,
    /// Number of subscriptions sharing each receive channel endpoint.
    ///
    /// Upstream `ReceiveChannelEndpoint` owns one transport per channel and aggregates
    /// stream/session interest through its `DataPacketDispatcher`; it does not bind one socket
    /// per subscription.
    receive_channel_references: HashMap<String, ReceiveChannelReference>,
    /// Network publication registrations whose sender state must be removed this cycle.
    removed_network_publications: Vec<i64>,
    /// Reassembly state for network images, keyed by channel, session, stream, and source.
    ///
    /// The channel endpoint is part of Aeron's dispatcher identity. Stream/session alone is not
    /// sufficient when two UDP channels reuse those numeric IDs.
    images: HashMap<NetworkImageKey, PublicationImage>,
    /// DataPacketDispatcher-equivalent state for network sessions without an active image.
    session_states: HashMap<NetworkSessionKey, (NetworkSessionState, i64)>,
    /// DATA-before-SETUP sessions for which the one-shot setup-eliciting SM was sent.
    setup_elicited: HashMap<NetworkSessionKey, i64>,
    /// client id -> last observed keepalive timestamp (ns).
    clients: HashMap<i64, i64>,
    termination_requested: bool,
}

impl DriverConductor {
    /// Construct a conductor bound to a mapped CnC file, storing its log files under `aeron_dir`.
    pub fn new(configuration: Configuration, cnc: CncFile, aeron_dir: PathBuf) -> Result<Self> {
        let to_driver = ManyToOneRingBuffer::new(cnc.to_driver_buffer())
            .map_err(|e| DriverError::IllegalState(format!("to-driver ring buffer: {e}")))?;
        let to_clients = BroadcastTransmitter::new(cnc.to_clients_buffer())
            .map_err(|e| DriverError::IllegalState(format!("to-clients broadcast: {e}")))?;
        let client_liveness_timeout_ns = cnc.client_liveness_timeout_ns();

        // Publish the control protocol version in the standard system-counter slot so a Java
        // Aeron client can negotiate the same command surface from the shared CnC file.
        if cnc.counters_metadata_length()
            >= aeron_client::cnc_file::COUNTER_METADATA_LENGTH
                * (SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION as usize + 1)
        {
            cnc.write_counter_metadata(
                SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION,
                DRIVER_SYSTEM_COUNTER_TYPE_ID,
                SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION as i64,
                CONTROL_PROTOCOL_SEMANTIC_VERSION,
                "Control protocol version",
            )?;
        }

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
            client_counters: HashMap::new(),
            subscriber_counters: HashMap::new(),
            new_network_publications: Vec::new(),
            new_receive_endpoints: Vec::new(),
            removed_receive_channels: Vec::new(),
            receive_channel_references: HashMap::new(),
            removed_network_publications: Vec::new(),
            images: HashMap::new(),
            session_states: HashMap::new(),
            setup_elicited: HashMap::new(),
            clients: HashMap::new(),
            termination_requested: false,
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

    /// Drain receive-channel removals for the Receiver agent.
    pub fn take_removed_receive_channels(&mut self) -> Vec<String> {
        std::mem::take(&mut self.removed_receive_channels)
    }

    /// Drain network-publication removals for the Sender agent.
    pub fn take_removed_network_publications(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.removed_network_publications)
    }

    /// One conductor duty cycle: drain and dispatch client commands, then sweep liveness.
    pub fn do_work(&mut self, now_ns: i64) -> Result<usize> {
        // Aeron clients use this trailer timestamp to distinguish a live driver from a stale
        // CnC directory before they publish their first command.
        self.to_driver
            .set_consumer_heartbeat_time(now_ns / 1_000_000);
        // Decode records into owned commands first; the ring-buffer read borrow cannot be
        // held while we mutate conductor state.
        let mut commands: Vec<Command> = Vec::new();
        let read = self.to_driver.read(|msg_type, buffer, offset, length| {
            if let Some(cmd) = decode_command(msg_type, buffer, offset, length) {
                commands.push(cmd);
            }
        });

        let mut work = read;
        for command in commands {
            let correlation_id = command.correlation_id();
            if let Err(error) = self.dispatch(command, now_ns) {
                if let Some(correlation_id) = correlation_id {
                    self.on_error(correlation_id, error_code(&error), error.to_string())?;
                    work += 1;
                } else {
                    return Err(error);
                }
            }
        }

        work += self.sweep_inactive_images(now_ns)?;
        work += self.sweep_timed_out_clients(now_ns)?;
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
                if !is_ipc_channel(&channel) && !is_udp_channel(&channel) {
                    return Err(DriverError::InvalidChannel(format!(
                        "unsupported media in channel: {channel}"
                    )));
                }
                let (identity, session_id, reliable) = subscription_options(&channel)?;
                if is_udp_channel(&channel)
                    && self.subscriptions.values().any(|existing| {
                        subscription_conflicts(existing, &identity, stream_id, session_id, reliable)
                    })
                {
                    return Err(DriverError::IllegalState(format!(
                        "conflicting reliable and unreliable subscriptions for channel {identity}, stream {stream_id}, session {session_id:?}"
                    )));
                }
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
                        session_id,
                        reliable,
                    },
                );
                self.clear_no_interest_for_subscription(&channel, stream_id, session_id);
                self.on_subscription_ready(correlation_id)?;
                if is_ipc_channel(&channel) {
                    self.link_ipc_subscription(registration_id, stream_id)?;
                } else if is_udp_channel(&channel) {
                    self.add_receive_endpoint(&channel)?;
                    self.link_network_subscription(registration_id, stream_id)?;
                }
                Ok(())
            }
            Command::AddCounter {
                client_id,
                correlation_id,
                type_id,
                key,
                label,
            } => {
                self.touch_client(client_id, now_ns);
                let counter_id = self.next_counter_id();
                self.counters
                    .insert(counter_id, Arc::new(AtomicI64::new(0)));
                self.publish_counter_with_registration_and_key(
                    counter_id,
                    type_id,
                    correlation_id,
                    0,
                    &key,
                    &label,
                )?;
                self.client_counters
                    .insert(correlation_id, (client_id, counter_id));
                self.on_counter_ready(correlation_id, counter_id)
            }
            Command::RemovePublication {
                correlation_id,
                registration_id,
            } => {
                if let Some(entry) = self.publications.remove(&registration_id) {
                    if !entry.is_ipc {
                        self.removed_network_publications.push(registration_id);
                    }
                    self.counters.remove(&entry.publication_limit_counter_id);
                    self.cnc.free_counter(entry.publication_limit_counter_id)?;
                }
                self.log_buffers.remove(&registration_id);
                self.on_operation_success(correlation_id)
            }
            Command::RemoveSubscription {
                correlation_id,
                registration_id,
            } => {
                if let Some(subscription) = self.subscriptions.remove(&registration_id) {
                    self.detach_images_for_subscription(registration_id, now_ns)?;
                    if let Some(endpoint_key) = self.release_receive_endpoint(&subscription.channel)
                    {
                        self.removed_receive_channels.push(endpoint_key);
                    }
                }
                self.on_operation_success(correlation_id)
            }
            Command::RemoveCounter {
                correlation_id,
                registration_id,
            } => {
                if let Some((_client_id, counter_id)) =
                    self.client_counters.remove(&registration_id)
                {
                    self.counters.remove(&counter_id);
                    self.cnc.free_counter(counter_id)?;
                    self.on_counter_unavailable(registration_id, counter_id)
                } else {
                    self.on_operation_success(correlation_id)
                }
            }
            Command::GetNextAvailableSessionId {
                client_id,
                correlation_id,
                stream_id: _stream_id,
            } => {
                self.touch_client(client_id, now_ns);
                let session_id = self.next_session_id();
                self.on_next_available_session_id(correlation_id, session_id)
            }
            Command::Keepalive { client_id } => {
                self.touch_client(client_id, now_ns);
                Ok(())
            }
            Command::ClientClose { client_id } => self.remove_client(client_id, now_ns),
            Command::TerminateDriver {
                client_id,
                correlation_id,
            } => {
                self.touch_client(client_id, now_ns);
                // The upstream default is DefaultDenyTerminationValidator. The boolean is an
                // explicit Rust adaptation until a pluggable validator receives the same
                // directory/token callback surface.
                if self.configuration.allow_termination {
                    self.termination_requested = true;
                    self.on_operation_success(correlation_id)
                } else {
                    Ok(())
                }
            }
            Command::Unknown {
                correlation_id,
                message_type,
            } => self.on_error(
                correlation_id,
                6,
                format!("ERROR - command typeId={message_type}"),
            ),
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
        if !is_ipc && !is_udp {
            return Err(DriverError::InvalidChannel(format!(
                "unsupported media in channel: {channel}"
            )));
        }
        let (identity, requested_session_id) = publication_options(&channel)?;
        let channel_uri = ChannelUri::parse(&channel)
            .map_err(|error| DriverError::InvalidChannel(format!("parse channel: {error}")))?;
        let params = PublicationParams::get_publication_params(
            &channel_uri,
            &self.configuration,
            &DummyPublicationLookup,
            stream_id,
            &channel,
        )?;
        let session_id = requested_session_id.unwrap_or_else(|| self.next_session_id());
        if let Some(existing) = self.publications.values().find(|publication| {
            publication.stream_id == stream_id
                && publication_options(&publication.channel)
                    .is_ok_and(|(existing_identity, _)| existing_identity == identity)
                && publication.session_id != session_id
        }) {
            return Err(DriverError::IllegalState(format!(
                "publication session-id {} does not match existing session-id {}",
                session_id, existing.session_id
            )));
        }
        if exclusive
            && self.publications.values().any(|publication| {
                publication.stream_id == stream_id
                    && publication.session_id == session_id
                    && publication_options(&publication.channel)
                        .is_ok_and(|(existing_identity, _)| existing_identity == identity)
            })
        {
            return Err(DriverError::InvalidChannel(format!(
                "exclusive publication session-id {session_id} is already in use"
            )));
        }
        let publication_limit_counter_id = self.next_counter_id();
        // Publication limit starts at 0 (no window). IPC opens it when a subscriber connects;
        // UDP opens it at creation (real Status-Message flow control lands with task-8).
        self.counters
            .insert(publication_limit_counter_id, Arc::new(AtomicI64::new(0)));
        self.publish_counter(
            publication_limit_counter_id,
            DRIVER_PUBLISHER_LIMIT_TYPE_ID,
            0,
            "Publisher limit",
        )?;

        let log_file_name = if is_ipc {
            let term_length = params.term_length as i32;
            let mtu = params.mtu_length as i32;
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
            let term_length = params.term_length as i32;
            let mtu = params.mtu_length as i32;
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
                self.counters
                    .get(&publication_limit_counter_id)
                    .cloned()
                    .ok_or_else(|| {
                        DriverError::IllegalState(format!(
                            "missing publication limit counter {publication_limit_counter_id}"
                        ))
                    })?,
            )?;
            self.open_publication_window(
                publication_limit_counter_id,
                self.configuration.publication_term_buffer_length,
            )?;
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
                    registration_id,
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
    #[allow(clippy::too_many_arguments)]
    fn create_network_publication(
        &mut self,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        buffers: LogBuffers,
        mtu: i32,
        publication_limit: Arc<AtomicI64>,
    ) -> Result<()> {
        let udp_channel = UdpChannel::parse(channel)
            .map_err(|e| DriverError::InvalidChannel(format!("parse udp channel: {e}")))?;
        let rrwm_default = if udp_channel.is_multicast {
            MULTICAST_RRWM_DEFAULT
        } else {
            UNICAST_RRWM_DEFAULT
        };
        let retransmit_receiver_window_multiple =
            retransmit_receiver_window_multiple(&udp_channel, rrwm_default)?;
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
            publication_limit,
            retransmit_receiver_window_multiple,
        ));
        Ok(())
    }

    /// Bind a receive channel endpoint for a UDP subscription and queue it for the Receiver.
    fn add_receive_endpoint(&mut self, channel: &str) -> Result<()> {
        let udp_channel = UdpChannel::parse(channel)
            .map_err(|e| DriverError::InvalidChannel(format!("parse udp channel: {e}")))?;
        let (endpoint_key, _, _) = subscription_options(channel)?;
        if let Some(reference) = self.receive_channel_references.get_mut(&endpoint_key) {
            reference.references += 1;
            return Ok(());
        }

        // Bind to the endpoint address so the publisher's frames arrive here.
        let bind = udp_channel.remote_data;
        let transport = UdpChannelTransport::bind(udp_channel.clone(), bind, None, 0, 0)?;
        let canonical_form = udp_channel.canonical_form.clone();
        self.new_receive_endpoints
            .push(ReceiveChannelEndpoint::new(udp_channel, transport));
        self.receive_channel_references.insert(
            endpoint_key,
            ReceiveChannelReference {
                references: 1,
                canonical_form,
            },
        );
        Ok(())
    }

    /// Drop one subscription's claim on a channel endpoint.
    ///
    /// Returns the canonical endpoint key only when the transport itself has no remaining
    /// subscription references and should be removed from the Receiver agent.
    fn release_receive_endpoint(&mut self, channel: &str) -> Option<String> {
        let endpoint_key = subscription_options(channel)
            .ok()
            .map(|options| options.0)?;
        let reference = self.receive_channel_references.get_mut(&endpoint_key)?;
        if reference.references > 1 {
            reference.references -= 1;
            None
        } else {
            let reference = self.receive_channel_references.remove(&endpoint_key)?;
            Some(reference.canonical_form)
        }
    }

    /// Dispatch one frame received off the wire: SETUP creates an image, DATA rebuilds it.
    pub fn on_received_frame(
        &mut self,
        frame: &[u8],
        source: SocketAddr,
        now_ns: i64,
    ) -> Result<()> {
        self.on_received_frame_for_channel(frame, source, now_ns, None)
    }

    /// Dispatch a frame received by a specific channel endpoint.
    ///
    /// `None` is retained for unit-test callers that inject a frame without endpoint context;
    /// real MediaDriver traffic always supplies the channel URI.
    pub fn on_received_frame_for_channel(
        &mut self,
        frame: &[u8],
        source: SocketAddr,
        now_ns: i64,
        channel: Option<&str>,
    ) -> Result<()> {
        self.on_received_frame_with_status(frame, source, now_ns, channel, None)
    }

    /// Dispatch a frame together with the receiver Status Message position reached while
    /// processing its datagram.
    pub fn on_received_frame_with_status(
        &mut self,
        frame: &[u8],
        source: SocketAddr,
        now_ns: i64,
        channel: Option<&str>,
        receiver_status: Option<ReceiverStatus>,
    ) -> Result<()> {
        if frame.len() < DATA_HEADER_LENGTH {
            return Ok(());
        }
        let mut bytes = frame.to_vec();
        let buf = AtomicBuffer::new(&mut bytes);
        let frame_type = buf.get_i16(TYPE_FIELD_OFFSET) as u16;
        match frame_type {
            HDR_TYPE_SETUP => self.on_setup(&buf, source, now_ns, channel),
            HDR_TYPE_DATA => {
                self.on_data(&buf, frame.len(), source, now_ns, channel, receiver_status);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// A SETUP frame: allocate the image log for a matching UDP subscription and announce it.
    fn on_setup(
        &mut self,
        buf: &AtomicBuffer,
        source: SocketAddr,
        now_ns: i64,
        channel: Option<&str>,
    ) -> Result<()> {
        let setup = SetupFlyweight::new(*buf);
        let session_id = setup.session_id();
        let stream_id = setup.stream_id();
        let channel_key = channel.unwrap_or("").to_owned();
        let image_key = (channel_key, session_id, stream_id, source);
        if self.images.contains_key(&image_key) {
            self.setup_elicited.remove(&image_key);
            self.session_states
                .insert(image_key.clone(), (NetworkSessionState::Active, now_ns));
            let Some(log_file_name) = self
                .images
                .get(&image_key)
                .map(|image| image.log_file_name().to_owned())
            else {
                return Ok(());
            };
            let subscribers: Vec<i64> = self
                .subscriptions
                .values()
                .filter(|s| {
                    is_udp_channel(&s.channel)
                        && s.stream_id == stream_id
                        && s.session_id.is_none_or(|id| id == session_id)
                        && channel.is_none_or(|value| s.channel == value)
                })
                .map(|s| s.registration_id)
                .collect();
            let mut new_links = Vec::new();
            for subscription_registration_id in subscribers {
                let already_linked = self
                    .images
                    .get(&image_key)
                    .is_some_and(|image| image.has_subscription(subscription_registration_id));
                if !already_linked {
                    let correlation_id = self.on_available_image(
                        session_id,
                        stream_id,
                        subscription_registration_id,
                        &log_file_name,
                        UDP_SOURCE_IDENTITY,
                    )?;
                    new_links.push((subscription_registration_id, correlation_id));
                }
            }
            if let Some(image) = self.images.get_mut(&image_key) {
                for (subscription_registration_id, correlation_id) in new_links {
                    image.add_subscription_link(subscription_registration_id, correlation_id);
                }
                image.touch(now_ns);
            }
            return Ok(());
        }

        if let Some((NetworkSessionState::OnCoolDown, entered_at)) =
            self.session_states.get(&image_key).copied()
        {
            if now_ns.saturating_sub(entered_at) < IMAGE_LIVENESS_TIMEOUT_NS {
                return Ok(());
            }
            self.session_states.remove(&image_key);
        }

        let subscribers: Vec<i64> = self
            .subscriptions
            .values()
            .filter(|s| {
                is_udp_channel(&s.channel)
                    && s.stream_id == stream_id
                    && s.session_id.is_none_or(|id| id == session_id)
                    && channel.is_none_or(|value| s.channel == value)
            })
            .map(|s| s.registration_id)
            .collect();
        if subscribers.is_empty() {
            // Upstream records NO_INTEREST so repeated SETUP frames do not repeatedly attempt
            // image creation until a matching subscription clears that state.
            self.session_states
                .insert(image_key.clone(), (NetworkSessionState::NoInterest, now_ns));
            self.setup_elicited.remove(&image_key);
            return Ok(());
        }

        let active_sessions: HashSet<i32> = self
            .images
            .keys()
            .filter(|(image_channel, _, image_stream, _)| {
                image_channel == &image_key.0 && *image_stream == stream_id
            })
            .map(|(_, image_session, _, _)| *image_session)
            .collect();
        if active_sessions.len() >= self.configuration.stream_session_limit {
            // Upstream rejects the SETUP when DataPacketDispatcher's stream session limit is
            // reached. The compact driver drops the frame and records NO_INTEREST so the duty
            // cycle remains alive for untrusted network input.
            self.session_states
                .insert(image_key.clone(), (NetworkSessionState::NoInterest, now_ns));
            return Ok(());
        }

        match self.session_states.get(&image_key).map(|(state, _)| *state) {
            Some(NetworkSessionState::InitInProgress)
            | Some(NetworkSessionState::Active)
            | Some(NetworkSessionState::NoInterest) => return Ok(()),
            Some(NetworkSessionState::PendingSetupFrame) | None => {}
            Some(NetworkSessionState::OnCoolDown) => return Ok(()),
        }
        // This is the INIT_IN_PROGRESS transition in DataPacketDispatcher.onSetupMessage.
        self.session_states.insert(
            image_key.clone(),
            (NetworkSessionState::InitInProgress, now_ns),
        );

        // The Java Publication exposes this metadata bit through Publication.isConnected().
        // It is set only after a real receiver setup has reached the driver, matching the
        // upstream NetworkPublication connection transition.
        let publication_registration_id = self
            .publications
            .values()
            .find(|p| {
                !p.is_ipc
                    && p.session_id == session_id
                    && p.stream_id == stream_id
                    && channel.is_none_or(|value| p.channel == value)
            })
            .map(|publication| publication.registration_id);
        if let Some(registration_id) = publication_registration_id {
            if let Some(buffers) = self.log_buffers.get(&registration_id) {
                aeron_client::logbuffer::set_is_connected(&buffers.meta_data_buffer(), true);
            }
        }

        let initial_term_id = setup.initial_term_id();
        let term_length = setup.term_length();
        let mtu_length = setup.mtu_length();
        let (log_file_name, buffers) = self.allocate_image_log_buffers(
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            mtu_length,
            channel,
        )?;

        let mut links = subscribers.into_iter();
        let Some(subscription_registration_id) = links.next() else {
            return Ok(());
        };
        let correlation_id = self.on_available_image(
            session_id,
            stream_id,
            subscription_registration_id,
            &log_file_name,
            UDP_SOURCE_IDENTITY,
        )?;
        let mut image = PublicationImage::new_with_active_position(
            session_id,
            stream_id,
            initial_term_id,
            setup.active_term_id(),
            setup.term_offset(),
            buffers,
            log_file_name.clone(),
            source,
            subscription_registration_id,
            correlation_id,
        );
        image.touch(now_ns);
        image.activate(now_ns);
        for subscription_registration_id in links {
            let correlation_id = self.on_available_image(
                session_id,
                stream_id,
                subscription_registration_id,
                &log_file_name,
                UDP_SOURCE_IDENTITY,
            )?;
            image.add_subscription_link(subscription_registration_id, correlation_id);
        }
        self.session_states
            .insert(image_key.clone(), (NetworkSessionState::Active, now_ns));
        self.setup_elicited.remove(&image_key);
        self.images.insert(image_key, image);
        Ok(())
    }

    /// A DATA frame (possibly several concatenated frames): rebuild each into its image.
    fn on_data(
        &mut self,
        buf: &AtomicBuffer,
        length: usize,
        source: SocketAddr,
        now_ns: i64,
        channel: Option<&str>,
        receiver_status: Option<ReceiverStatus>,
    ) {
        let session_id = buf.get_i32(DATA_SESSION_ID_FIELD_OFFSET);
        let stream_id = buf.get_i32(DATA_STREAM_ID_FIELD_OFFSET);
        let image_key = channel.map(|value| (value.to_owned(), session_id, stream_id, source));
        let image = match image_key {
            Some(key) => self.images.get_mut(&key),
            None => self
                .images
                .iter_mut()
                .find(|((_, image_session, image_stream, image_source), _)| {
                    *image_session == session_id
                        && *image_stream == stream_id
                        && *image_source == source
                })
                .map(|(_, image)| image),
        };
        let Some(image) = image else {
            // Mirrors DataPacketDispatcher.onDataPacket: only the first DATA frame for an
            // interested session moves it to PENDING_SETUP_FRAME. Unknown sessions are
            // remembered as NO_INTEREST, so repeated DATA frames stay discarded.
            if buf.get_u8(FLAGS_FIELD_OFFSET) & EOS_FLAG == 0 {
                if let Some(key) =
                    channel.map(|value| (value.to_owned(), session_id, stream_id, source))
                {
                    if !self.session_states.contains_key(&key) {
                        let state = if self.has_udp_interest(channel, stream_id, session_id) {
                            NetworkSessionState::PendingSetupFrame
                        } else {
                            NetworkSessionState::NoInterest
                        };
                        self.session_states.insert(key, (state, now_ns));
                    }
                }
            }
            return;
        };
        let mut offset = 0usize;
        while offset + DATA_HEADER_LENGTH <= length {
            let frame_length = buf.get_i32(offset + FRAME_LENGTH_FIELD_OFFSET);
            if frame_length == 0 {
                let term_id = buf.get_i32(offset + DATA_TERM_ID_FIELD_OFFSET);
                let term_offset = buf.get_i32(offset + DATA_TERM_OFFSET_FIELD_OFFSET);
                image.on_heartbeat(term_id, term_offset.max(0) as usize, now_ns);
                break;
            }
            if frame_length < 0 {
                break;
            }
            if frame_length < DATA_HEADER_LENGTH as i32 {
                break;
            }
            let aligned = align(frame_length, FRAME_ALIGNMENT) as usize;
            if aligned > length.saturating_sub(offset) {
                break;
            }
            let frame_session_id = buf.get_i32(offset + DATA_SESSION_ID_FIELD_OFFSET);
            let frame_stream_id = buf.get_i32(offset + DATA_STREAM_ID_FIELD_OFFSET);
            if frame_session_id != session_id || frame_stream_id != stream_id {
                break;
            }
            let term_id = buf.get_i32(offset + DATA_TERM_ID_FIELD_OFFSET);
            let term_offset = buf.get_i32(offset + DATA_TERM_OFFSET_FIELD_OFFSET);
            if term_offset < 0 {
                break;
            }

            let mut frame_bytes = vec![0u8; aligned];
            buf.get_bytes(offset, &mut frame_bytes);
            let frame = AtomicBuffer::new(&mut frame_bytes);
            image.on_data_frame(term_id, term_offset as usize, &frame, aligned, now_ns);

            offset += aligned;
        }
        if let Some(status) = receiver_status {
            // The status describes the contiguous position reached after this datagram. Apply it
            // after rebuilding so the datagram that advances the receiver is not mistaken for an
            // under-run relative to its own newly advertised position.
            image.update_flow_control(
                status.term_id,
                status.term_offset,
                status.receiver_window_length,
            );
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
        channel: Option<&str>,
    ) -> Result<(String, LogBuffers)> {
        let dir = self.aeron_dir.join("images");
        std::fs::create_dir_all(&dir)
            .map_err(|e| DriverError::IllegalState(format!("create images dir: {e}")))?;
        let channel_tag = channel.map(stable_channel_tag).unwrap_or_default();
        let path = dir.join(format!("{session_id}-{stream_id}-{channel_tag}.logbuffer"));

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

    fn has_udp_interest(&self, channel: Option<&str>, stream_id: i32, session_id: i32) -> bool {
        self.subscriptions.values().any(|subscription| {
            is_udp_channel(&subscription.channel)
                && subscription.stream_id == stream_id
                && subscription
                    .session_id
                    .is_none_or(|sub_session_id| sub_session_id == session_id)
                && channel.is_none_or(|value| subscription.channel == value)
        })
    }

    /// Claim the one-shot setup elicitation requested by `DataPacketDispatcher.onDataPacket`.
    pub fn take_setup_elicitation(
        &mut self,
        channel: &str,
        session_id: i32,
        stream_id: i32,
        source: SocketAddr,
        now_ns: i64,
    ) -> bool {
        let key = (channel.to_owned(), session_id, stream_id, source);
        if !self
            .session_states
            .get(&key)
            .is_some_and(|(state, _)| *state == NetworkSessionState::PendingSetupFrame)
        {
            return false;
        }

        let should_send = self
            .setup_elicited
            .get(&key)
            .is_none_or(|sent_at| now_ns.saturating_sub(*sent_at) >= PENDING_SETUPS_TIMEOUT_NS);
        if should_send {
            self.setup_elicited.insert(key, now_ns);
        }
        should_send
    }

    /// Adding interest clears the upstream NO_INTEREST marker for the matching stream.
    fn clear_no_interest_for_subscription(
        &mut self,
        channel: &str,
        stream_id: i32,
        subscription_session_id: Option<i32>,
    ) {
        for ((session_channel, session_id, session_stream, _), (state, _)) in
            &mut self.session_states
        {
            if *state == NetworkSessionState::NoInterest
                && *session_stream == stream_id
                && session_channel == channel
                && subscription_session_id.is_none_or(|id| id == *session_id)
            {
                *state = NetworkSessionState::PendingSetupFrame;
            }
        }
    }

    /// Connect a newly added IPC subscription to every matching existing IPC publication.
    fn link_ipc_subscription(
        &mut self,
        subscription_registration_id: i64,
        stream_id: i32,
    ) -> Result<()> {
        let matches: Vec<(i64, i32, i32, String)> = self
            .publications
            .values()
            .filter(|p| p.is_ipc && p.stream_id == stream_id)
            .filter_map(|p| {
                p.log_file_name.clone().map(|name| {
                    (
                        p.registration_id,
                        p.session_id,
                        p.publication_limit_counter_id,
                        name,
                    )
                })
            })
            .collect();
        for (
            publication_registration_id,
            session_id,
            publication_limit_counter_id,
            log_file_name,
        ) in matches
        {
            self.open_publication_window(
                publication_limit_counter_id,
                self.configuration.ipc_term_buffer_length,
            )?;
            if let Some(buffers) = self.log_buffers.get(&publication_registration_id) {
                aeron_client::logbuffer::set_is_connected(&buffers.meta_data_buffer(), true);
            }
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

    /// Link a newly registered UDP subscription to images already active for its stream.
    ///
    /// Aeron performs this link from the conductor; waiting for another SETUP packet would
    /// leave a late subscriber disconnected until the publisher's periodic announcement.
    fn link_network_subscription(
        &mut self,
        subscription_registration_id: i64,
        stream_id: i32,
    ) -> Result<()> {
        let Some((channel, subscription_session_id)) = self
            .subscriptions
            .get(&subscription_registration_id)
            .map(|subscription| (subscription.channel.clone(), subscription.session_id))
        else {
            return Ok(());
        };
        let images: Vec<(NetworkImageKey, String)> = self
            .images
            .iter()
            .filter(|((image_channel, _, _, _), image)| {
                (image_channel.is_empty() || image_channel == &channel)
                    && image.stream_id() == stream_id
                    && subscription_session_id.is_none_or(|id| image.session_id() == id)
                    && !image.has_subscription(subscription_registration_id)
            })
            .map(|(key, image)| (key.clone(), image.log_file_name().to_owned()))
            .collect();

        for (key, log_file_name) in images {
            let (_, session_id, _, _) = &key;
            let correlation_id = self.on_available_image(
                *session_id,
                stream_id,
                subscription_registration_id,
                &log_file_name,
                UDP_SOURCE_IDENTITY,
            )?;
            if let Some(image) = self.images.get_mut(&key) {
                image.add_subscription_link(subscription_registration_id, correlation_id);
            }
        }
        Ok(())
    }

    /// Connect a newly added IPC publication to every matching existing IPC subscription.
    fn link_ipc_publication(
        &mut self,
        publication_registration_id: i64,
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
            )?;
            if let Some(buffers) = self.log_buffers.get(&publication_registration_id) {
                aeron_client::logbuffer::set_is_connected(&buffers.meta_data_buffer(), true);
            }
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
    fn open_publication_window(
        &self,
        publication_limit_counter_id: i32,
        term_length: usize,
    ) -> Result<()> {
        let value = (term_length / 2) as i64;
        if let Some(limit) = self.counters.get(&publication_limit_counter_id) {
            limit.store(value, Ordering::Release);
        }
        if self.cnc.counters_values_length() > 0 {
            self.cnc
                .set_counter_value(publication_limit_counter_id, value)?;
        }
        Ok(())
    }

    fn publish_counter(
        &self,
        counter_id: i32,
        type_id: i32,
        value: i64,
        label: &str,
    ) -> Result<()> {
        self.publish_counter_with_registration(counter_id, type_id, counter_id as i64, value, label)
    }

    fn publish_counter_with_registration(
        &self,
        counter_id: i32,
        type_id: i32,
        registration_id: i64,
        value: i64,
        label: &str,
    ) -> Result<()> {
        self.publish_counter_with_registration_and_key(
            counter_id,
            type_id,
            registration_id,
            value,
            &[],
            label,
        )
    }

    fn publish_counter_with_registration_and_key(
        &self,
        counter_id: i32,
        type_id: i32,
        registration_id: i64,
        value: i64,
        key: &[u8],
        label: &str,
    ) -> Result<()> {
        if self.cnc.counters_metadata_length()
            >= aeron_client::cnc_file::COUNTER_METADATA_LENGTH * (counter_id as usize + 1)
        {
            self.cnc.write_counter_metadata_with_key(
                counter_id,
                type_id,
                registration_id,
                value,
                key,
                label,
            )?;
        }
        Ok(())
    }

    /// Publication limit counter for the given id, shared with an in-process client.
    pub fn counters(&self) -> &HashMap<i32, Arc<AtomicI64>> {
        &self.counters
    }

    fn touch_client(&mut self, client_id: i64, now_ns: i64) {
        self.clients.insert(client_id, now_ns);
    }

    fn remove_client(&mut self, client_id: i64, now_ns: i64) -> Result<()> {
        self.clients.remove(&client_id);
        let removed: Vec<i64> = self
            .publications
            .values()
            .filter(|p| p.client_id == client_id)
            .map(|p| p.registration_id)
            .collect();
        for registration_id in removed {
            if let Some(entry) = self.publications.remove(&registration_id) {
                if !entry.is_ipc {
                    self.removed_network_publications.push(registration_id);
                }
                self.counters.remove(&entry.publication_limit_counter_id);
                self.cnc.free_counter(entry.publication_limit_counter_id)?;
            }
            self.log_buffers.remove(&registration_id);
        }
        let removed_subscriptions: Vec<(i64, String)> = self
            .subscriptions
            .iter()
            .filter(|(_, subscription)| subscription.client_id == client_id)
            .map(|(registration_id, subscription)| (*registration_id, subscription.channel.clone()))
            .collect();
        for (registration_id, channel) in removed_subscriptions {
            self.detach_images_for_subscription(registration_id, now_ns)?;
            if let Some(endpoint_key) = self.release_receive_endpoint(&channel) {
                self.removed_receive_channels.push(endpoint_key);
            }
            self.subscriptions.remove(&registration_id);
        }
        let removed_counters: Vec<(i64, i32)> = self
            .client_counters
            .iter()
            .filter(|(_, (owner, _))| *owner == client_id)
            .map(|(&registration_id, &(_, counter_id))| (registration_id, counter_id))
            .collect();
        for (registration_id, counter_id) in removed_counters {
            self.client_counters.remove(&registration_id);
            self.counters.remove(&counter_id);
            self.cnc.free_counter(counter_id)?;
            self.on_counter_unavailable(registration_id, counter_id)?;
        }
        Ok(())
    }

    /// Detach all images linked to a removed subscription before its endpoint is removed.
    ///
    /// A detached image must not continue rebuilding data into a client-owned log. Dropping the
    /// mapped buffers also releases the file so a later subscription can create a fresh image.
    fn detach_images_for_subscription(
        &mut self,
        subscription_registration_id: i64,
        now_ns: i64,
    ) -> Result<()> {
        let keys: Vec<NetworkImageKey> = self
            .images
            .iter()
            .filter(|(_, image)| image.has_subscription(subscription_registration_id))
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys {
            let remove_image = if let Some(image) = self.images.get_mut(&key) {
                image.remove_subscription_link(subscription_registration_id);
                image.has_no_subscriptions()
            } else {
                false
            };
            if remove_image {
                if let Some(mut image) = self.images.remove(&key) {
                    image.deactivate(now_ns);
                    self.session_states
                        .insert(key.clone(), (NetworkSessionState::OnCoolDown, now_ns));
                    self.setup_elicited.remove(&key);
                    let log_file_name = image.log_file_name().to_owned();
                    drop(image);
                    let _ = std::fs::remove_file(log_file_name);
                }
            }
        }
        self.release_subscriber_counters(subscription_registration_id)?;
        Ok(())
    }

    fn release_subscriber_counters(&mut self, subscription_registration_id: i64) -> Result<()> {
        let keys: Vec<_> = self
            .subscriber_counters
            .keys()
            .filter(|(subscription_id, _)| *subscription_id == subscription_registration_id)
            .copied()
            .collect();
        for key in keys {
            if let Some(counter_id) = self.subscriber_counters.remove(&key) {
                self.counters.remove(&counter_id);
                self.cnc.free_counter(counter_id)?;
            }
        }
        Ok(())
    }

    fn release_image_counter(
        &mut self,
        subscription_registration_id: i64,
        correlation_id: i64,
    ) -> Result<()> {
        if let Some(counter_id) = self
            .subscriber_counters
            .remove(&(subscription_registration_id, correlation_id))
        {
            self.counters.remove(&counter_id);
            self.cnc.free_counter(counter_id)?;
        }
        Ok(())
    }

    fn sweep_timed_out_clients(&mut self, now_ns: i64) -> Result<usize> {
        let timeout = self.client_liveness_timeout_ns;
        if timeout <= 0 {
            return Ok(0);
        }
        let expired: Vec<i64> = self
            .clients
            .iter()
            .filter(|&(_, &last)| now_ns - last > timeout)
            .map(|(&id, _)| id)
            .collect();
        for client_id in &expired {
            self.on_client_timeout(*client_id)?;
            self.remove_client(*client_id, now_ns)?;
        }
        Ok(expired.len())
    }

    /// Drive every network image through its end-of-life state machine once per duty cycle.
    ///
    /// Mirrors the split upstream keeps between the `Receiver` (drops an image that has lost
    /// liveness: `deactivate` + `receiverRelease`) and `DriverConductor.checkManagedResources`
    /// (runs `onTimeEvent`, notifies subscribers on the DRAINING->LINGER edge, frees the image once
    /// it reaches end of life). Both roles live in this single-threaded conductor sweep.
    fn sweep_inactive_images(&mut self, now_ns: i64) -> Result<usize> {
        let keys: Vec<NetworkImageKey> = self.images.keys().cloned().collect();

        let mut work = 0;
        for key in keys {
            let Some(image) = self.images.get_mut(&key) else {
                continue;
            };

            // Receiver role: a silent image is deactivated and released so the state machine can run.
            if !image.is_connected(now_ns, IMAGE_LIVENESS_TIMEOUT_NS)
                && !image.has_reached_end_of_life()
            {
                image.deactivate(now_ns);
                image.receiver_release();
                self.session_states
                    .insert(key.clone(), (NetworkSessionState::OnCoolDown, now_ns));
                self.setup_elicited.remove(&key);
            }

            // Conductor role: advance DRAINING->LINGER->DONE and react to each transition.
            match image.on_time_event(now_ns, STATUS_MESSAGE_TIMEOUT_NS, IMAGE_LIVENESS_TIMEOUT_NS)
            {
                ImageLifecycleEvent::None => {}
                ImageLifecycleEvent::EnteredLinger => {
                    let links: Vec<_> = image.image_links().collect();
                    for (subscription_registration_id, correlation_id) in links {
                        self.on_unavailable_image(correlation_id, subscription_registration_id)?;
                    }
                    work += 1;
                }
                ImageLifecycleEvent::ReachedEndOfLife => {
                    if let Some(image) = self.images.remove(&key) {
                        let links: Vec<_> = image.image_links().collect();
                        let log_file_name = image.log_file_name().to_owned();
                        drop(image);
                        let _ = std::fs::remove_file(log_file_name);
                        for (subscription_registration_id, correlation_id) in links {
                            self.release_image_counter(
                                subscription_registration_id,
                                correlation_id,
                            )?;
                        }
                    }
                    work += 1;
                }
            }
        }
        Ok(work)
    }

    /// Collect NAKs from every image whose rebuild has an outstanding, retry-due gap.
    ///
    /// Called after this cycle's DATA frames have been rebuilt into the images; the driver routes
    /// each returned NAK to the receive endpoint that owns the channel.
    pub fn poll_image_loss(&mut self, now_ns: i64) -> Vec<PendingNak> {
        let mut naks = Vec::new();
        for ((channel, session_id, stream_id, source), image) in &mut self.images {
            if let Some(gap) = image.track_rebuild(now_ns) {
                naks.push(PendingNak {
                    channel: channel.clone(),
                    session_id: *session_id,
                    stream_id: *stream_id,
                    source: *source,
                    term_id: gap.term_id,
                    term_offset: gap.term_offset,
                    length: gap.length,
                });
            }
        }
        naks
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
    ) -> Result<i64> {
        let correlation_id = self.next_id();
        let subscriber_position_id = self.next_counter_id();
        // The subscriber owns its position counter (starts at 0).
        self.counters
            .insert(subscriber_position_id, Arc::new(AtomicI64::new(0)));
        self.publish_counter(
            subscriber_position_id,
            DRIVER_SUBSCRIBER_POSITION_TYPE_ID,
            0,
            "Subscriber position",
        )?;
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
        self.transmit(ON_AVAILABLE_IMAGE, &buf, len)?;
        self.subscriber_counters.insert(
            (subscription_registration_id, correlation_id),
            subscriber_position_id,
        );
        Ok(correlation_id)
    }

    fn on_unavailable_image(
        &self,
        correlation_id: i64,
        subscription_registration_id: i64,
    ) -> Result<()> {
        let mut scratch = [0u8; 16];
        let buf = AtomicBuffer::new(&mut scratch);
        buf.put_i64(0, correlation_id);
        buf.put_i64(8, subscription_registration_id);
        self.transmit(ON_UNAVAILABLE_IMAGE, &buf, 16)
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

    fn on_counter_ready(&self, correlation_id: i64, counter_id: i32) -> Result<()> {
        self.transmit_counter_update(ON_COUNTER_READY, correlation_id, counter_id)
    }

    fn on_counter_unavailable(&self, correlation_id: i64, counter_id: i32) -> Result<()> {
        self.transmit_counter_update(ON_UNAVAILABLE_COUNTER, correlation_id, counter_id)
    }

    fn on_next_available_session_id(&self, correlation_id: i64, session_id: i32) -> Result<()> {
        let mut scratch = [0u8; 12];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        buf.put_i64(0, correlation_id);
        buf.put_i32(8, session_id);
        self.transmit(ON_NEXT_AVAILABLE_SESSION_ID, &buf, len)
    }

    fn transmit_counter_update(
        &self,
        message_type: i32,
        correlation_id: i64,
        counter_id: i32,
    ) -> Result<()> {
        let mut scratch = [0u8; 12];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        buf.put_i64(0, correlation_id);
        buf.put_i32(8, counter_id);
        self.transmit(message_type, &buf, len)
    }

    fn on_client_timeout(&self, client_id: i64) -> Result<()> {
        let mut scratch = [0u8; 8];
        let len = scratch.len();
        let buf = AtomicBuffer::new(&mut scratch);
        let flyweight = ClientTimeoutFlyweight::wrap(buf, 0);
        flyweight.set_client_id(client_id);
        self.transmit(ON_CLIENT_TIMEOUT, &buf, len)
    }

    fn on_error(&self, correlation_id: i64, code: i32, message: String) -> Result<()> {
        let length = 12 + 4 + message.len();
        let mut bytes = vec![0u8; length];
        let buf = AtomicBuffer::new(&mut bytes);
        let error = ErrorResponseFlyweight::wrap(buf, 0);
        error.set_offending_command_correlation_id(correlation_id);
        error.set_error_code(code);
        error.set_error_message(&message);
        self.transmit(ON_ERROR, &buf, length)
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

    pub fn termination_requested(&self) -> bool {
        self.termination_requested
    }
}

fn decode_command(
    msg_type: i32,
    buffer: &AtomicBuffer,
    offset: usize,
    length: usize,
) -> Option<Command> {
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
        ADD_COUNTER if length >= aeron_client::command::KEY_BUFFER_OFFSET => {
            let fw = CounterMessageFlyweight::new(*buffer, offset);
            let key_length = fw.key_buffer_length();
            let label_offset = fw.label_buffer_offset();
            if label_offset + 4 > length {
                return None;
            }
            let label_length = fw.label_buffer_length();
            if aeron_client::command::KEY_BUFFER_OFFSET + key_length + 4 + label_length > length {
                return None;
            }
            let mut key = vec![0u8; key_length];
            buffer.get_bytes(offset + fw.key_buffer_offset(), &mut key);
            let mut label_bytes = vec![0u8; label_length];
            buffer.get_bytes(offset + label_offset + 4, &mut label_bytes);
            Some(Command::AddCounter {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                type_id: fw.type_id(),
                key,
                label: String::from_utf8_lossy(&label_bytes).into_owned(),
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
        REMOVE_COUNTER => {
            let fw = RemoveMessageFlyweight::new(*buffer, offset);
            Some(Command::RemoveCounter {
                correlation_id: fw.correlation_id(),
                registration_id: fw.registration_id(),
            })
        }
        GET_NEXT_AVAILABLE_SESSION_ID
            if length >= aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH + 4 =>
        {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(*buffer, offset);
            Some(Command::GetNextAvailableSessionId {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                stream_id: buffer
                    .get_i32(offset + aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH),
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
        TERMINATE_DRIVER => {
            let fw = TerminateDriverFlyweight::new(*buffer, offset);
            Some(Command::TerminateDriver {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
            })
        }
        _ if length >= aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH => {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(*buffer, offset);
            Some(Command::Unknown {
                correlation_id: fw.correlation_id(),
                message_type: msg_type,
            })
        }
        _ => None,
    }
}

fn error_code(error: &DriverError) -> i32 {
    match error {
        DriverError::InvalidChannel(_) => 1,
        DriverError::IllegalState(message) | DriverError::IllegalArgument(message)
            if message.contains("No space left on device")
                || message.contains("There is not enough space on the disk") =>
        {
            12
        }
        DriverError::IllegalState(_) | DriverError::IllegalArgument(_) => 11,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aeron_client::cnc_file::{
        DEFAULT_COUNTER_METADATA_LENGTH, DEFAULT_COUNTER_VALUES_LENGTH, DEFAULT_ERROR_LOG_LENGTH,
        DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH,
    };
    use aeron_client::driver_events::{
        CounterUpdateFlyweight, ErrorResponseFlyweight, NextAvailableSessionIdFlyweight,
        ON_CLIENT_TIMEOUT, ON_COUNTER_READY, ON_ERROR, ON_NEXT_AVAILABLE_SESSION_ID,
        ON_SUBSCRIPTION_READY, ON_UNAVAILABLE_COUNTER,
    };
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
        let cnc = CncFile::create_new_with_regions(
            &path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            DEFAULT_COUNTER_METADATA_LENGTH,
            DEFAULT_COUNTER_VALUES_LENGTH,
            DEFAULT_ERROR_LOG_LENGTH,
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
    fn honors_explicit_publication_session_id_through_the_cnc() {
        let (cnc, path) = temp_cnc("publication-session");
        let dir = temp_dir("publication-session");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        client
            .add_publication("aeron:ipc?session-id=4096", 7)
            .unwrap();

        conductor.do_work(1_000).unwrap();

        assert_eq!(conductor.publications().len(), 1);
        assert_eq!(
            conductor.publications().values().next().unwrap().session_id,
            4096
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn applies_publication_term_and_mtu_parameters_at_registration() {
        let (cnc, path) = temp_cnc("publication-params");
        let dir = temp_dir("publication-params");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        client
            .add_publication("aeron:ipc?term-length=64k|mtu=4k", 7)
            .unwrap();

        conductor.do_work(1_000).unwrap();

        let entry = conductor.publications().values().next().unwrap();
        let buffers = LogBuffers::map_existing(entry.log_file_name.as_ref().unwrap()).unwrap();
        assert_eq!(buffers.term_length(), 64 * 1024);
        assert_eq!(
            aeron_client::logbuffer::mtu_length(&buffers.meta_data_buffer()),
            4 * 1024
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_a_publication_with_a_conflicting_explicit_session_id() {
        let (cnc, path) = temp_cnc("publication-session-conflict");
        let dir = temp_dir("publication-session-conflict");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        client.add_publication("aeron:ipc", 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let correlation_id = client
            .add_publication("aeron:ipc?session-id=4096", 7)
            .unwrap();

        conductor.do_work(2_000).unwrap();

        assert_eq!(conductor.publications().len(), 1);
        let mut error_correlation = None;
        while events
            .receive_next(|msg_type, buf, offset, _| {
                if msg_type == ON_ERROR {
                    error_correlation = Some(
                        ErrorResponseFlyweight::wrap(*buf, offset)
                            .offending_command_correlation_id(),
                    );
                }
            })
            .unwrap()
        {
            if error_correlation.is_some() {
                break;
            }
        }
        assert_eq!(error_correlation, Some(correlation_id));

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
    fn adds_and_removes_client_counters_through_the_cnc() {
        let (cnc, path) = temp_cnc("client-counter");
        let dir = temp_dir("client-counter");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        let registration_id = client.add_counter(77, &[1, 2, 3], "test counter").unwrap();

        assert_eq!(conductor.do_work(1_000).unwrap(), 1);
        let mut ready_correlation = 0;
        let mut counter_id = -1;
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    assert_eq!(msg_type, ON_COUNTER_READY);
                    let update = CounterUpdateFlyweight::wrap(*buf, offset);
                    ready_correlation = update.correlation_id();
                    counter_id = update.counter_id();
                })
                .unwrap()
        );
        assert_eq!(ready_correlation, registration_id);
        assert!(conductor.counters().contains_key(&counter_id));
        let metadata = conductor.cnc().counters_metadata_buffer();
        let metadata_offset = counter_id as usize * aeron_client::cnc_file::COUNTER_METADATA_LENGTH;
        assert_eq!(
            metadata.get_i32(metadata_offset),
            aeron_client::cnc_file::COUNTER_RECORD_ALLOCATED
        );
        assert_eq!(
            metadata.get_i32(metadata_offset + aeron_client::cnc_file::COUNTER_TYPE_ID_OFFSET),
            77
        );

        let remove_correlation = client.remove_counter(registration_id).unwrap();
        assert_eq!(conductor.do_work(2_000).unwrap(), 1);
        let mut unavailable_correlation = 0;
        let mut unavailable_id = -1;
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    assert_eq!(msg_type, ON_UNAVAILABLE_COUNTER);
                    let update = CounterUpdateFlyweight::wrap(*buf, offset);
                    unavailable_correlation = update.correlation_id();
                    unavailable_id = update.counter_id();
                })
                .unwrap()
        );
        assert_eq!(unavailable_correlation, registration_id);
        assert_eq!(unavailable_id, counter_id);
        assert_ne!(remove_correlation, unavailable_correlation);
        assert!(!conductor.counters().contains_key(&counter_id));
        assert_eq!(metadata.get_i32(metadata_offset), 0);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn returns_next_available_session_ids_through_the_cnc() {
        let (cnc, path) = temp_cnc("next-session-id");
        let dir = temp_dir("next-session-id");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);

        let first_correlation = client.get_next_available_session_id(7).unwrap();
        let second_correlation = client.get_next_available_session_id(8).unwrap();
        assert_eq!(conductor.do_work(1_000).unwrap(), 2);

        let mut responses = Vec::new();
        while events
            .receive_next(|msg_type, buf, offset, _len| {
                assert_eq!(msg_type, ON_NEXT_AVAILABLE_SESSION_ID);
                let response = NextAvailableSessionIdFlyweight::wrap(*buf, offset);
                responses.push((response.correlation_id(), response.next_session_id()));
            })
            .unwrap()
        {}
        assert_eq!(
            responses,
            vec![(first_correlation, 1), (second_correlation, 2)]
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    fn receive_event_type(events: &mut BroadcastReceiver<'_>) -> i32 {
        let mut seen_type = 0;
        assert!(
            events
                .receive_next(|msg_type, _, _, _| seen_type = msg_type)
                .unwrap()
        );
        seen_type
    }

    #[test]
    fn rejects_conflicting_reliable_and_unreliable_wildcard_subscriptions() {
        let (cnc, path) = temp_cnc("reliability-conflict");
        let dir = temp_dir("reliability-conflict");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0";

        client.add_subscription(channel, 7).unwrap();
        assert_eq!(conductor.do_work(1_000).unwrap(), 1);
        assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);
        client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0|reliable=false", 7)
            .unwrap();
        assert_eq!(conductor.do_work(2_000).unwrap(), 2);
        assert_eq!(conductor.subscriptions().len(), 1);
        assert_eq!(receive_event_type(&mut events), ON_ERROR);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_conflicting_reliability_for_the_same_session_specific_subscription() {
        let (cnc, path) = temp_cnc("session-reliability-conflict");
        let dir = temp_dir("session-reliability-conflict");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);

        client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0|session-id=1024", 7)
            .unwrap();
        assert_eq!(conductor.do_work(1_000).unwrap(), 1);
        assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);

        client
            .add_subscription(
                "aeron:udp?endpoint=127.0.0.1:0|session-id=1024|reliable=false",
                7,
            )
            .unwrap();
        assert_eq!(conductor.do_work(2_000).unwrap(), 2);
        assert_eq!(conductor.subscriptions().len(), 1);
        assert_eq!(receive_event_type(&mut events), ON_ERROR);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn allows_conflicting_reliability_for_different_session_specific_subscriptions() {
        let (cnc, path) = temp_cnc("session-reliability");
        let dir = temp_dir("session-reliability");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);

        client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0|session-id=1024", 7)
            .unwrap();
        client
            .add_subscription(
                "aeron:udp?endpoint=127.0.0.1:0|session-id=1025|reliable=false",
                7,
            )
            .unwrap();
        assert_eq!(conductor.do_work(1_000).unwrap(), 2);
        assert_eq!(conductor.subscriptions().len(), 2);
        assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);
        assert_eq!(receive_event_type(&mut events), ON_SUBSCRIPTION_READY);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn allows_wildcard_and_different_session_reliability_subscriptions() {
        let (cnc, path) = temp_cnc("wildcard-session-reliability");
        let dir = temp_dir("wildcard-session-reliability");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);

        client
            .add_subscription(
                "aeron:udp?endpoint=127.0.0.1:0|session-id=1024|reliable=false",
                7,
            )
            .unwrap();
        client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 7)
            .unwrap();
        client
            .add_subscription(
                "aeron:udp?endpoint=127.0.0.1:0|session-id=1025|reliable=false",
                7,
            )
            .unwrap();
        assert_eq!(conductor.do_work(1_000).unwrap(), 3);
        assert_eq!(conductor.subscriptions().len(), 3);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reports_unknown_client_commands_to_the_originating_correlation() {
        let (cnc, path) = temp_cnc("unknown-command");
        let dir = temp_dir("unknown-command");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();

        let mut command_bytes = [0u8; aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH];
        let command = aeron_client::command::CorrelatedMessageFlyweight::new(
            AtomicBuffer::new(&mut command_bytes),
            0,
        );
        command.set_client_id(CLIENT_ID);
        command.set_correlation_id(77);
        let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
        let command_length = command_bytes.len();
        ring.write(
            932_467_234,
            &AtomicBuffer::new(&mut command_bytes),
            0,
            command_length,
        )
        .unwrap();

        conductor.do_work(1_000).unwrap();

        let mut seen_type = 0;
        let mut seen_correlation = 0;
        let mut seen_code = 0;
        let mut seen_message = String::new();
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    seen_type = msg_type;
                    let error = ErrorResponseFlyweight::wrap(*buf, offset);
                    seen_correlation = error.offending_command_correlation_id();
                    seen_code = error.error_code();
                    seen_message = error.error_message().unwrap();
                })
                .unwrap()
        );
        assert_eq!(seen_type, ON_ERROR);
        assert_eq!(seen_correlation, 77);
        assert_eq!(seen_code, 6);
        assert_eq!(seen_message, "ERROR - command typeId=932467234");

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reports_invalid_channel_and_continues_processing_commands() {
        let (cnc, path) = temp_cnc("invalid-channel");
        let dir = temp_dir("invalid-channel");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);

        let invalid_correlation = client.add_publication("aeron:unknown", 7).unwrap();
        assert_eq!(conductor.do_work(1_000).unwrap(), 2);
        assert!(conductor.publications().is_empty());

        let mut seen_type = 0;
        let mut seen_correlation = 0;
        let mut seen_code = 0;
        let mut seen_message = String::new();
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    seen_type = msg_type;
                    let error = ErrorResponseFlyweight::wrap(*buf, offset);
                    seen_correlation = error.offending_command_correlation_id();
                    seen_code = error.error_code();
                    seen_message = error.error_message().unwrap();
                })
                .unwrap()
        );
        assert_eq!(seen_type, ON_ERROR);
        assert_eq!(seen_correlation, invalid_correlation);
        assert_eq!(seen_code, 1);
        assert!(seen_message.contains("unsupported media"));

        let valid_correlation = client.add_subscription("aeron:ipc", 7).unwrap();
        assert_eq!(conductor.do_work(2_000).unwrap(), 1);
        assert_eq!(conductor.subscriptions().len(), 1);
        let mut ready_correlation = 0;
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    assert_eq!(msg_type, ON_SUBSCRIPTION_READY);
                    ready_correlation =
                        SubscriptionReadyFlyweight::wrap(*buf, offset).correlation_id();
                })
                .unwrap()
        );
        assert_eq!(ready_correlation, valid_correlation);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn classifies_command_failures_like_aeron_error_codes() {
        assert_eq!(error_code(&DriverError::InvalidChannel("bad".into())), 1);
        assert_eq!(
            error_code(&DriverError::IllegalState("No space left on device".into())),
            12
        );
        assert_eq!(
            error_code(&DriverError::IllegalArgument(
                "There is not enough space on the disk".into()
            )),
            12
        );
        assert_eq!(error_code(&DriverError::IllegalState("other".into())), 11);
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
    fn removing_udp_subscription_detaches_image_and_receive_channel() {
        let (cnc, path) = temp_cnc("remove-udp-subscription");
        let dir = temp_dir("remove-udp-subscription");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0";
        let registration_id = client.add_subscription(channel, 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let mut endpoints = conductor.take_new_receive_endpoints();
        assert_eq!(endpoints.len(), 1);
        let endpoint_key = endpoints[0].canonical_form().to_owned();
        endpoints.pop();

        let source = SocketAddr::from(([127, 0, 0, 1], 9003));
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(7)
            .set_stream_id(7)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);
        conductor
            .on_received_frame(&setup_bytes, source, 2_000)
            .unwrap();
        assert_eq!(conductor.images.len(), 1);

        client.remove_subscription(registration_id).unwrap();
        conductor.do_work(3_000).unwrap();

        assert!(conductor.images.is_empty());
        assert!(conductor.counters().is_empty());
        assert_eq!(
            conductor.take_removed_receive_channels(),
            vec![endpoint_key]
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn releases_ipc_subscriber_position_counter_when_subscription_is_removed() {
        let (cnc, path) = temp_cnc("ipc-counter-release");
        let dir = temp_dir("ipc-counter-release");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let publication = client.add_publication("aeron:ipc", 7).unwrap();
        let subscription = client.add_subscription("aeron:ipc", 7).unwrap();
        conductor.do_work(1_000).unwrap();
        assert_eq!(conductor.counters().len(), 2);

        client.remove_subscription(subscription).unwrap();
        conductor.do_work(2_000).unwrap();
        assert_eq!(conductor.counters().len(), 1);

        client.remove_publication(publication, false).unwrap();
        conductor.do_work(3_000).unwrap();
        assert!(conductor.counters().is_empty());

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn shares_udp_receive_endpoint_until_last_subscription_is_removed() {
        let (cnc, path) = temp_cnc("shared-udp-endpoint");
        let dir = temp_dir("shared-udp-endpoint");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0";

        let first = client.add_subscription(channel, 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let endpoints = conductor.take_new_receive_endpoints();
        assert_eq!(endpoints.len(), 1);
        let endpoint_key = endpoints[0].canonical_form().to_owned();

        let second = client.add_subscription(channel, 8).unwrap();
        conductor.do_work(2_000).unwrap();
        assert!(conductor.take_new_receive_endpoints().is_empty());

        client.remove_subscription(first).unwrap();
        conductor.do_work(3_000).unwrap();
        assert!(conductor.take_removed_receive_channels().is_empty());

        client.remove_subscription(second).unwrap();
        conductor.do_work(4_000).unwrap();
        assert_eq!(
            conductor.take_removed_receive_channels(),
            vec![endpoint_key]
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn shares_udp_receive_endpoint_across_session_and_reliability_uri_options() {
        let (cnc, path) = temp_cnc("shared-udp-options");
        let dir = temp_dir("shared-udp-options");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);

        let first = client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0|session-id=1024", 7)
            .unwrap();
        conductor.do_work(1_000).unwrap();
        let endpoints = conductor.take_new_receive_endpoints();
        assert_eq!(endpoints.len(), 1);
        let endpoint_key = endpoints[0].canonical_form().to_owned();

        let second = client
            .add_subscription(
                "aeron:udp?endpoint=127.0.0.1:0|session-id=1025|reliable=false",
                7,
            )
            .unwrap();
        conductor.do_work(2_000).unwrap();
        assert!(conductor.take_new_receive_endpoints().is_empty());

        client.remove_subscription(first).unwrap();
        conductor.do_work(3_000).unwrap();
        assert!(conductor.take_removed_receive_channels().is_empty());

        client.remove_subscription(second).unwrap();
        conductor.do_work(4_000).unwrap();
        assert_eq!(
            conductor.take_removed_receive_channels(),
            vec![endpoint_key]
        );

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn routes_identical_streams_by_receive_channel_identity() {
        let (cnc, path) = temp_cnc("channel-isolation");
        let dir = temp_dir("channel-isolation");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let first_channel = "aeron:udp?endpoint=127.0.0.1:0";
        let second_channel = "aeron:udp?endpoint=localhost:0";
        let first = client.add_subscription(first_channel, 7).unwrap();
        let second = client.add_subscription(second_channel, 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();

        let source = SocketAddr::from(([127, 0, 0, 1], 9010));
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(7)
            .set_stream_id(7)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);

        conductor
            .on_received_frame_for_channel(&setup_bytes, source, 2_000, Some(first_channel))
            .unwrap();
        let image = conductor.images.values().next().unwrap();
        assert_eq!(image.image_links().count(), 1);
        assert!(image.has_subscription(first));
        assert!(!image.has_subscription(second));

        drop(conductor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn tracks_data_before_setup_and_only_creates_image_after_setup() {
        let (cnc, path) = temp_cnc("dispatcher-session-state");
        let dir = temp_dir("dispatcher-session-state");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0";
        client.add_subscription(channel, 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();

        let source = SocketAddr::from(([127, 0, 0, 1], 9011));
        let mut data_bytes = vec![0u8; DATA_HEADER_LENGTH];
        let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut data_bytes));
        data.set_frame_length(DATA_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_DATA)
            .set_flags(0)
            .set_session_id(7)
            .set_stream_id(7)
            .set_term_id(0)
            .set_term_offset(0);

        conductor
            .on_received_frame_for_channel(&data_bytes, source, 2_000, Some(channel))
            .unwrap();
        assert!(conductor.images.is_empty());
        assert_eq!(
            conductor
                .session_states
                .get(&(channel.to_owned(), 7, 7, source)),
            Some(&(NetworkSessionState::PendingSetupFrame, 2_000))
        );
        assert!(conductor.take_setup_elicitation(channel, 7, 7, source, 2_000));
        assert!(!conductor.take_setup_elicitation(channel, 7, 7, source, 2_000 + 999_999_999,));
        assert!(conductor.take_setup_elicitation(channel, 7, 7, source, 2_000 + 1_000_000_000,));

        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(7)
            .set_stream_id(7)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);
        conductor
            .on_received_frame_for_channel(&setup_bytes, source, 3_000, Some(channel))
            .unwrap();

        assert_eq!(conductor.images.len(), 1);
        assert_eq!(
            conductor
                .session_states
                .get(&(channel.to_owned(), 7, 7, source)),
            Some(&(NetworkSessionState::Active, 3_000))
        );

        drop(conductor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn records_no_interest_until_a_matching_subscription_clears_it() {
        let (cnc, path) = temp_cnc("dispatcher-no-interest");
        let dir = temp_dir("dispatcher-no-interest");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let channel = "aeron:udp?endpoint=127.0.0.1:0";
        let source = SocketAddr::from(([127, 0, 0, 1], 9012));

        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(8)
            .set_stream_id(8)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);

        conductor
            .on_received_frame_for_channel(&setup_bytes, source, 1_000, Some(channel))
            .unwrap();
        let key = (channel.to_owned(), 8, 8, source);
        assert!(conductor.images.is_empty());
        assert_eq!(
            conductor.session_states.get(&key),
            Some(&(NetworkSessionState::NoInterest, 1_000))
        );

        let client = proxy(&cnc);
        client.add_subscription(channel, 8).unwrap();
        conductor.do_work(2_000).unwrap();
        conductor
            .on_received_frame_for_channel(&setup_bytes, source, 3_000, Some(channel))
            .unwrap();
        assert_eq!(conductor.images.len(), 1);
        assert_eq!(
            conductor.session_states.get(&key),
            Some(&(NetworkSessionState::Active, 3_000))
        );

        drop(conductor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn applies_session_specific_subscription_interest() {
        let (cnc, path) = temp_cnc("dispatcher-session-filter");
        let dir = temp_dir("dispatcher-session-filter");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0|session-id=7";
        client.add_subscription(channel, 9).unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();
        let source = SocketAddr::from(([127, 0, 0, 1], 9013));

        for session_id in [8, 7] {
            let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
            let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
            setup
                .header()
                .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
                .set_version(CURRENT_VERSION)
                .set_header_type(HDR_TYPE_SETUP);
            setup
                .set_session_id(session_id)
                .set_stream_id(9)
                .set_initial_term_id(0)
                .set_active_term_id(0)
                .set_term_offset(0)
                .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
                .set_mtu_length(1408)
                .set_ttl(0);
            conductor
                .on_received_frame_for_channel(
                    &setup_bytes,
                    source,
                    2_000 + session_id as i64,
                    Some(channel),
                )
                .unwrap();
        }

        assert_eq!(conductor.images.len(), 1);
        assert!(
            conductor
                .images
                .keys()
                .any(|(_, session_id, _, _)| *session_id == 7)
        );
        assert!(
            conductor
                .session_states
                .get(&(channel.to_owned(), 8, 9, source))
                .is_some_and(|(state, _)| *state == NetworkSessionState::NoInterest)
        );

        drop(conductor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn enforces_stream_session_limit() {
        let (cnc, path) = temp_cnc("dispatcher-session-limit");
        let dir = temp_dir("dispatcher-session-limit");
        let mut config = small_config();
        config.stream_session_limit = 1;
        let mut conductor = DriverConductor::new(config, cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0";
        client.add_subscription(channel, 10).unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();
        let source = SocketAddr::from(([127, 0, 0, 1], 9014));

        for session_id in [10, 11] {
            let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
            let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
            setup
                .header()
                .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
                .set_version(CURRENT_VERSION)
                .set_header_type(HDR_TYPE_SETUP);
            setup
                .set_session_id(session_id)
                .set_stream_id(10)
                .set_initial_term_id(0)
                .set_active_term_id(0)
                .set_term_offset(0)
                .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
                .set_mtu_length(1408)
                .set_ttl(0);
            conductor
                .on_received_frame_for_channel(
                    &setup_bytes,
                    source,
                    2_000 + session_id as i64,
                    Some(channel),
                )
                .unwrap();
        }

        assert_eq!(conductor.images.len(), 1);
        assert!(
            conductor
                .session_states
                .get(&(channel.to_owned(), 11, 10, source))
                .is_some_and(|(state, _)| *state == NetworkSessionState::NoInterest)
        );

        drop(conductor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn receive_image_starts_at_setup_active_position() {
        let (cnc, path) = temp_cnc("udp-image-active-position");
        let dir = temp_dir("udp-image-active-position");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 7)
            .unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();

        let source = SocketAddr::from(([127, 0, 0, 1], 9005));
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(7)
            .set_stream_id(7)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(1024)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);

        conductor
            .on_received_frame(&setup_bytes, source, 2_000)
            .unwrap();
        let image = conductor.images.values().next().unwrap();
        assert_eq!(image.hwm_position(), 1024);
        assert_eq!(image.rebuild_position(), 1024);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn removing_udp_publication_detaches_sender_resource() {
        let (cnc, path) = temp_cnc("remove-udp-publication");
        let dir = temp_dir("remove-udp-publication");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);
        let registration_id = client
            .add_publication("aeron:udp?endpoint=127.0.0.1:0", 7)
            .unwrap();
        conductor.do_work(1_000).unwrap();
        assert_eq!(conductor.take_new_network_publications().len(), 1);

        client.remove_publication(registration_id, false).unwrap();
        conductor.do_work(2_000).unwrap();

        assert_eq!(
            conductor.take_removed_network_publications(),
            vec![registration_id]
        );
        assert!(conductor.publications().is_empty());

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn links_late_udp_subscription_to_existing_image() {
        let (cnc, path) = temp_cnc("udp-image-fanout");
        let dir = temp_dir("udp-image-fanout");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        let channel = "aeron:udp?endpoint=127.0.0.1:0";
        let first_subscription = client.add_subscription(channel, 7).unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();

        let source = SocketAddr::from(([127, 0, 0, 1], 9004));
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(7)
            .set_stream_id(7)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);
        conductor
            .on_received_frame(&setup_bytes, source, 2_000)
            .unwrap();
        let mut initial_available = 0;
        while events
            .receive_next(|msg_type, _, _, _| {
                if msg_type == ON_AVAILABLE_IMAGE {
                    initial_available += 1;
                }
            })
            .unwrap()
        {}
        assert_eq!(initial_available, 1);

        let second_subscription = client.add_subscription(channel, 7).unwrap();
        conductor.do_work(3_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();

        let mut late_available = 0;
        while events
            .receive_next(|msg_type, _, _, _| {
                if msg_type == ON_AVAILABLE_IMAGE {
                    late_available += 1;
                }
            })
            .unwrap()
        {}
        assert_eq!(late_available, 1);
        assert_eq!(conductor.images.len(), 1);
        assert_eq!(
            conductor
                .images
                .values()
                .next()
                .map(|image| image.image_links().count()),
            Some(2)
        );

        client.remove_subscription(second_subscription).unwrap();
        conductor.do_work(5_000).unwrap();
        assert_eq!(
            conductor
                .images
                .values()
                .next()
                .map(|image| image.image_links().count()),
            Some(1)
        );
        client.remove_subscription(first_subscription).unwrap();
        conductor.do_work(6_000).unwrap();
        assert!(conductor.images.is_empty());

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
    fn reports_client_timeout_before_removing_client_resources() {
        let (cnc, path) = temp_cnc("client-timeout-event");
        let dir = temp_dir("client-timeout-event");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);

        client.client_keepalive().unwrap();
        let counter_registration = client.add_counter(88, &[9, 8], "timeout counter").unwrap();
        conductor.do_work(1_000).unwrap();
        assert_eq!(conductor.active_client_count(), 1);
        let mut counter_id = -1;
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    assert_eq!(msg_type, ON_COUNTER_READY);
                    counter_id = CounterUpdateFlyweight::wrap(*buf, offset).counter_id();
                })
                .unwrap()
        );
        assert!(conductor.counters().contains_key(&counter_id));

        assert_eq!(
            conductor.do_work(1_000 + LIVENESS_TIMEOUT_NS + 1).unwrap(),
            1
        );
        assert_eq!(conductor.active_client_count(), 0);
        assert!(!conductor.counters().contains_key(&counter_id));

        let mut seen_client_id = 0;
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    assert_eq!(msg_type, ON_CLIENT_TIMEOUT);
                    seen_client_id = ClientTimeoutFlyweight::wrap(*buf, offset).client_id();
                })
                .unwrap()
        );
        assert_eq!(seen_client_id, CLIENT_ID);
        let mut unavailable_registration = 0;
        let mut unavailable_id = -1;
        assert!(
            events
                .receive_next(|msg_type, buf, offset, _len| {
                    assert_eq!(msg_type, ON_UNAVAILABLE_COUNTER);
                    let update = CounterUpdateFlyweight::wrap(*buf, offset);
                    unavailable_registration = update.correlation_id();
                    unavailable_id = update.counter_id();
                })
                .unwrap()
        );
        assert_eq!(unavailable_registration, counter_registration);
        assert_eq!(unavailable_id, counter_id);

        drop(conductor);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn expires_network_image_and_emits_unavailable_image() {
        let (cnc, path) = temp_cnc("image-liveness");
        let dir = temp_dir("image-liveness");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let mut events = BroadcastReceiver::new(cnc.to_clients_buffer()).unwrap();
        let client = proxy(&cnc);
        let subscription_registration = client
            .add_subscription("aeron:udp?endpoint=127.0.0.1:0", 7)
            .unwrap();
        conductor.do_work(1_000).unwrap();
        let _ = conductor.take_new_receive_endpoints();

        let source = SocketAddr::from(([127, 0, 0, 1], 9002));
        let mut setup_bytes = vec![0u8; aeron_client::protocol::SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut setup_bytes));
        setup
            .header()
            .set_frame_length(aeron_client::protocol::SETUP_HEADER_LENGTH as i32)
            .set_version(CURRENT_VERSION)
            .set_header_type(HDR_TYPE_SETUP);
        setup
            .set_session_id(7)
            .set_stream_id(7)
            .set_initial_term_id(0)
            .set_active_term_id(0)
            .set_term_offset(0)
            .set_term_length(aeron_client::logbuffer::TERM_MIN_LENGTH)
            .set_mtu_length(1408)
            .set_ttl(0);
        conductor
            .on_received_frame(&setup_bytes, source, 1_000)
            .unwrap();

        let mut available_correlation = 0i64;
        while events
            .receive_next(|msg_type, buf, offset, _| {
                if msg_type == ON_AVAILABLE_IMAGE {
                    available_correlation = buf.get_i64(offset);
                }
            })
            .unwrap()
        {}
        assert_ne!(available_correlation, 0);

        client.client_keepalive().unwrap();
        conductor.do_work(5_000_000_000).unwrap();
        // Past the image-liveness timeout: the receiver deactivates and releases the silent image,
        // moving it to DRAINING — but no UNAVAILABLE_IMAGE is emitted until it enters LINGER.
        conductor
            .do_work(IMAGE_LIVENESS_TIMEOUT_NS + 2_000)
            .unwrap();
        // Past the DRAINING release window (5 status-message timeouts) the image enters LINGER and
        // notifies subscribers. The client keepalive keeps the subscription alive throughout.
        conductor
            .do_work(IMAGE_LIVENESS_TIMEOUT_NS + STATUS_MESSAGE_TIMEOUT_NS * 5 + 3_000)
            .unwrap();

        let mut unavailable = None;
        while events
            .receive_next(|msg_type, buf, offset, _| {
                if msg_type == ON_UNAVAILABLE_IMAGE {
                    unavailable = Some((buf.get_i64(offset), buf.get_i64(offset + 8)));
                }
            })
            .unwrap()
        {}
        assert_eq!(
            unavailable,
            Some((available_correlation, subscription_registration))
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
    fn ipc_publication_limit_starts_closed_and_opens_for_a_subscription() {
        let (cnc, path) = temp_cnc("ipc-limit");
        let dir = temp_dir("ipc-limit");
        let mut conductor =
            DriverConductor::new(small_config(), cnc.clone(), dir.0.clone()).unwrap();
        let client = proxy(&cnc);

        let publication = client.add_publication("aeron:ipc", 1010).unwrap();
        conductor.do_work(1_000).unwrap();
        let counter_id = conductor
            .publications()
            .get(&publication)
            .unwrap()
            .publication_limit_counter_id;
        assert_eq!(
            conductor
                .counters()
                .get(&counter_id)
                .unwrap()
                .load(Ordering::Acquire),
            0
        );

        client.add_subscription("aeron:ipc", 1010).unwrap();
        conductor.do_work(2_000).unwrap();
        assert!(
            conductor
                .counters()
                .get(&counter_id)
                .unwrap()
                .load(Ordering::Acquire)
                > 0
        );

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
