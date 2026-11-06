use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use aeron_client::uri::{
    ChannelUri, REJOIN_PARAM_NAME, RELIABLE_PARAM_NAME, SESSION_ID_PARAM_NAME, TETHER_PARAM_NAME,
};

use crate::error::{DriverError, Result};
use crate::publication_image::UntetheredSubscriptionStateKind;
use crate::publication_params::{
    TaggedPublicationInfo, TaggedPublicationLookup, UNTETHERED_LINGER_TIMEOUT_PARAM_NAME,
    UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME,
    parse_duration,
};

pub use crate::publication_image::UntetheredSubscriptionStateKind as IpcSubscriptionStateKind;

/// Source identity reported for IPC images — matches upstream's `aeron:ipc` channel identity.
pub const IPC_SOURCE_IDENTITY: &str = "aeron:ipc";

/// Source identity reported for network images.
pub const UDP_SOURCE_IDENTITY: &str = "aeron:udp";

pub const IMAGE_LIVENESS_TIMEOUT_NS: i64 = 10_000_000_000;
/// Matches `Configuration.STATUS_MESSAGE_TIMEOUT_DEFAULT_NS` in vendored Aeron; paces the image
/// DRAINING->LINGER release window.
pub const STATUS_MESSAGE_TIMEOUT_NS: i64 = 200_000_000;
/// Matches `Configuration.PENDING_SETUPS_TIMEOUT_NS` in vendored Aeron.
pub const PENDING_SETUPS_TIMEOUT_NS: i64 = 1_000_000_000;

pub type NetworkImageKey = (String, i32, i32, SocketAddr);
pub type NetworkSessionKey = NetworkImageKey;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationUpdate {
    Add(SocketAddr),
    Remove(SocketAddr),
}

/// Receive-side session states from Aeron's `DataPacketDispatcher`.
///
/// Reference: `vendor/aeron/aeron-driver/src/main/java/io/aeron/driver/DataPacketDispatcher.java`
/// (`SessionState`, `onDataPacket`, and `onSetupMessage`). This is the Rust equivalent of
/// Aeron's `DataPacketDispatcher`; the dispatcher is intentionally kept in the conductor so
/// endpoint ownership, image lifecycle, and CnC events share one state machine. This state is
/// intentionally separate from `images`: DATA may arrive before SETUP, and cooldown/no-interest
/// sessions must not create an image just because a later SETUP is received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkSessionState {
    PendingSetupFrame,
    InitInProgress,
    Active,
    OnCoolDown,
    NoInterest,
}

/// Returns `true` for an IPC channel (`aeron:ipc`, optionally with parameters).
pub fn is_ipc_channel(channel: &str) -> bool {
    channel == "aeron:ipc" || channel.starts_with("aeron:ipc?")
}

pub fn is_spy_channel(channel: &str) -> bool {
    ChannelUri::parse(channel).is_ok_and(|uri| uri.prefix() == aeron_client::uri::SPY_QUALIFIER)
}

pub fn underlying_channel(channel: &str) -> Result<String> {
    let mut uri = ChannelUri::parse(channel)
        .map_err(|error| DriverError::InvalidChannel(format!("parse channel: {error}")))?;
    if uri.prefix() == aeron_client::uri::SPY_QUALIFIER {
        uri.set_prefix("");
    }
    Ok(uri.to_string())
}

/// Returns `true` for a UDP (network) channel.
pub fn is_udp_channel(channel: &str) -> bool {
    channel.starts_with("aeron:udp")
}

#[derive(Debug, Clone)]
pub struct SubscriptionOptions {
    pub identity: String,
    pub session_id: Option<i32>,
    pub reliable: bool,
    pub tethered: bool,
    pub rejoin: bool,
    pub untethered_window_limit_timeout_ns: Option<u64>,
    pub untethered_linger_timeout_ns: Option<u64>,
    pub untethered_resting_timeout_ns: Option<u64>,
}

/// Return the transport identity and options that affect subscription dispatch.
///
/// Reliability is a subscription property, while `session-id` narrows the interest within an
/// otherwise shared transport. Removing both from the identity lets the conductor reject the
/// same-session reliability conflict without treating subscriptions for different sessions as
/// conflicting.
pub fn subscription_options(channel: &str) -> Result<SubscriptionOptions> {
    let mut uri = ChannelUri::parse(underlying_channel(channel)?.as_str())
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
    let tethered = uri
        .get(TETHER_PARAM_NAME)
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                DriverError::InvalidChannel(format!("invalid tether value in channel: {value}"))
            })
        })
        .transpose()?
        .unwrap_or(true);
    let rejoin = uri
        .get(REJOIN_PARAM_NAME)
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                DriverError::InvalidChannel(format!("invalid rejoin value in channel: {value}"))
            })
        })
        .transpose()?
        .unwrap_or(true);
    let untethered_window_limit_timeout_ns = uri
        .get(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME)
        .map(|value| parse_duration(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME, value))
        .transpose()?;
    let untethered_linger_timeout_ns = uri
        .get(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME)
        .map(|value| parse_duration(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME, value))
        .transpose()?;
    let untethered_resting_timeout_ns = uri
        .get(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME)
        .map(|value| parse_duration(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, value))
        .transpose()?;

    uri.remove(RELIABLE_PARAM_NAME);
    uri.remove(SESSION_ID_PARAM_NAME);
    uri.remove(TETHER_PARAM_NAME);
    uri.remove(REJOIN_PARAM_NAME);
    uri.remove(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME);
    uri.remove(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME);
    uri.remove(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME);
    Ok(SubscriptionOptions {
        identity: uri.to_string(),
        session_id,
        reliable,
        tethered,
        rejoin,
        untethered_window_limit_timeout_ns,
        untethered_linger_timeout_ns,
        untethered_resting_timeout_ns,
    })
}

pub fn subscription_conflicts(
    existing: &SubscriptionEntry,
    identity: &str,
    stream_id: i32,
    session_id: Option<i32>,
    reliable: bool,
) -> bool {
    subscription_options(&existing.channel).is_ok_and(|opts| opts.identity == identity)
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
pub fn publication_options(channel: &str) -> Result<(String, Option<i32>)> {
    let mut uri = ChannelUri::parse(channel)
        .map_err(|error| DriverError::InvalidChannel(format!("parse channel: {error}")))?;
    let session_id = uri
        .get(SESSION_ID_PARAM_NAME)
        .and_then(|value| value.parse::<i32>().ok());
    uri.remove(SESSION_ID_PARAM_NAME);
    Ok((uri.to_string(), session_id))
}

/// Round up to the next 4-byte boundary (variable-length command fields are SBE-aligned).
pub fn align4(value: usize) -> usize {
    (value + 3) & !3
}

pub fn stable_channel_tag(channel: &str) -> String {
    let mut hasher = DefaultHasher::new();
    channel.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationState {
    Active,
    Draining,
    Linger,
    Closed,
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
    pub spies_simulate_connection: bool,
    pub session_id: i32,
    /// Entity tag used to resolve tagged session ids and reject duplicate tagged publications.
    pub entity_tag: i64,
    pub term_length: usize,
    pub mtu_length: usize,
    pub initial_term_id: i32,
    /// Counter id of the publication limit position (driver-assigned).
    pub publication_limit_counter_id: i32,
    /// Absolute path of the shared log file backing this publication; `None` until a log
    /// buffer is allocated (network publications get theirs in the Sender data-path task).
    pub log_file_name: Option<String>,
    pub state: PublicationState,
    pub time_of_last_state_change_ns: i64,
    pub clean_position: i64,
}

pub struct ActivePublicationLookup<'a> {
    pub publications: &'a HashMap<i64, PublicationEntry>,
    pub next_session_id: i32,
}

impl TaggedPublicationLookup for ActivePublicationLookup<'_> {
    fn find_network_publication_by_tag(&self, tag: i64) -> Option<TaggedPublicationInfo> {
        self.publications.values().find_map(|publication| {
            (publication.entity_tag == tag && !publication.is_ipc).then_some(
                TaggedPublicationInfo {
                    session_id: publication.session_id,
                    term_length: publication.term_length,
                    mtu_length: publication.mtu_length,
                },
            )
        })
    }

    fn find_ipc_publication_by_tag(&self, tag: i64) -> Option<TaggedPublicationInfo> {
        self.publications.values().find_map(|publication| {
            (publication.entity_tag == tag && publication.is_ipc).then_some(TaggedPublicationInfo {
                session_id: publication.session_id,
                term_length: publication.term_length,
                mtu_length: publication.mtu_length,
            })
        })
    }

    fn next_available_session_id(&self, _stream_id: i32, _media: &str) -> i32 {
        self.next_session_id
    }
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
    /// Whether this subscription participates in local publication flow control.
    pub tethered: bool,
    /// Whether an untethered subscription rejoins after linger.
    pub rejoin: bool,
    pub untethered_window_limit_timeout_ns: Option<u64>,
    pub untethered_linger_timeout_ns: Option<u64>,
    pub untethered_resting_timeout_ns: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct IpcSubscriptionState {
    pub publication_registration_id: i64,
    pub correlation_id: i64,
    pub subscriber_position_id: i32,
    pub tethered: bool,
    pub rejoin: bool,
    pub state: UntetheredSubscriptionStateKind,
    pub time_of_last_update_ns: i64,
    pub window_limit_timeout_ns: u64,
    pub linger_timeout_ns: u64,
    pub resting_timeout_ns: u64,
}

#[derive(Debug)]
pub struct ReceiveChannelReference {
    pub references: usize,
    /// The Receiver identity of the endpoint that was actually bound. Wildcard ports can
    /// receive a unique canonical suffix each time their URI is parsed.
    pub canonical_form: String,
}
