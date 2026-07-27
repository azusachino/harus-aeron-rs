//! Publication parameters parsed from channel URI and configuration.
//!
//! Mirrors `io.aeron.driver.PublicationParams.java`.
//!
//! Note on tagged-session resolution: `DriverConductor` resolves entity tags and tagged session IDs
//! upstream. In this module, the tag lookup dependency is represented by the [`TaggedPublicationLookup`]
//! trait, which a later driver-conductor epic will back with the real driver publication registries.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::configuration::{self, Configuration};
use crate::error::{DriverError, Result};
use aeron_client::logbuffer::{TERM_MAX_LENGTH, check_term_length, compute_max_message_length};
use aeron_client::uri::{
    CONTROL_MODE_PARAM_NAME, ChannelUri, EOS_PARAM_NAME, INITIAL_TERM_ID_PARAM_NAME,
    MAX_RESEND_PARAM_NAME, MTU_LENGTH_PARAM_NAME, PUB_WINDOW_PARAM_NAME, SESSION_ID_PARAM_NAME,
    SPARSE_PARAM_NAME, SSC_PARAM_NAME, STREAM_ID_PARAM_NAME, TERM_ID_PARAM_NAME,
    TERM_LENGTH_PARAM_NAME, TERM_OFFSET_PARAM_NAME,
};

pub const LINGER_PARAM_NAME: &str = "linger";
pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME: &str = "untethered-window-limit-timeout";
pub const UNTETHERED_LINGER_TIMEOUT_PARAM_NAME: &str = "untethered-linger-timeout";
pub const UNTETHERED_RESTING_TIMEOUT_PARAM_NAME: &str = "untethered-resting-timeout";
pub const RESPONSE_CORRELATION_ID_PARAM_NAME: &str = "response-correlation-id";
pub const PUBLICATION_WINDOW_LENGTH_PARAM_NAME: &str = PUB_WINDOW_PARAM_NAME;
pub const SPIES_SIMULATE_CONNECTION_PARAM_NAME: &str = SSC_PARAM_NAME;
pub const MDC_CONTROL_MODE_PARAM_NAME: &str = CONTROL_MODE_PARAM_NAME;

pub const PROTOTYPE_VALUE_CORRELATION_ID: i64 = -2;
pub const PROTOTYPE_CORRELATION_ID_STR: &str = "prototype";
pub const CONTROL_MODE_RESPONSE: &str = "response";

static SEED: AtomicU64 = AtomicU64::new(0x123456789);

pub fn generate_randomised_id() -> i32 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let count = SEED.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
    let mut x = now ^ count;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    x as i32
}

/// Info returned when resolving a tagged publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaggedPublicationInfo {
    pub session_id: i32,
    pub term_length: usize,
    pub mtu_length: usize,
}

/// Interface for resolving tagged network and IPC publications, and allocating next session ID.
pub trait TaggedPublicationLookup {
    fn find_network_publication_by_tag(&self, tag: i64) -> Option<TaggedPublicationInfo> {
        let _ = tag;
        None
    }
    fn find_ipc_publication_by_tag(&self, tag: i64) -> Option<TaggedPublicationInfo> {
        let _ = tag;
        None
    }
    fn next_available_session_id(&self, stream_id: i32, media: &str) -> i32 {
        let _ = (stream_id, media);
        generate_randomised_id()
    }
}

pub struct DummyPublicationLookup;
impl TaggedPublicationLookup for DummyPublicationLookup {}

/// Parsed publication parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationParams {
    pub linger_timeout_ns: u64,
    pub entity_tag: i64,
    pub untethered_window_limit_timeout_ns: u64,
    pub untethered_linger_timeout_ns: u64,
    pub untethered_resting_timeout_ns: u64,
    pub response_correlation_id: i64,
    pub term_length: usize,
    pub mtu_length: usize,
    pub publication_window_length: usize,
    pub initial_term_id: i32,
    pub term_id: i32,
    pub term_offset: usize,
    pub session_id: i32,
    pub stream_id: i32,
    pub max_resend: i32,
    pub has_position: bool,
    pub is_session_id_tagged: bool,
    pub signal_eos: bool,
    pub is_sparse: bool,
    pub spies_simulate_connection: bool,
    pub is_response: bool,
}

impl PublicationParams {
    pub fn get_publication_params(
        channel_uri: &ChannelUri,
        ctx: &Configuration,
        lookup: &impl TaggedPublicationLookup,
        stream_id: i32,
        canonical_form: &str,
    ) -> Result<Self> {
        let mut params = PublicationParams {
            linger_timeout_ns: ctx.publication_linger_timeout_ns,
            entity_tag: -1,
            untethered_window_limit_timeout_ns: ctx.untethered_window_limit_timeout_ns,
            untethered_linger_timeout_ns: 0,
            untethered_resting_timeout_ns: ctx.untethered_resting_timeout_ns,
            response_correlation_id: -1,
            term_length: if channel_uri.is_ipc() {
                ctx.ipc_term_buffer_length
            } else {
                ctx.publication_term_buffer_length
            },
            mtu_length: if channel_uri.is_ipc() {
                ctx.ipc_mtu_length
            } else {
                ctx.mtu_length
            },
            publication_window_length: 0,
            initial_term_id: 0,
            term_id: 0,
            term_offset: 0,
            session_id: 0,
            stream_id: 0,
            max_resend: ctx.max_resend,
            has_position: false,
            is_session_id_tagged: false,
            signal_eos: true,
            is_sparse: ctx.term_buffer_sparse_file,
            spies_simulate_connection: ctx.spies_simulate_connection,
            is_response: false,
        };

        params.get_stream_id(channel_uri, stream_id)?;
        params.get_entity_tag(channel_uri, lookup)?;
        params.get_session_id(channel_uri, lookup, stream_id, canonical_form)?;
        params.get_term_buffer_length(channel_uri)?;
        params.get_mtu_length(channel_uri)?;
        params.get_publication_window_length(channel_uri, ctx)?;
        params.get_linger_timeout_ns(channel_uri, ctx)?;
        params.get_eos(channel_uri);
        params.get_sparse(channel_uri, ctx);
        params.get_spies_simulate_connection(channel_uri, ctx);
        params.get_untethered_window_limit_timeout(channel_uri, ctx)?;
        params.get_untethered_linger_timeout(channel_uri, ctx)?;
        params.get_untethered_resting_timeout(channel_uri, ctx)?;
        params.get_max_resend(channel_uri, ctx)?;

        params.process_term_triple(channel_uri)?;

        params.is_response =
            channel_uri.get(MDC_CONTROL_MODE_PARAM_NAME) == Some(CONTROL_MODE_RESPONSE);
        params.response_correlation_id = parse_response_correlation_id(channel_uri)?;

        Ok(params)
    }

    fn get_stream_id(&mut self, channel_uri: &ChannelUri, stream_id: i32) -> Result<()> {
        if let Some(stream_id_param) = channel_uri.get(STREAM_ID_PARAM_NAME) {
            let configured_stream_id = stream_id_param.parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel(
                    "ERROR - invalid stream-id, must be a number".to_string(),
                )
            })?;
            if stream_id != configured_stream_id {
                return Err(DriverError::InvalidChannel(format!(
                    "ERROR - stream-id={configured_stream_id} does not match provided streamId={stream_id}"
                )));
            }
        }
        self.stream_id = stream_id;
        Ok(())
    }

    fn get_entity_tag(
        &mut self,
        channel_uri: &ChannelUri,
        lookup: &impl TaggedPublicationLookup,
    ) -> Result<()> {
        if let Some(tag_param) = channel_uri.entity_tag() {
            self.entity_tag = parse_entity_tag(tag_param, lookup, channel_uri)?;
        }
        Ok(())
    }

    fn get_session_id(
        &mut self,
        channel_uri: &ChannelUri,
        lookup: &impl TaggedPublicationLookup,
        stream_id: i32,
        canonical_form: &str,
    ) -> Result<()> {
        if let Some(session_id_str) = channel_uri.get(SESSION_ID_PARAM_NAME) {
            if let Some(tag_str) = session_id_str.strip_prefix("tag:") {
                self.is_session_id_tagged = true;
                let tag = tag_str.parse::<i64>().map_err(|_| {
                    DriverError::InvalidChannel(format!(
                        "ERROR - session-id={session_id_str} has an invalid tag: channel={channel_uri}"
                    ))
                })?;

                if channel_uri.is_ipc() {
                    let publication = lookup.find_ipc_publication_by_tag(tag).ok_or_else(|| {
                        DriverError::InvalidChannel(format!(
                            "ERROR - session-id={session_id_str} must reference an IPC publication: channel={channel_uri}"
                        ))
                    })?;
                    self.session_id = publication.session_id;
                    self.mtu_length = publication.mtu_length;
                    self.term_length = publication.term_length;
                } else {
                    let publication = lookup.find_network_publication_by_tag(tag).ok_or_else(|| {
                        DriverError::InvalidChannel(format!(
                            "ERROR - session-id={session_id_str} must reference a network publication: channel={channel_uri}"
                        ))
                    })?;
                    self.session_id = publication.session_id;
                    self.mtu_length = publication.mtu_length;
                    self.term_length = publication.term_length;
                }
            } else {
                self.session_id = session_id_str.parse::<i32>().map_err(|_| {
                    DriverError::InvalidChannel(
                        "ERROR - invalid session-id, must be a number".to_string(),
                    )
                })?;
            }
        } else {
            self.session_id = lookup.next_available_session_id(stream_id, canonical_form);
        }
        Ok(())
    }

    fn get_term_buffer_length(&mut self, channel_uri: &ChannelUri) -> Result<()> {
        if let Some(term_length_param) = channel_uri.get(TERM_LENGTH_PARAM_NAME) {
            let term_length = parse_size(TERM_LENGTH_PARAM_NAME, term_length_param)?;
            check_term_length(term_length as i32).map_err(|msg| {
                DriverError::InvalidChannel(format!("invalid term-length: {msg}"))
            })?;
            if self.is_session_id_tagged && term_length != self.term_length {
                return Err(DriverError::InvalidChannel(format!(
                    "term-length={term_length} does not match session-id tag value: channel={channel_uri}"
                )));
            }
            self.term_length = term_length;
        }
        Ok(())
    }

    fn get_mtu_length(&mut self, channel_uri: &ChannelUri) -> Result<()> {
        if let Some(mtu_param) = channel_uri.get(MTU_LENGTH_PARAM_NAME) {
            let mtu_length = parse_size(MTU_LENGTH_PARAM_NAME, mtu_param)?;
            validate_mtu_length(mtu_length)?;
            if self.is_session_id_tagged && mtu_length != self.mtu_length {
                return Err(DriverError::InvalidChannel(format!(
                    "mtu={mtu_length} does not match session-id tag value: channel={channel_uri}"
                )));
            }
            self.mtu_length = mtu_length;
        }

        let max_message_length = compute_max_message_length(self.term_length as i32) as usize;
        if self.mtu_length > max_message_length {
            return Err(DriverError::InvalidChannel(format!(
                "MTU greater than max message length for term length: mtu={} maxMessageLength={} termLength={} channel={}",
                self.mtu_length, max_message_length, self.term_length, channel_uri
            )));
        }
        Ok(())
    }

    fn get_publication_window_length(
        &mut self,
        channel_uri: &ChannelUri,
        ctx: &Configuration,
    ) -> Result<()> {
        if let Some(pub_window_param) = channel_uri.get(PUBLICATION_WINDOW_LENGTH_PARAM_NAME) {
            let pub_window = parse_size(PUBLICATION_WINDOW_LENGTH_PARAM_NAME, pub_window_param)?;
            if pub_window < self.mtu_length {
                return Err(DriverError::InvalidChannel(format!(
                    "ERROR - pub-wnd={pub_window} cannot be less than the mtu={}",
                    self.mtu_length
                )));
            }
            let half_term = self.term_length >> 1;
            if pub_window > half_term {
                return Err(DriverError::InvalidChannel(format!(
                    "ERROR - pub-wnd={pub_window} must not exceed half the term-length={}",
                    self.term_length
                )));
            }
            self.publication_window_length = pub_window;
        } else {
            let config_window = if channel_uri.is_ipc() {
                ctx.ipc_publication_term_window_length
            } else {
                ctx.publication_term_window_length
            };
            self.publication_window_length =
                producer_window_length(self.term_length, config_window);
        }
        Ok(())
    }

    fn get_linger_timeout_ns(
        &mut self,
        channel_uri: &ChannelUri,
        ctx: &Configuration,
    ) -> Result<()> {
        if let Some(linger_param) = channel_uri.get(LINGER_PARAM_NAME) {
            self.linger_timeout_ns = parse_duration(LINGER_PARAM_NAME, linger_param)?;
        } else {
            self.linger_timeout_ns = ctx.publication_linger_timeout_ns;
        }
        Ok(())
    }

    fn get_eos(&mut self, channel_uri: &ChannelUri) {
        if let Some(eos_str) = channel_uri.get(EOS_PARAM_NAME) {
            self.signal_eos = eos_str == "true";
        } else {
            self.signal_eos = true;
        }
    }

    fn get_sparse(&mut self, channel_uri: &ChannelUri, ctx: &Configuration) {
        if let Some(sparse_str) = channel_uri.get(SPARSE_PARAM_NAME) {
            self.is_sparse = sparse_str == "true";
        } else {
            self.is_sparse = ctx.term_buffer_sparse_file;
        }
    }

    fn get_spies_simulate_connection(&mut self, channel_uri: &ChannelUri, ctx: &Configuration) {
        if let Some(ssc_str) = channel_uri.get(SPIES_SIMULATE_CONNECTION_PARAM_NAME) {
            self.spies_simulate_connection = ssc_str == "true";
        } else {
            self.spies_simulate_connection = ctx.spies_simulate_connection;
        }
    }

    fn get_untethered_window_limit_timeout(
        &mut self,
        channel_uri: &ChannelUri,
        ctx: &Configuration,
    ) -> Result<()> {
        if let Some(timeout_str) = channel_uri.get(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME) {
            self.untethered_window_limit_timeout_ns =
                parse_duration(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME, timeout_str)?;
        } else {
            self.untethered_window_limit_timeout_ns = ctx.untethered_window_limit_timeout_ns;
        }
        Ok(())
    }

    fn get_untethered_linger_timeout(
        &mut self,
        channel_uri: &ChannelUri,
        ctx: &Configuration,
    ) -> Result<()> {
        if let Some(timeout_str) = channel_uri.get(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME) {
            let val = parse_duration(UNTETHERED_LINGER_TIMEOUT_PARAM_NAME, timeout_str)?;
            self.untethered_linger_timeout_ns = if val == 0 {
                self.untethered_window_limit_timeout_ns
            } else {
                val
            };
        } else {
            let ctx_linger = ctx.untethered_linger_timeout_ns;
            if ctx_linger < 0 {
                self.untethered_linger_timeout_ns = self.untethered_window_limit_timeout_ns;
            } else {
                self.untethered_linger_timeout_ns = ctx_linger as u64;
            }
        }
        Ok(())
    }

    fn get_untethered_resting_timeout(
        &mut self,
        channel_uri: &ChannelUri,
        ctx: &Configuration,
    ) -> Result<()> {
        if let Some(timeout_str) = channel_uri.get(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME) {
            self.untethered_resting_timeout_ns =
                parse_duration(UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, timeout_str)?;
        } else {
            self.untethered_resting_timeout_ns = ctx.untethered_resting_timeout_ns;
        }
        Ok(())
    }

    fn get_max_resend(&mut self, channel_uri: &ChannelUri, ctx: &Configuration) -> Result<()> {
        if let Some(max_resend_str) = channel_uri.get(MAX_RESEND_PARAM_NAME) {
            let max_resend = max_resend_str.parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel("invalid max-resend, must be a number".to_string())
            })?;
            if max_resend <= 0 || max_resend > configuration::MAX_RESEND_MAX {
                return Err(DriverError::InvalidChannel(format!(
                    "invalid max-resend={max_resend}, must be > 0 and <= {}",
                    configuration::MAX_RESEND_MAX
                )));
            }
            self.max_resend = max_resend;
        } else {
            self.max_resend = ctx.max_resend;
        }
        Ok(())
    }

    fn process_term_triple(&mut self, channel_uri: &ChannelUri) -> Result<()> {
        let initial_term_id_str = channel_uri.get(INITIAL_TERM_ID_PARAM_NAME);
        let term_id_str = channel_uri.get(TERM_ID_PARAM_NAME);
        let term_offset_str = channel_uri.get(TERM_OFFSET_PARAM_NAME);

        let mut count = 0;
        if initial_term_id_str.is_some() {
            count += 1;
        }
        if term_id_str.is_some() {
            count += 1;
        }
        if term_offset_str.is_some() {
            count += 1;
        }

        if count > 0 {
            if count < 3 {
                return Err(DriverError::InvalidChannel(format!(
                    "params must be used as a complete set: initial-term-id term-id term-offset channel={channel_uri}"
                )));
            }

            self.initial_term_id = initial_term_id_str.unwrap().parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel("invalid initial-term-id, must be a number".to_string())
            })?;
            self.term_id = term_id_str.unwrap().parse::<i32>().map_err(|_| {
                DriverError::InvalidChannel("invalid term-id, must be a number".to_string())
            })?;
            self.term_offset = term_offset_str.unwrap().parse::<usize>().map_err(|_| {
                DriverError::InvalidChannel("invalid term-offset, must be a number".to_string())
            })?;

            if self.term_offset > self.term_length {
                return Err(DriverError::InvalidChannel(format!(
                    "term-offset={} > term-length={}: channel={channel_uri}",
                    self.term_offset, self.term_length
                )));
            }

            if self.term_offset > TERM_MAX_LENGTH as usize {
                return Err(DriverError::InvalidChannel(format!(
                    "term-offset={} out of range: channel={channel_uri}",
                    self.term_offset
                )));
            }

            if self.term_offset % 32 != 0 {
                return Err(DriverError::InvalidChannel(format!(
                    "term-offset={} must be a multiple of FRAME_ALIGNMENT: channel={channel_uri}",
                    self.term_offset
                )));
            }

            if (self.term_id as i64 - self.initial_term_id as i64) < 0 {
                return Err(DriverError::InvalidChannel(format!(
                    "difference greater than 2^31 - 1: initial-term-id={} when term-id={} channel={channel_uri}",
                    self.initial_term_id, self.term_id
                )));
            }

            self.has_position = true;
        } else {
            self.initial_term_id = generate_randomised_id();
            self.term_id = self.initial_term_id;
            self.term_offset = 0;
            self.has_position = false;
        }
        Ok(())
    }
}

pub fn producer_window_length(term_length: usize, config_window: usize) -> usize {
    let half_term = term_length >> 1;
    if config_window != 0 {
        config_window.min(half_term)
    } else {
        half_term
    }
}

pub fn validate_mtu_length(mtu_length: usize) -> Result<()> {
    if mtu_length < 32 {
        return Err(DriverError::InvalidChannel(format!(
            "MTU length less than min length of 32: mtu={mtu_length}"
        )));
    }
    if mtu_length > 65504 {
        return Err(DriverError::InvalidChannel(format!(
            "MTU length greater than max length of 65504: mtu={mtu_length}"
        )));
    }
    if mtu_length % 32 != 0 {
        return Err(DriverError::InvalidChannel(format!(
            "MTU length must be a multiple of 32: mtu={mtu_length}"
        )));
    }
    Ok(())
}

pub fn parse_size(param_name: &str, s: &str) -> Result<usize> {
    let s = s.trim();
    if s.is_empty() {
        return Err(DriverError::InvalidChannel(format!(
            "invalid {param_name}, empty value"
        )));
    }
    let (num_str, multiplier) = if let Some(stripped) = s.strip_suffix(['k', 'K']) {
        (stripped, 1024)
    } else if let Some(stripped) = s.strip_suffix(['m', 'M']) {
        (stripped, 1024 * 1024)
    } else if let Some(stripped) = s.strip_suffix(['g', 'G']) {
        (stripped, 1024 * 1024 * 1024)
    } else {
        (s, 1)
    };

    num_str
        .parse::<usize>()
        .map(|v| v * multiplier)
        .map_err(|_| {
            DriverError::InvalidChannel(format!("ERROR - invalid {param_name}, must be a number"))
        })
}

pub fn parse_duration(param_name: &str, s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(DriverError::InvalidChannel(format!(
            "invalid {param_name}, empty value"
        )));
    }
    let (num_str, multiplier_ns) = if let Some(stripped) = s.strip_suffix("ns") {
        (stripped, 1u64)
    } else if let Some(stripped) = s.strip_suffix("us") {
        (stripped, 1_000u64)
    } else if let Some(stripped) = s.strip_suffix("ms") {
        (stripped, 1_000_000u64)
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, 1_000_000_000u64)
    } else {
        (s, 1u64)
    };

    num_str
        .parse::<u64>()
        .map(|v| v * multiplier_ns)
        .map_err(|_| {
            DriverError::InvalidChannel(format!("ERROR - invalid {param_name}, must be a number"))
        })
}

fn parse_response_correlation_id(channel_uri: &ChannelUri) -> Result<i64> {
    let id_str = channel_uri
        .get(RESPONSE_CORRELATION_ID_PARAM_NAME)
        .unwrap_or("-1");

    if id_str == PROTOTYPE_CORRELATION_ID_STR {
        return Ok(PROTOTYPE_VALUE_CORRELATION_ID);
    }

    match id_str.parse::<i64>() {
        Ok(value) => {
            if value < -1 {
                Err(DriverError::InvalidChannel(format!(
                    "invalid {RESPONSE_CORRELATION_ID_PARAM_NAME}, must be a number greater than or equal to -1, or '{PROTOTYPE_CORRELATION_ID_STR}'"
                )))
            } else {
                Ok(value)
            }
        }
        Err(_) => Err(DriverError::InvalidChannel(format!(
            "invalid {RESPONSE_CORRELATION_ID_PARAM_NAME}, must be a number greater than or equal to -1, or '{PROTOTYPE_CORRELATION_ID_STR}'"
        ))),
    }
}

fn parse_entity_tag(
    tag_param: &str,
    lookup: &impl TaggedPublicationLookup,
    channel_uri: &ChannelUri,
) -> Result<i64> {
    let entity_tag = tag_param.parse::<i64>().map_err(|_| {
        DriverError::InvalidChannel("invalid entity tag, must be a number".to_string())
    })?;

    if entity_tag == -1 {
        return Err(DriverError::InvalidChannel(format!(
            "-1 tag is reserved: channel={channel_uri}"
        )));
    }

    if lookup.find_network_publication_by_tag(entity_tag).is_some()
        || lookup.find_ipc_publication_by_tag(entity_tag).is_some()
    {
        return Err(DriverError::InvalidChannel(format!(
            "{entity_tag} entityTag already in use: channel={channel_uri}"
        )));
    }

    Ok(entity_tag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct TestLookup {
        next_session_id: i32,
        udp_pub: Option<TaggedPublicationInfo>,
        ipc_pub: Option<TaggedPublicationInfo>,
        network_pub_called_with: RefCell<Vec<i64>>,
        ipc_pub_called_with: RefCell<Vec<i64>>,
    }

    impl TestLookup {
        fn new(next_session_id: i32) -> Self {
            Self {
                next_session_id,
                udp_pub: None,
                ipc_pub: None,
                network_pub_called_with: RefCell::new(Vec::new()),
                ipc_pub_called_with: RefCell::new(Vec::new()),
            }
        }
    }

    impl TaggedPublicationLookup for TestLookup {
        fn find_network_publication_by_tag(&self, tag: i64) -> Option<TaggedPublicationInfo> {
            self.network_pub_called_with.borrow_mut().push(tag);
            self.udp_pub
        }

        fn find_ipc_publication_by_tag(&self, tag: i64) -> Option<TaggedPublicationInfo> {
            self.ipc_pub_called_with.borrow_mut().push(tag);
            self.ipc_pub
        }

        fn next_available_session_id(&self, _stream_id: i32, _media: &str) -> i32 {
            self.next_session_id
        }
    }

    #[test]
    fn basic_parse() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(42);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:1010").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 42, "UDP").unwrap();

        assert_eq!(ctx.max_resend, params.max_resend);
    }

    #[test]
    fn has_max_retransmits() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(8);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:1010|max-resend=100").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 8, "UDP").unwrap();

        assert_eq!(100, params.max_resend);
    }

    #[test]
    fn has_negative_max_retransmits() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(-19);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:1010|max-resend=-1234").unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, -19, "UDP").unwrap_err();

        assert!(err.to_string().contains("must be > 0"));
    }

    #[test]
    fn has_too_big_max_retransmits() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(22);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:1010|max-resend=512").unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 22, "UDP").unwrap_err();

        assert!(err.to_string().contains("and <= 256"));
    }

    #[test]
    fn has_invalid_max_retransmits() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(8);
        let uri =
            ChannelUri::parse("aeron:udp?endpoint=localhost:1010|max-resend=notanumber").unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 8, "UDP").unwrap_err();

        assert!(err.to_string().contains("must be a number"));
    }

    #[test]
    fn should_set_stream_id() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(42);
        let stream_id = 42;
        let uri = ChannelUri::parse("aeron:ipc?alias=x|stream-id=42").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, stream_id, "IPC")
                .unwrap();

        assert_eq!(stream_id, params.stream_id);
    }

    #[test]
    fn should_reject_stream_id_that_is_not_a_number() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(42);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:8080|stream-id=abc").unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 42, "UDP").unwrap_err();

        assert_eq!(
            "ERROR - invalid stream-id, must be a number",
            err.to_string()
        );
    }

    #[test]
    fn should_reject_stream_id_if_it_does_not_match() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(2112);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:8080|stream-id=-19").unwrap();
        let err = PublicationParams::get_publication_params(&uri, &ctx, &lookup, 2112, "UDP")
            .unwrap_err();

        assert_eq!(
            "ERROR - stream-id=-19 does not match provided streamId=2112",
            err.to_string()
        );
    }

    #[test]
    fn should_reject_publication_window_less_than_mtu() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(2112);
        for pub_window in [0, 1407] {
            let uri_str =
                format!("aeron:udp?endpoint=localhost:8080|mtu=1408|pub-wnd={pub_window}");
            let uri = ChannelUri::parse(&uri_str).unwrap();
            let err = PublicationParams::get_publication_params(&uri, &ctx, &lookup, 2112, "UDP")
                .unwrap_err();

            assert_eq!(
                format!("ERROR - pub-wnd={pub_window} cannot be less than the mtu=1408"),
                err.to_string()
            );
        }
    }

    #[test]
    fn should_reject_publication_window_larger_than_half_a_term_length() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(2112);
        for pub_window in [100_000, 32 * 1024 + 1] {
            let uri_str =
                format!("aeron:udp?endpoint=localhost:8080|term-length=64k|pub-wnd={pub_window}");
            let uri = ChannelUri::parse(&uri_str).unwrap();
            let err = PublicationParams::get_publication_params(&uri, &ctx, &lookup, 2112, "UDP")
                .unwrap_err();

            assert_eq!(
                format!("ERROR - pub-wnd={pub_window} must not exceed half the term-length=65536"),
                err.to_string()
            );
        }
    }

    #[test]
    fn should_set_publication_window_from_uri_parameter() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(2112);
        for pub_window in [2048, 32 * 1024] {
            let uri_str =
                format!("aeron:udp?endpoint=localhost:8080|term-length=64k|pub-wnd={pub_window}");
            let uri = ChannelUri::parse(&uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 2112, "UDP")
                    .unwrap();

            assert_eq!(pub_window, params.publication_window_length);
        }
    }

    #[test]
    fn should_set_publication_window_to_half_a_term_if_not_set() {
        let ctx = Configuration {
            ipc_publication_term_window_length: 0,
            publication_term_window_length: 0,
            ..Default::default()
        };
        let lookup = TestLookup::new(100);

        for uri_str in [
            "aeron:ipc?term-length=1m",
            "aeron:udp?endpoint=localhost:5555|term-length=128k",
        ] {
            let uri = ChannelUri::parse(uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 100, "test")
                    .unwrap();

            assert_eq!(params.term_length / 2, params.publication_window_length);
        }
    }

    #[test]
    fn should_set_publication_window_from_config_ipc() {
        let ctx = Configuration {
            ipc_publication_term_window_length: 128 * 1024,
            publication_term_window_length: 0,
            ..Default::default()
        };
        let lookup = TestLookup::new(100);

        let uri = ChannelUri::parse("aeron:ipc?term-length=4m").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 100, "IPC").unwrap();

        assert_eq!(128 * 1024, params.publication_window_length);
    }

    #[test]
    fn should_set_publication_window_from_config_udp() {
        let ctx = Configuration {
            ipc_publication_term_window_length: 0,
            publication_term_window_length: 4096,
            ..Default::default()
        };
        let lookup = TestLookup::new(100);

        let uri = ChannelUri::parse("aeron:udp?term-length=16m|endpoint=localhost:7777").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 100, "UDP").unwrap();

        assert_eq!(4096, params.publication_window_length);
    }

    #[test]
    fn should_generate_session_id_when_not_set() {
        let ctx = Configuration::default();
        let stream_id = -98;
        let expected_session_id = 134238032;
        let lookup = TestLookup::new(expected_session_id);

        for uri_str in [
            "aeron:ipc?term-length=1m",
            "aeron:udp?endpoint=localhost:5555|term-length=128k",
        ] {
            let uri = ChannelUri::parse(uri_str).unwrap();
            let params = PublicationParams::get_publication_params(
                &uri,
                &ctx,
                &lookup,
                stream_id,
                uri.media(),
            )
            .unwrap();

            assert_eq!(expected_session_id, params.session_id);
        }
    }

    #[test]
    fn should_set_session_id_from_the_uri_parameter() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(99999);
        let stream_id = 42;
        let session_id = 12345;

        for uri_str in ["aeron:ipc?mtu=1408", "aeron:udp?endpoint=localhost:5555"] {
            let full_uri = format!("{uri_str}|session-id={session_id}");
            let uri = ChannelUri::parse(&full_uri).unwrap();
            let params = PublicationParams::get_publication_params(
                &uri,
                &ctx,
                &lookup,
                stream_id,
                uri.media(),
            )
            .unwrap();

            assert_eq!(session_id, params.session_id);
        }
    }

    #[test]
    fn should_reject_tagged_session_if_tag_is_invalid() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(42);
        let uri =
            ChannelUri::parse("aeron:udp?endpoint=localhost:5555|session-id=tag:abc").unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 42, "UDP").unwrap_err();

        assert_eq!(
            "ERROR - session-id=tag:abc has an invalid tag: channel=aeron:udp?endpoint=localhost:5555|session-id=tag:abc",
            err.to_string()
        );
    }

    #[test]
    fn should_reject_tagged_session_if_unknown_publication_reference_udp() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(42);
        let tag = 888;
        let uri_str = format!("aeron:udp?endpoint=localhost:5555|session-id=tag:{tag}");
        let uri = ChannelUri::parse(&uri_str).unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 42, "UDP").unwrap_err();

        assert_eq!(
            format!(
                "ERROR - session-id=tag:{tag} must reference a network publication: channel={uri}"
            ),
            err.to_string()
        );
        assert_eq!(vec![tag], *lookup.network_pub_called_with.borrow());
    }

    #[test]
    fn should_reject_tagged_session_if_unknown_publication_reference_ipc() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(42);
        let tag = 777;
        let uri_str = format!("aeron:ipc?session-id=tag:{tag}");
        let uri = ChannelUri::parse(&uri_str).unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 42, "IPC").unwrap_err();

        assert_eq!(
            format!(
                "ERROR - session-id=tag:{tag} must reference an IPC publication: channel={uri}"
            ),
            err.to_string()
        );
        assert_eq!(vec![tag], *lookup.ipc_pub_called_with.borrow());
    }

    #[test]
    fn should_initialize_session_id_term_buffer_length_and_mtu_from_referenced_udp_publication_if_tagged()
     {
        let ctx = Configuration::default();
        let tag = 42;
        let mtu = 1408;
        let term_length = 1024 * 1024;
        let session_id = -4658437;
        let mut lookup = TestLookup::new(0);
        lookup.udp_pub = Some(TaggedPublicationInfo {
            session_id,
            term_length,
            mtu_length: mtu,
        });

        let uri_str = format!("aeron:udp?endpoint=localhost:5555|session-id=tag:{tag}");
        let uri = ChannelUri::parse(&uri_str).unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 42, "UDP").unwrap();

        assert_eq!(session_id, params.session_id);
        assert_eq!(mtu, params.mtu_length);
        assert_eq!(term_length, params.term_length);
        assert_eq!(vec![tag], *lookup.network_pub_called_with.borrow());
    }

    #[test]
    fn should_initialize_session_id_term_buffer_length_and_mtu_from_referenced_ipc_publication_if_tagged()
     {
        let ctx = Configuration::default();
        let tag = -5;
        let mtu = 2048;
        let term_length = 64 * 1024;
        let session_id = 100;
        let mut lookup = TestLookup::new(0);
        lookup.ipc_pub = Some(TaggedPublicationInfo {
            session_id,
            term_length,
            mtu_length: mtu,
        });

        let uri_str = format!("aeron:ipc?session-id=tag:{tag}");
        let uri = ChannelUri::parse(&uri_str).unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, -87, "IPC").unwrap();

        assert_eq!(session_id, params.session_id);
        assert_eq!(mtu, params.mtu_length);
        assert_eq!(term_length, params.term_length);
        assert_eq!(vec![tag], *lookup.ipc_pub_called_with.borrow());
    }

    #[test]
    fn should_return_true_for_eos_if_not_defined() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        for uri_str in ["aeron:ipc", "aeron:udp?endpoint=localhost:5555"] {
            let uri = ChannelUri::parse(uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, uri.media())
                    .unwrap();
            assert!(params.signal_eos);
        }
    }

    #[test]
    fn should_set_eos_from_uri() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        for eos in [true, false] {
            let uri_str = format!("aeron:ipc?eos={eos}");
            let uri = ChannelUri::parse(&uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, uri.media())
                    .unwrap();
            assert_eq!(eos, params.signal_eos);
        }
    }

    #[test]
    fn should_return_config_value_for_sparse() {
        let lookup = TestLookup::new(1);
        for sparse in [true, false] {
            let ctx = Configuration {
                term_buffer_sparse_file: sparse,
                ..Default::default()
            };
            let uri = ChannelUri::parse("aeron:ipc").unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();
            assert_eq!(sparse, params.is_sparse);
        }
    }

    #[test]
    fn should_set_sparse_from_uri() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        for sparse in [true, false] {
            let uri_str = format!("aeron:ipc?sparse={sparse}");
            let uri = ChannelUri::parse(&uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();
            assert_eq!(sparse, params.is_sparse);
        }
    }

    #[test]
    fn should_return_config_value_for_spies_simulate_connection_if_not_defined() {
        let lookup = TestLookup::new(1);
        for ssc in [true, false] {
            let ctx = Configuration {
                spies_simulate_connection: ssc,
                ..Default::default()
            };
            let uri = ChannelUri::parse("aeron:ipc").unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();
            assert_eq!(ssc, params.spies_simulate_connection);
        }
    }

    #[test]
    fn should_set_spies_simulate_connection_from_uri() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        for ssc in [true, false] {
            let uri_str = format!("aeron:ipc?ssc={ssc}");
            let uri = ChannelUri::parse(&uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();
            assert_eq!(ssc, params.spies_simulate_connection);
        }
    }

    #[test]
    fn should_return_linger_timeout_from_config_if_not_set() {
        let ctx = Configuration {
            publication_linger_timeout_ns: 7_000_000_000,
            ..Default::default()
        };
        let lookup = TestLookup::new(1);

        for uri_str in ["aeron:ipc", "aeron:udp?endpoint=localhost:5555"] {
            let uri = ChannelUri::parse(uri_str).unwrap();
            let params =
                PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, uri.media())
                    .unwrap();
            assert_eq!(7_000_000_000, params.linger_timeout_ns);
        }
    }

    #[test]
    fn should_set_linger_timeout() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        let linger_ms = 321;
        let uri_str = format!("aeron:ipc?mtu=4k|linger={linger_ms}ms");
        let uri = ChannelUri::parse(&uri_str).unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();

        assert_eq!(321_000_000, params.linger_timeout_ns);
    }

    #[test]
    fn should_return_untethered_window_limit_timeout_from_config_if_not_set() {
        let ctx = Configuration {
            untethered_window_limit_timeout_ns: 7_000_000_000,
            ..Default::default()
        };
        let lookup = TestLookup::new(1);

        let uri = ChannelUri::parse("aeron:ipc").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();
        assert_eq!(7_000_000_000, params.untethered_window_limit_timeout_ns);
    }

    #[test]
    fn should_set_untethered_window_limit_timeout() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        let uri =
            ChannelUri::parse("aeron:ipc?mtu=4k|untethered-window-limit-timeout=456us").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();

        assert_eq!(456_000, params.untethered_window_limit_timeout_ns);
    }

    #[test]
    fn should_return_untethered_resting_timeout_from_config_if_not_set() {
        let ctx = Configuration {
            untethered_resting_timeout_ns: 7_000_000_000,
            ..Default::default()
        };
        let lookup = TestLookup::new(1);

        let uri = ChannelUri::parse("aeron:ipc").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();
        assert_eq!(7_000_000_000, params.untethered_resting_timeout_ns);
    }

    #[test]
    fn should_set_untethered_resting_timeout() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        let uri = ChannelUri::parse("aeron:ipc?mtu=4k|untethered-resting-timeout=456ns").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap();

        assert_eq!(456, params.untethered_resting_timeout_ns);
    }

    #[test]
    fn should_honor_precedence_for_untethered_linger_timeout_with_defaults_in_context() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        let uri = ChannelUri::parse(
            "aeron:udp?endpoint=localhost:5555|untethered-window-limit-timeout=444ms",
        )
        .unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "UDP").unwrap();

        assert_eq!(444_000_000, params.untethered_window_limit_timeout_ns);
        assert_eq!(444_000_000, params.untethered_linger_timeout_ns);
    }

    #[test]
    fn should_honor_precedence_for_untethered_linger_timeout_with_overridden_context() {
        let ctx = Configuration {
            untethered_linger_timeout_ns: 555_000_000,
            ..Default::default()
        };
        let lookup = TestLookup::new(1);
        let uri = ChannelUri::parse(
            "aeron:udp?endpoint=localhost:5555|untethered-window-limit-timeout=444ms",
        )
        .unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "UDP").unwrap();

        assert_eq!(444_000_000, params.untethered_window_limit_timeout_ns);
        assert_eq!(555_000_000, params.untethered_linger_timeout_ns);
    }

    #[test]
    fn should_honor_channel_override_for_untethered_linger_timeout() {
        let ctx = Configuration {
            untethered_linger_timeout_ns: 555_000_000,
            ..Default::default()
        };
        let lookup = TestLookup::new(1);
        let uri = ChannelUri::parse("aeron:udp?endpoint=localhost:5555|untethered-window-limit-timeout=444ms|untethered-linger-timeout=333ms").unwrap();
        let params =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "UDP").unwrap();

        assert_eq!(444_000_000, params.untethered_window_limit_timeout_ns);
        assert_eq!(333_000_000, params.untethered_linger_timeout_ns);
    }

    #[test]
    fn should_discard_session_id_if_not_integer() {
        let ctx = Configuration::default();
        let lookup = TestLookup::new(1);
        let uri = ChannelUri::parse("aeron:ipc?session-id=abc").unwrap();
        let err =
            PublicationParams::get_publication_params(&uri, &ctx, &lookup, 1, "IPC").unwrap_err();

        assert_eq!(
            "ERROR - invalid session-id, must be a number",
            err.to_string()
        );
    }
}
