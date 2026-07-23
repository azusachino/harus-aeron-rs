//! Parser and builder for Aeron channel URIs.
//!
//! Aeron URI format:
//! ```text
//! aeron-uri = [ prefix ":" ] "aeron:" media [ "?" param *( "|" param ) ]
//! media     = "udp" | "ipc"
//! param     = key "=" value
//! ```

use std::collections::BTreeMap;
use std::fmt;

pub const AERON_SCHEME: &str = "aeron";
pub const SPY_QUALIFIER: &str = "aeron-spy";
pub const SPY_PREFIX: &str = "aeron-spy:";
pub const AERON_PREFIX: &str = "aeron:";
pub const MAX_URI_LENGTH: usize = 4095;
pub const UDP_MEDIA: &str = "udp";
pub const IPC_MEDIA: &str = "ipc";
pub const INVALID_TAG: i64 = -1;

pub const TAGS_PARAM_NAME: &str = "tags";
pub const ENDPOINT_PARAM_NAME: &str = "endpoint";
pub const INTERFACE_PARAM_NAME: &str = "interface";
pub const INITIAL_TERM_ID_PARAM_NAME: &str = "initial-term-id";
pub const TERM_ID_PARAM_NAME: &str = "term-id";
pub const TERM_OFFSET_PARAM_NAME: &str = "term-offset";
pub const TERM_LENGTH_PARAM_NAME: &str = "term-length";
pub const SESSION_ID_PARAM_NAME: &str = "session-id";
pub const MTU_LENGTH_PARAM_NAME: &str = "mtu";
pub const TTL_PARAM_NAME: &str = "ttl";
pub const CONTROL_PARAM_NAME: &str = "control";
pub const CONTROL_MODE_PARAM_NAME: &str = "control-mode";
pub const ALIAS_PARAM_NAME: &str = "alias";
pub const CC_PARAM_NAME: &str = "cc";
pub const FC_PARAM_NAME: &str = "fc";
pub const RELIABLE_PARAM_NAME: &str = "reliable";
pub const SPARSE_PARAM_NAME: &str = "sparse";
pub const EOS_PARAM_NAME: &str = "eos";
pub const TETHER_PARAM_NAME: &str = "tether";
pub const GROUP_PARAM_NAME: &str = "group";
pub const REJOIN_PARAM_NAME: &str = "rejoin";
pub const SSC_PARAM_NAME: &str = "ssc";
pub const SPIES_SIMULATE_OFFLINE_PARAM_NAME: &str = "spies-simulate-offline";
pub const SOCKET_SNDBUF_PARAM_NAME: &str = "so-sndbuf";
pub const SOCKET_RCVBUF_PARAM_NAME: &str = "so-rcvbuf";
pub const RCV_INITIAL_WINDOW_PARAM_NAME: &str = "rcv-wnd";
pub const MAX_RESEND_PARAM_NAME: &str = "max-resend";
pub const STREAM_ID_PARAM_NAME: &str = "stream-id";
pub const PUB_WINDOW_PARAM_NAME: &str = "pub-wnd";
pub const GROUP_TAG_PARAM_NAME: &str = "gtag";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChannelUriError {
    #[error("URI length ({length}) exceeds max supported length ({max_len}): {truncated}")]
    ExceedsMaxLength {
        length: usize,
        max_len: usize,
        truncated: String,
    },

    #[error("Aeron URIs must start with 'aeron:', found: {0}")]
    InvalidPrefix(String),

    #[error("invalid media: {0}")]
    InvalidMedia(String),

    #[error("encountered '{character}' within media definition at index {index} in {uri}")]
    InvalidCharInMedia {
        character: char,
        index: usize,
        uri: String,
    },

    #[error("empty key not allowed at index {index} in {uri}")]
    EmptyKey { index: usize, uri: String },

    #[error("invalid end of key at index {index} in {uri}")]
    InvalidKeyEnd { index: usize, uri: String },

    #[error("unexpected end of URI input: {0}")]
    UnexpectedEnd(String),

    #[error("invalid position: {0}")]
    InvalidPosition(i64),
}

/// Parsed representation of an Aeron Channel URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelUri {
    prefix: String,
    media: String,
    params: BTreeMap<String, String>,
    tags: Vec<String>,
}

impl ChannelUri {
    /// Create a new `ChannelUri` directly from components.
    pub fn new(
        prefix: impl Into<String>,
        media: impl Into<String>,
        params: BTreeMap<String, String>,
    ) -> Result<Self, ChannelUriError> {
        let media_str = media.into();
        Self::validate_media(&media_str)?;
        let tags = Self::split_tags(params.get(TAGS_PARAM_NAME).map(|s| s.as_str()));
        Ok(Self {
            prefix: prefix.into(),
            media: media_str,
            params,
            tags,
        })
    }

    /// Parse a channel URI string.
    pub fn parse(uri: &str) -> Result<Self, ChannelUriError> {
        let length = uri.len();
        if length > MAX_URI_LENGTH {
            let truncated = if uri.len() >= MAX_URI_LENGTH {
                uri[..MAX_URI_LENGTH].to_string()
            } else {
                uri.to_string()
            };
            return Err(ChannelUriError::ExceedsMaxLength {
                length,
                max_len: MAX_URI_LENGTH,
                truncated,
            });
        }

        let mut position = 0;
        let prefix = if uri.starts_with(SPY_PREFIX) {
            position = SPY_PREFIX.len();
            SPY_QUALIFIER.to_string()
        } else {
            String::new()
        };

        if !uri[position..].starts_with(AERON_PREFIX) {
            return Err(ChannelUriError::InvalidPrefix(uri.to_string()));
        }
        position += AERON_PREFIX.len();

        #[derive(Copy, Clone, PartialEq, Eq)]
        enum State {
            Media,
            ParamsKey,
            ParamsValue,
        }

        let mut state = State::Media;
        let mut builder = String::new();
        let mut params = BTreeMap::new();
        let mut media = String::new();
        let mut current_key = String::new();

        for (i, c) in uri[position..].chars().enumerate() {
            let idx = position + i;
            match state {
                State::Media => match c {
                    '?' => {
                        media = builder.clone();
                        builder.clear();
                        state = State::ParamsKey;
                    }
                    ':' | '|' | '=' => {
                        return Err(ChannelUriError::InvalidCharInMedia {
                            character: c,
                            index: idx,
                            uri: uri.to_string(),
                        });
                    }
                    _ => builder.push(c),
                },
                State::ParamsKey => match c {
                    '=' => {
                        if builder.is_empty() {
                            return Err(ChannelUriError::EmptyKey {
                                index: idx,
                                uri: uri.to_string(),
                            });
                        }
                        current_key = builder.clone();
                        builder.clear();
                        state = State::ParamsValue;
                    }
                    '|' => {
                        return Err(ChannelUriError::InvalidKeyEnd {
                            index: idx,
                            uri: uri.to_string(),
                        });
                    }
                    _ => builder.push(c),
                },
                State::ParamsValue => match c {
                    '|' => {
                        params.insert(current_key.clone(), builder.clone());
                        builder.clear();
                        state = State::ParamsKey;
                    }
                    _ => builder.push(c),
                },
            }
        }

        match state {
            State::Media => {
                media = builder;
                Self::validate_media(&media)?;
            }
            State::ParamsValue => {
                params.insert(current_key, builder);
            }
            _ => {
                return Err(ChannelUriError::UnexpectedEnd(uri.to_string()));
            }
        }

        let tags = Self::split_tags(params.get(TAGS_PARAM_NAME).map(|s| s.as_str()));

        Ok(Self {
            prefix,
            media,
            params,
            tags,
        })
    }

    fn validate_media(media: &str) -> Result<(), ChannelUriError> {
        if media != UDP_MEDIA && media != IPC_MEDIA {
            return Err(ChannelUriError::InvalidMedia(media.to_string()));
        }
        Ok(())
    }

    fn split_tags(tags_val: Option<&str>) -> Vec<String> {
        match tags_val {
            Some(tags_str) if !tags_str.is_empty() => {
                tags_str.split(',').map(|s| s.to_string()).collect()
            }
            _ => Vec::new(),
        }
    }

    pub fn scheme(&self) -> &str {
        AERON_SCHEME
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn set_prefix(&mut self, prefix: impl Into<String>) {
        self.prefix = prefix.into();
    }

    pub fn media(&self) -> &str {
        &self.media
    }

    pub fn set_media(&mut self, media: impl Into<String>) -> Result<(), ChannelUriError> {
        let m = media.into();
        Self::validate_media(&m)?;
        self.media = m;
        Ok(())
    }

    pub fn is_udp(&self) -> bool {
        self.media == UDP_MEDIA
    }

    pub fn is_ipc(&self) -> bool {
        self.media == IPC_MEDIA
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(|s| s.as_str())
    }

    pub fn get_or<'a>(&'a self, key: &str, default_value: &'a str) -> &'a str {
        self.params
            .get(key)
            .map(|s| s.as_str())
            .unwrap_or(default_value)
    }

    pub fn put(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        let k = key.into();
        let v = value.into();
        let prev = self.params.insert(k.clone(), v);
        if k == TAGS_PARAM_NAME {
            self.tags = Self::split_tags(self.params.get(TAGS_PARAM_NAME).map(|s| s.as_str()));
        }
        prev
    }

    pub fn remove(&mut self, key: &str) -> Option<String> {
        let prev = self.params.remove(key);
        if key == TAGS_PARAM_NAME {
            self.tags = Vec::new();
        }
        prev
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.params.contains_key(key)
    }

    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    pub fn channel_tag(&self) -> Option<&str> {
        self.tags.first().map(|s| s.as_str())
    }

    pub fn entity_tag(&self) -> Option<&str> {
        self.tags.get(1).map(|s| s.as_str())
    }

    pub fn add_session_id(channel: &str, session_id: i32) -> Result<String, ChannelUriError> {
        let mut uri = Self::parse(channel)?;
        uri.put(SESSION_ID_PARAM_NAME, session_id.to_string());
        Ok(uri.to_string())
    }
}

impl fmt::Display for ChannelUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.prefix.is_empty() {
            write!(f, "{}:", self.prefix)?;
        }
        write!(f, "{}{}", AERON_PREFIX, self.media)?;

        if !self.params.is_empty() {
            write!(f, "?")?;
            let mut first = true;
            for (k, v) in &self.params {
                if !first {
                    write!(f, "|")?;
                }
                first = false;
                write!(f, "{}={}", k, v)?;
            }
        }
        Ok(())
    }
}

/// Typesafe builder for building Aeron channel URIs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelUriStringBuilder {
    prefix: Option<String>,
    media: Option<String>,
    params: BTreeMap<String, String>,
}

impl ChannelUriStringBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_uri(uri_str: &str) -> Result<Self, ChannelUriError> {
        let parsed = ChannelUri::parse(uri_str)?;
        let prefix = if parsed.prefix().is_empty() {
            None
        } else {
            Some(parsed.prefix().to_string())
        };
        Ok(Self {
            prefix,
            media: Some(parsed.media().to_string()),
            params: parsed.params,
        })
    }

    pub fn prefix(&mut self, prefix: impl Into<String>) -> &mut Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn media(&mut self, media: impl Into<String>) -> &mut Self {
        self.media = Some(media.into());
        self
    }

    pub fn endpoint(&mut self, endpoint: impl Into<String>) -> &mut Self {
        self.params
            .insert(ENDPOINT_PARAM_NAME.to_string(), endpoint.into());
        self
    }

    pub fn network_interface(&mut self, interface: impl Into<String>) -> &mut Self {
        self.params
            .insert(INTERFACE_PARAM_NAME.to_string(), interface.into());
        self
    }

    pub fn control_endpoint(&mut self, control: impl Into<String>) -> &mut Self {
        self.params
            .insert(CONTROL_PARAM_NAME.to_string(), control.into());
        self
    }

    pub fn control_mode(&mut self, control_mode: impl Into<String>) -> &mut Self {
        self.params
            .insert(CONTROL_MODE_PARAM_NAME.to_string(), control_mode.into());
        self
    }

    pub fn tags(&mut self, tags: impl Into<String>) -> &mut Self {
        self.params.insert(TAGS_PARAM_NAME.to_string(), tags.into());
        self
    }

    pub fn alias(&mut self, alias: impl Into<String>) -> &mut Self {
        self.params
            .insert(ALIAS_PARAM_NAME.to_string(), alias.into());
        self
    }

    pub fn cc(&mut self, cc: impl Into<String>) -> &mut Self {
        self.params.insert(CC_PARAM_NAME.to_string(), cc.into());
        self
    }

    pub fn fc(&mut self, fc: impl Into<String>) -> &mut Self {
        self.params.insert(FC_PARAM_NAME.to_string(), fc.into());
        self
    }

    pub fn ttl(&mut self, ttl: i32) -> &mut Self {
        self.params
            .insert(TTL_PARAM_NAME.to_string(), ttl.to_string());
        self
    }

    pub fn mtu(&mut self, mtu: i32) -> &mut Self {
        self.params
            .insert(MTU_LENGTH_PARAM_NAME.to_string(), mtu.to_string());
        self
    }

    pub fn term_length(&mut self, term_length: i32) -> &mut Self {
        self.params
            .insert(TERM_LENGTH_PARAM_NAME.to_string(), term_length.to_string());
        self
    }

    pub fn initial_term_id(&mut self, initial_term_id: i32) -> &mut Self {
        self.params.insert(
            INITIAL_TERM_ID_PARAM_NAME.to_string(),
            initial_term_id.to_string(),
        );
        self
    }

    pub fn term_id(&mut self, term_id: i32) -> &mut Self {
        self.params
            .insert(TERM_ID_PARAM_NAME.to_string(), term_id.to_string());
        self
    }

    pub fn term_offset(&mut self, term_offset: i32) -> &mut Self {
        self.params
            .insert(TERM_OFFSET_PARAM_NAME.to_string(), term_offset.to_string());
        self
    }

    pub fn session_id(&mut self, session_id: i64) -> &mut Self {
        self.params
            .insert(SESSION_ID_PARAM_NAME.to_string(), session_id.to_string());
        self
    }

    pub fn reliable(&mut self, reliable: bool) -> &mut Self {
        self.params
            .insert(RELIABLE_PARAM_NAME.to_string(), reliable.to_string());
        self
    }

    pub fn sparse(&mut self, sparse: bool) -> &mut Self {
        self.params
            .insert(SPARSE_PARAM_NAME.to_string(), sparse.to_string());
        self
    }

    pub fn eos(&mut self, eos: bool) -> &mut Self {
        self.params
            .insert(EOS_PARAM_NAME.to_string(), eos.to_string());
        self
    }

    pub fn tether(&mut self, tether: bool) -> &mut Self {
        self.params
            .insert(TETHER_PARAM_NAME.to_string(), tether.to_string());
        self
    }

    pub fn group(&mut self, group: bool) -> &mut Self {
        self.params
            .insert(GROUP_PARAM_NAME.to_string(), group.to_string());
        self
    }

    pub fn custom_param(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.params.insert(key.into(), value.into());
        self
    }

    pub fn build(&self) -> String {
        let media = self.media.as_deref().unwrap_or(UDP_MEDIA);
        let prefix = self.prefix.as_deref().unwrap_or("");
        let uri = ChannelUri::new(prefix, media, self.params.clone()).unwrap();
        uri.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_simple_default_uri() {
        let uri_udp = ChannelUri::parse("aeron:udp").unwrap();
        assert_eq!(uri_udp.media(), "udp");
        assert!(uri_udp.is_udp());

        let uri_ipc = ChannelUri::parse("aeron:ipc").unwrap();
        assert_eq!(uri_ipc.media(), "ipc");
        assert!(uri_ipc.is_ipc());

        let uri_spy = ChannelUri::parse("aeron-spy:aeron:ipc").unwrap();
        assert_eq!(uri_spy.prefix(), "aeron-spy");
        assert_eq!(uri_spy.media(), "ipc");
    }

    #[test]
    fn should_reject_uri_without_aeron_prefix() {
        assert!(ChannelUri::parse(":udp").is_err());
        assert!(ChannelUri::parse("aeron").is_err());
        assert!(ChannelUri::parse("aron:").is_err());
        assert!(ChannelUri::parse("eeron:").is_err());
    }

    #[test]
    fn should_reject_with_out_of_place_colon() {
        assert!(ChannelUri::parse("aeron:udp:").is_err());
    }

    #[test]
    fn should_reject_with_invalid_media() {
        assert!(ChannelUri::parse("aeron:ipcsdfgfdhfgf").is_err());
    }

    #[test]
    fn should_reject_with_missing_query_separator_when_followed_with_params() {
        assert!(ChannelUri::parse("aeron:ipc|sparse=true").is_err());
    }

    #[test]
    fn should_reject_with_invalid_params() {
        assert!(ChannelUri::parse("aeron:udp?endpoint=localhost:4652|").is_err());
    }

    #[test]
    fn should_reject_uri_if_length_exceeds_max_allowed() {
        let mut long_uri = String::from("aeron:ipc?alias=");
        while long_uri.len() <= MAX_URI_LENGTH {
            long_uri.push('x');
        }
        let err = ChannelUri::parse(&long_uri).unwrap_err();
        match err {
            ChannelUriError::ExceedsMaxLength {
                length, max_len, ..
            } => {
                assert_eq!(length, long_uri.len());
                assert_eq!(max_len, MAX_URI_LENGTH);
            }
            _ => panic!("expected ExceedsMaxLength error"),
        }
    }

    #[test]
    fn should_parse_with_multiple_parameters() {
        let uri =
            ChannelUri::parse("aeron:udp?endpoint=224.10.9.8:4567|interface=192.168.0.3|ttl=16")
                .unwrap();

        assert_eq!(uri.get("endpoint"), Some("224.10.9.8:4567"));
        assert_eq!(uri.get("interface"), Some("192.168.0.3"));
        assert_eq!(uri.get("ttl"), Some("16"));
    }

    #[test]
    fn should_parse_tags() {
        let uri = ChannelUri::parse("aeron:udp?tags=1001,2002").unwrap();
        assert_eq!(uri.tags(), &["1001".to_string(), "2002".to_string()]);
        assert_eq!(uri.channel_tag(), Some("1001"));
        assert_eq!(uri.entity_tag(), Some("2002"));
    }

    #[test]
    fn should_add_session_id() {
        let uri_str = ChannelUri::add_session_id("aeron:ipc", 42).unwrap();
        let parsed = ChannelUri::parse(&uri_str).unwrap();
        assert_eq!(parsed.get(SESSION_ID_PARAM_NAME), Some("42"));
    }

    #[test]
    fn should_build_channel_uri_with_builder() {
        let mut builder = ChannelUriStringBuilder::new();
        builder
            .media("udp")
            .endpoint("localhost:40123")
            .term_length(65536)
            .ttl(16);

        let uri_str = builder.build();
        let parsed = ChannelUri::parse(&uri_str).unwrap();

        assert_eq!(parsed.media(), "udp");
        assert_eq!(parsed.get("endpoint"), Some("localhost:40123"));
        assert_eq!(parsed.get("term-length"), Some("65536"));
        assert_eq!(parsed.get("ttl"), Some("16"));
    }

    #[test]
    fn should_initialize_builder_from_channel_uri() {
        let uri = "aeron:udp?endpoint=127.0.0.1:40123|ttl=32";
        let mut builder = ChannelUriStringBuilder::from_uri(uri).unwrap();
        builder.session_id(1001);

        let new_uri_str = builder.build();
        let parsed = ChannelUri::parse(&new_uri_str).unwrap();

        assert_eq!(parsed.get("endpoint"), Some("127.0.0.1:40123"));
        assert_eq!(parsed.get("ttl"), Some("32"));
        assert_eq!(parsed.get("session-id"), Some("1001"));
    }
}
