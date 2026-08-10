//! Archive client control-channel configuration.
//!
//! Mirrors the configuration boundary of `io.aeron.archive.client.AeronArchive.Context`.
//! Connection ownership and control-session polling remain in the managed service
//! boundary until a high-level Rust Archive client is introduced.

use aeron_client::uri::{
    CONTROL_MODE_PARAM_NAME, ChannelUri, ChannelUriError, MTU_LENGTH_PARAM_NAME,
    SESSION_ID_PARAM_NAME, SPARSE_PARAM_NAME, TERM_LENGTH_PARAM_NAME,
};

pub const DEFAULT_MESSAGE_RETRY_ATTEMPTS: i32 = 3;
pub const DEFAULT_CONTROL_TERM_BUFFER_LENGTH: i32 = 64 * 1024;
pub const DEFAULT_CONTROL_MTU_LENGTH: i32 = 1408;
pub const DEFAULT_CONTROL_TERM_BUFFER_SPARSE: bool = true;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveClientContextError {
    #[error(transparent)]
    ChannelUri(#[from] ChannelUriError),
    #[error("message retry attempts must be > 0, got: {0}")]
    InvalidRetryAttempts(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveClientContext {
    control_request_channel: String,
    control_request_stream_id: i32,
    control_response_channel: String,
    control_response_stream_id: i32,
    control_term_buffer_sparse: bool,
    control_term_buffer_length: i32,
    control_mtu_length: i32,
    message_retry_attempts: i32,
    owns_aeron_client: bool,
}

impl Default for ArchiveClientContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveClientContext {
    pub fn new() -> Self {
        Self {
            control_request_channel: "aeron:ipc".into(),
            control_request_stream_id: 10,
            control_response_channel: "aeron:ipc".into(),
            control_response_stream_id: 20,
            control_term_buffer_sparse: DEFAULT_CONTROL_TERM_BUFFER_SPARSE,
            control_term_buffer_length: DEFAULT_CONTROL_TERM_BUFFER_LENGTH,
            control_mtu_length: DEFAULT_CONTROL_MTU_LENGTH,
            message_retry_attempts: DEFAULT_MESSAGE_RETRY_ATTEMPTS,
            owns_aeron_client: true,
        }
    }

    pub fn control_request_channel(&self) -> &str {
        &self.control_request_channel
    }

    pub fn control_request_stream_id(&self) -> i32 {
        self.control_request_stream_id
    }

    pub fn control_response_channel(&self) -> &str {
        &self.control_response_channel
    }

    pub fn control_response_stream_id(&self) -> i32 {
        self.control_response_stream_id
    }

    pub fn control_term_buffer_sparse(&self) -> bool {
        self.control_term_buffer_sparse
    }

    pub fn control_term_buffer_length(&self) -> i32 {
        self.control_term_buffer_length
    }

    pub fn control_mtu_length(&self) -> i32 {
        self.control_mtu_length
    }

    pub fn message_retry_attempts(&self) -> i32 {
        self.message_retry_attempts
    }

    pub fn owns_aeron_client(&self) -> bool {
        self.owns_aeron_client
    }

    pub fn set_control_request_channel(&mut self, channel: impl Into<String>) -> &mut Self {
        self.control_request_channel = channel.into();
        self
    }

    pub fn set_control_request_stream_id(&mut self, stream_id: i32) -> &mut Self {
        self.control_request_stream_id = stream_id;
        self
    }

    pub fn set_control_response_channel(&mut self, channel: impl Into<String>) -> &mut Self {
        self.control_response_channel = channel.into();
        self
    }

    pub fn set_control_response_stream_id(&mut self, stream_id: i32) -> &mut Self {
        self.control_response_stream_id = stream_id;
        self
    }

    pub fn set_control_term_buffer_sparse(&mut self, sparse: bool) -> &mut Self {
        self.control_term_buffer_sparse = sparse;
        self
    }

    pub fn set_control_term_buffer_length(&mut self, length: i32) -> &mut Self {
        self.control_term_buffer_length = length;
        self
    }

    pub fn set_control_mtu_length(&mut self, length: i32) -> &mut Self {
        self.control_mtu_length = length;
        self
    }

    pub fn set_message_retry_attempts(&mut self, attempts: i32) -> &mut Self {
        self.message_retry_attempts = attempts;
        self
    }

    pub fn set_owns_aeron_client(&mut self, owns: bool) -> &mut Self {
        self.owns_aeron_client = owns;
        self
    }

    pub fn conclude_with_session_id(
        &mut self,
        session_id: i32,
    ) -> Result<&mut Self, ArchiveClientContextError> {
        if self.message_retry_attempts <= 0 {
            return Err(ArchiveClientContextError::InvalidRetryAttempts(
                self.message_retry_attempts,
            ));
        }

        let mut request = ChannelUri::parse(&self.control_request_channel)?;
        let mut response = ChannelUri::parse(&self.control_response_channel)?;
        let response_owns_session_id = response.get(CONTROL_MODE_PARAM_NAME) == Some("response");

        if !response_owns_session_id {
            request.put(SESSION_ID_PARAM_NAME, session_id.to_string());
            response.put(SESSION_ID_PARAM_NAME, session_id.to_string());
        }

        for channel in [&mut request, &mut response] {
            if !channel.contains_key(MTU_LENGTH_PARAM_NAME) {
                channel.put(MTU_LENGTH_PARAM_NAME, self.control_mtu_length.to_string());
            }
            if !channel.contains_key(TERM_LENGTH_PARAM_NAME) {
                channel.put(
                    TERM_LENGTH_PARAM_NAME,
                    self.control_term_buffer_length.to_string(),
                );
            }
            if !channel.contains_key(SPARSE_PARAM_NAME) {
                channel.put(
                    SPARSE_PARAM_NAME,
                    self.control_term_buffer_sparse.to_string(),
                );
            }
        }

        self.control_request_channel = request.to_string();
        self.control_response_channel = response.to_string();
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_one_session_id_and_defaults_to_both_channels() {
        let mut context = ArchiveClientContext::new();
        context
            .set_control_request_channel("aeron:udp?endpoint=localhost:3388")
            .set_control_response_channel("aeron:udp?endpoint=localhost:0")
            .set_control_mtu_length(2048)
            .set_control_term_buffer_length(128 * 1024)
            .set_control_term_buffer_sparse(false)
            .conclude_with_session_id(42)
            .expect("conclude context");

        let request = ChannelUri::parse(context.control_request_channel()).expect("request uri");
        let response = ChannelUri::parse(context.control_response_channel()).expect("response uri");
        assert_eq!(request.get(SESSION_ID_PARAM_NAME), Some("42"));
        assert_eq!(response.get(SESSION_ID_PARAM_NAME), Some("42"));
        for channel in [request, response] {
            assert_eq!(channel.get(MTU_LENGTH_PARAM_NAME), Some("2048"));
            assert_eq!(channel.get(TERM_LENGTH_PARAM_NAME), Some("131072"));
            assert_eq!(channel.get(SPARSE_PARAM_NAME), Some("false"));
        }
    }

    #[test]
    fn preserves_existing_channel_parameters() {
        let mut context = ArchiveClientContext::new();
        context
            .set_control_request_channel("aeron:ipc?alias=request|ssc=false")
            .set_control_response_channel("aeron:ipc?alias=response")
            .conclude_with_session_id(7)
            .expect("conclude context");

        let request = ChannelUri::parse(context.control_request_channel()).expect("request uri");
        assert_eq!(request.get("alias"), Some("request"));
        assert_eq!(request.get("ssc"), Some("false"));
        assert_eq!(request.get(SESSION_ID_PARAM_NAME), Some("7"));
    }

    #[test]
    fn does_not_add_session_id_for_response_control_mode() {
        let mut context = ArchiveClientContext::new();
        context
            .set_control_request_channel("aeron:udp?endpoint=localhost:8080")
            .set_control_response_channel("aeron:udp?control-mode=response|control=localhost:10002")
            .conclude_with_session_id(7)
            .expect("conclude context");

        let request = ChannelUri::parse(context.control_request_channel()).expect("request uri");
        let response = ChannelUri::parse(context.control_response_channel()).expect("response uri");
        assert_eq!(request.get(SESSION_ID_PARAM_NAME), None);
        assert_eq!(response.get(SESSION_ID_PARAM_NAME), None);
    }

    #[test]
    fn rejects_non_positive_retry_attempts() {
        for attempts in [i32::MIN, -1, 0] {
            let mut context = ArchiveClientContext::new();
            context.set_message_retry_attempts(attempts);
            assert_eq!(
                context.conclude_with_session_id(7),
                Err(ArchiveClientContextError::InvalidRetryAttempts(attempts))
            );
        }
    }

    #[test]
    fn defaults_retry_attempts_and_ownership() {
        let context = ArchiveClientContext::new();
        assert_eq!(context.message_retry_attempts(), 3);
        assert!(context.owns_aeron_client());
    }
}
