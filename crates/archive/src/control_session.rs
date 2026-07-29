//! Control session state for Aeron Archive client connections.
//!
//! Mirrors `io.aeron.archive.ControlSession.java`.

use crate::codecs::{ControlResponse, ControlResponseCode, PROTOCOL_SEMANTIC_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSessionState {
    Init,
    Connected,
    Closed,
}

#[derive(Debug, Clone)]
pub struct ControlSession {
    control_session_id: i64,
    state: ControlSessionState,
    last_correlation_id: i64,
}

impl ControlSession {
    pub fn new(control_session_id: i64) -> Self {
        Self {
            control_session_id,
            state: ControlSessionState::Init,
            last_correlation_id: -1,
        }
    }

    pub fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    pub fn state(&self) -> ControlSessionState {
        self.state
    }

    pub fn last_correlation_id(&self) -> i64 {
        self.last_correlation_id
    }

    pub fn connect(&mut self) {
        self.state = ControlSessionState::Connected;
    }

    pub fn close(&mut self) {
        self.state = ControlSessionState::Closed;
    }

    pub fn build_ok_response(&mut self, correlation_id: i64, relevant_id: i64) -> ControlResponse {
        self.last_correlation_id = correlation_id;
        ControlResponse {
            control_session_id: self.control_session_id,
            correlation_id,
            relevant_id,
            code: ControlResponseCode::Ok,
            version: PROTOCOL_SEMANTIC_VERSION,
            error_message: String::new(),
        }
    }

    pub fn build_error_response(
        &mut self,
        correlation_id: i64,
        error_code: i32,
    ) -> ControlResponse {
        self.last_correlation_id = correlation_id;
        ControlResponse {
            control_session_id: self.control_session_id,
            correlation_id,
            relevant_id: error_code as i64,
            code: ControlResponseCode::Error,
            version: PROTOCOL_SEMANTIC_VERSION,
            error_message: format!("archive error {error_code}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_manage_control_session_lifecycle() {
        let mut session = ControlSession::new(100);
        assert_eq!(session.state(), ControlSessionState::Init);

        session.connect();
        assert_eq!(session.state(), ControlSessionState::Connected);

        let ok_resp = session.build_ok_response(500, 42);
        assert_eq!(ok_resp.control_session_id, 100);
        assert_eq!(ok_resp.correlation_id, 500);
        assert_eq!(ok_resp.relevant_id, 42);
        assert_eq!(ok_resp.code, ControlResponseCode::Ok);

        let err_resp = session.build_error_response(501, 1);
        assert_eq!(err_resp.code, ControlResponseCode::Error);

        session.close();
        assert_eq!(session.state(), ControlSessionState::Closed);
    }
}
