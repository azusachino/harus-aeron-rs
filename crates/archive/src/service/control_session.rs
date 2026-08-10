//! Control session state for Aeron Archive client connections.
//!
//! Mirrors `io.aeron.archive.ControlSession.java`.

use crate::codecs::{ControlResponse, ControlResponseCode, PROTOCOL_SEMANTIC_VERSION};

pub const SESSION_CLOSED_MSG: &str = "session closed";
pub const RESPONSE_NOT_CONNECTED_MSG: &str = "control response publication is not connected";
pub const REQUEST_IMAGE_NOT_AVAILABLE_MSG: &str = "control request publication image unavailable";

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
    abort_reason: Option<String>,
    active_listing: bool,
    active_listing_abort_reason: Option<String>,
}

impl ControlSession {
    pub fn new(control_session_id: i64) -> Self {
        Self {
            control_session_id,
            state: ControlSessionState::Init,
            last_correlation_id: -1,
            abort_reason: None,
            active_listing: false,
            active_listing_abort_reason: None,
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

    pub fn is_done(&self) -> bool {
        self.state == ControlSessionState::Closed
    }

    pub fn connect(&mut self) {
        self.state = ControlSessionState::Connected;
    }

    pub fn close(&mut self) {
        self.state = ControlSessionState::Closed;
    }

    pub fn abort(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        let may_replace = self.abort_reason.is_some() && reason == SESSION_CLOSED_MSG;
        if !self.is_done() || may_replace {
            self.state = ControlSessionState::Closed;
            self.abort_reason = Some(reason.clone());
            if self.active_listing {
                self.active_listing_abort_reason = Some(reason);
            }
        }
    }

    pub fn abort_reason(&self) -> Option<&str> {
        self.abort_reason.as_deref()
    }

    pub fn set_active_listing(&mut self, active: bool) {
        self.active_listing = active;
        if !active {
            self.active_listing_abort_reason = None;
        }
    }

    pub fn has_active_listing(&self) -> bool {
        self.active_listing
    }

    pub fn active_listing_abort_reason(&self) -> Option<&str> {
        self.active_listing_abort_reason.as_deref()
    }

    pub fn close_warning(&self) -> Option<String> {
        let reason = self.abort_reason.as_deref()?;
        if reason == SESSION_CLOSED_MSG
            || reason == RESPONSE_NOT_CONNECTED_MSG
            || reason.starts_with(REQUEST_IMAGE_NOT_AVAILABLE_MSG)
        {
            return None;
        }
        Some(format!(
            "controlSessionId={} terminated: {reason}",
            self.control_session_id
        ))
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

    #[test]
    fn should_capture_abort_reason_and_abort_active_listing() {
        let mut session = ControlSession::new(42);
        session.set_active_listing(true);

        session.abort("stop execution");

        assert!(session.is_done());
        assert_eq!(session.abort_reason(), Some("stop execution"));
        assert_eq!(
            session.active_listing_abort_reason(),
            Some("stop execution")
        );
    }

    #[test]
    fn abort_is_idempotent_except_for_clean_termination() {
        let mut session = ControlSession::new(42);
        session.set_active_listing(true);

        session.abort("call1");
        session.abort("call2");
        assert_eq!(session.abort_reason(), Some("call1"));
        assert_eq!(session.active_listing_abort_reason(), Some("call1"));

        session.abort(SESSION_CLOSED_MSG);
        assert_eq!(session.abort_reason(), Some(SESSION_CLOSED_MSG));
        assert_eq!(
            session.active_listing_abort_reason(),
            Some(SESSION_CLOSED_MSG)
        );
    }

    #[test]
    fn clean_termination_without_prior_abort_is_not_a_warning() {
        let mut session = ControlSession::new(42);
        session.abort(SESSION_CLOSED_MSG);

        assert_eq!(session.close_warning(), None);
    }

    #[test]
    fn close_warning_classifies_abort_reasons() {
        let mut session = ControlSession::new(42);
        session.abort("test abort reason");
        assert_eq!(
            session.close_warning().as_deref(),
            Some("controlSessionId=42 terminated: test abort reason")
        );

        for reason in [
            RESPONSE_NOT_CONNECTED_MSG,
            "control request publication image unavailable: abc",
            SESSION_CLOSED_MSG,
        ] {
            let mut session = ControlSession::new(42);
            session.abort(reason);
            assert_eq!(session.close_warning(), None, "reason={reason}");
        }
    }
}
