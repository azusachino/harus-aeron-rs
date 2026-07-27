//! Control session state for Aeron Archive client connections.
//!
//! Mirrors `io.aeron.archive.ControlSession.java`.

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
}

impl ControlSession {
    pub fn new(control_session_id: i64) -> Self {
        Self {
            control_session_id,
            state: ControlSessionState::Init,
        }
    }

    pub fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    pub fn state(&self) -> ControlSessionState {
        self.state
    }

    pub fn connect(&mut self) {
        self.state = ControlSessionState::Connected;
    }

    pub fn close(&mut self) {
        self.state = ControlSessionState::Closed;
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

        session.close();
        assert_eq!(session.state(), ControlSessionState::Closed);
    }
}
