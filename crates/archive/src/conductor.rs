//! ArchiveConductor for orchestrating archive recording, replay, and control sessions.
//!
//! Mirrors `io.aeron.archive.ArchiveConductor.java`.

use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::control_session::ControlSession;
use crate::recording_descriptor::RecordingDescriptor;

#[derive(Debug, Clone)]
pub struct RecordingSession {
    pub recording_id: i64,
    pub start_position: i64,
    pub current_position: i64,
    pub stop_position: i64,
    pub is_active: bool,
}

#[derive(Debug, Clone)]
pub struct ReplaySession {
    pub replay_id: i64,
    pub recording_id: i64,
    pub position: i64,
    pub length: i64,
    pub is_active: bool,
}

pub struct ArchiveConductor {
    catalog: Catalog,
    control_sessions: HashMap<i64, ControlSession>,
    recording_sessions: HashMap<i64, RecordingSession>,
    replay_sessions: HashMap<i64, ReplaySession>,
    next_control_session_id: i64,
    next_replay_id: i64,
}

impl Default for ArchiveConductor {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveConductor {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            control_sessions: HashMap::new(),
            recording_sessions: HashMap::new(),
            replay_sessions: HashMap::new(),
            next_control_session_id: 1,
            next_replay_id: 1,
        }
    }

    pub fn do_work(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let mut work_count = 0;

        // Process active recording sessions
        for session in self.recording_sessions.values_mut() {
            if session.is_active && session.current_position < session.stop_position {
                session.current_position += 1408;
                work_count += 1;
            }
        }

        // Process active replay sessions
        for session in self.replay_sessions.values_mut() {
            if session.is_active && session.length > 0 {
                session.position += 1408;
                session.length -= 1408;
                if session.length <= 0 {
                    session.is_active = false;
                }
                work_count += 1;
            }
        }

        Ok(work_count)
    }

    pub fn on_connect(&mut self) -> i64 {
        let session_id = self.next_control_session_id;
        self.next_control_session_id += 1;

        let mut session = ControlSession::new(session_id);
        session.connect();
        self.control_sessions.insert(session_id, session);
        session_id
    }

    pub fn on_start_recording(
        &mut self,
        stream_id: i32,
        channel: &str,
        source_identity: &str,
    ) -> i64 {
        let rec_id = self.catalog.allocate_recording_id();
        let desc = RecordingDescriptor::new(
            0,
            0,
            rec_id,
            0,
            0,
            0,
            0,
            0,
            128 * 1024 * 1024,
            64 * 1024,
            1408,
            0,
            stream_id,
            channel.to_string(),
            channel.to_string(),
            source_identity.to_string(),
        );

        self.catalog.add_recording(desc);

        let recording_session = RecordingSession {
            recording_id: rec_id,
            start_position: 0,
            current_position: 0,
            stop_position: 10 * 1408,
            is_active: true,
        };
        self.recording_sessions.insert(rec_id, recording_session);

        rec_id
    }

    pub fn on_start_replay(&mut self, recording_id: i64, position: i64, length: i64) -> i64 {
        let replay_id = self.next_replay_id;
        self.next_replay_id += 1;

        let replay_session = ReplaySession {
            replay_id,
            recording_id,
            position,
            length,
            is_active: true,
        };
        self.replay_sessions.insert(replay_id, replay_session);

        replay_id
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn control_sessions(&self) -> &HashMap<i64, ControlSession> {
        &self.control_sessions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_handle_connect_and_start_recording() {
        let mut conductor = ArchiveConductor::new();
        let session_id = conductor.on_connect();
        assert_eq!(session_id, 1);
        assert_eq!(conductor.control_sessions().len(), 1);

        let rec_id = conductor.on_start_recording(1001, "aeron:ipc", "test");
        assert_eq!(rec_id, 1);
        assert_eq!(conductor.catalog().recordings().len(), 1);
    }
}
