//! ArchiveConductor for orchestrating archive recording, replay, and control sessions.
//!
//! Mirrors `io.aeron.archive.ArchiveConductor.java`.

use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::control_session::ControlSession;
use crate::recording_descriptor::RecordingDescriptor;

pub struct ArchiveConductor {
    catalog: Catalog,
    control_sessions: HashMap<i64, ControlSession>,
    next_control_session_id: i64,
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
            next_control_session_id: 1,
        }
    }

    pub fn do_work(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let work_count = 0;
        // Process control session requests and active recording sessions
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
        rec_id
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
