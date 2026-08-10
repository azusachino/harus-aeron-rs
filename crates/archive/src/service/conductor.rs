//! ArchiveConductor for orchestrating archive recording, replay, and control sessions.
//!
//! Mirrors `io.aeron.archive.ArchiveConductor.java`.

use std::collections::HashMap;

use aeron_client::uri::{ChannelUri, ChannelUriError, ChannelUriStringBuilder};

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

/// Build the recording channel form used by Archive descriptors and replay.
///
/// Aeron keeps transport and session-identifying parameters while dropping
/// publication-only settings that should not affect recording identity.
pub fn stripped_channel(channel: &str) -> Result<String, ChannelUriError> {
    let uri = ChannelUri::parse(channel)?;
    let mut builder = ChannelUriStringBuilder::new();
    builder.media(uri.media());

    for key in [
        "endpoint",
        "interface",
        "control",
        "control-mode",
        "tags",
        "rejoin",
        "group",
        "tether",
        "fc",
        "gtag",
        "cc",
        "so-rcvbuf",
        "so-sndbuf",
        "rcv-wnd",
        "channel-snd-ts-offset",
        "channel-rcv-ts-offset",
        "media-rcv-ts-offset",
        "session-id",
        "alias",
        "response-correlation-id",
        "response-endpoint",
        "ttl",
        "stream-id",
    ] {
        if let Some(value) = uri.get(key) {
            builder.custom_param(key, value);
        }
    }

    Ok(builder.build())
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

    pub fn on_stop_recording(&mut self, recording_id: i64) -> bool {
        if let Some(session) = self.recording_sessions.get_mut(&recording_id) {
            if session.is_active {
                session.is_active = false;
                session.stop_position = session.current_position;
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                self.catalog
                    .update_stop_position(recording_id, session.current_position, now_ts);
                return true;
            }
        }
        false
    }

    pub fn on_stop_replay(&mut self, replay_id: i64) -> bool {
        if let Some(session) = self.replay_sessions.get_mut(&replay_id) {
            if session.is_active {
                session.is_active = false;
                return true;
            }
        }
        false
    }

    pub fn list_recordings(
        &self,
        from_recording_id: i64,
        record_count: i32,
    ) -> Vec<RecordingDescriptor> {
        if record_count <= 0 {
            return Vec::new();
        }
        let count = record_count as usize;
        self.catalog
            .recordings()
            .iter()
            .filter(|d| d.recording_id >= from_recording_id)
            .take(count)
            .cloned()
            .collect()
    }

    pub fn list_recordings_for_uri(
        &self,
        from_recording_id: i64,
        record_count: i32,
        channel: &str,
        stream_id: i32,
    ) -> Vec<RecordingDescriptor> {
        if record_count <= 0 {
            return Vec::new();
        }
        let count = record_count as usize;
        self.catalog
            .recordings()
            .iter()
            .filter(|d| {
                d.recording_id >= from_recording_id
                    && d.stream_id == stream_id
                    && d.original_channel.contains(channel)
            })
            .take(count)
            .cloned()
            .collect()
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

        // List recordings
        let list = conductor.list_recordings(1, 10);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].recording_id, rec_id);

        let list_uri = conductor.list_recordings_for_uri(1, 10, "aeron:ipc", 1001);
        assert_eq!(list_uri.len(), 1);

        // Replay lifecycle
        let replay_id = conductor.on_start_replay(rec_id, 0, 14080);
        assert_eq!(replay_id, 1);
        assert!(conductor.on_stop_replay(replay_id));
        assert!(!conductor.on_stop_replay(replay_id));

        // Stop recording
        assert!(conductor.on_stop_recording(rec_id));
        assert!(!conductor.on_stop_recording(rec_id));
    }

    #[test]
    fn stripped_channel_copies_recording_identity_parameters() {
        let original = "aeron:udp?term-length=1g|mtu=2048|session-id=tag:424242424242|rcv-wnd=32k|so-sndbuf=2m|so-rcvbuf=1m|endpoint=some-host:7777|interface=192.168.0.1/24|control-mode=dynamic|control=localhost:5555|tether=false|reliable=false|linger=1321ms|sparse=false|cc=cubic|alias=test-stripped-channel|fc=tagged,g:1111/3,t:4s|gtag=2222|tags=17,432|tag=5|ttl=3|eos=true|rejoin=false|ssc=true|group=true|response-correlation-id=88888888881|response-endpoint=publisher:10101|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=136|channel-snd-ts-offset=144|init-term-id=300|term-id=341|term-offset=4096|nak-delay=100ns|untethered-window-limit-timeout=3s|untethered-linger-timeout=2s|untethered-resting-timeout=1s|max-resend=13|stream-id=-654|pub-wnd=32m";

        let stripped = ChannelUri::parse(&stripped_channel(original).unwrap()).unwrap();
        assert_eq!(stripped.media(), "udp");
        assert_eq!(stripped.get("endpoint"), Some("some-host:7777"));
        assert_eq!(stripped.get("interface"), Some("192.168.0.1/24"));
        assert_eq!(stripped.get("control"), Some("localhost:5555"));
        assert_eq!(stripped.get("tags"), Some("17,432"));
        assert_eq!(stripped.get("session-id"), Some("tag:424242424242"));
        assert_eq!(stripped.get("stream-id"), Some("-654"));
        assert_eq!(stripped.get("response-endpoint"), Some("publisher:10101"));
        assert!(stripped.get("term-length").is_none());
        assert!(stripped.get("reliable").is_none());
        assert!(stripped.get("pub-wnd").is_none());
        assert!(stripped.get("tag").is_none());
    }
}
