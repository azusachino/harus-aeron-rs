//! Incremental Archive listing filtered by stream ID and original channel.
//!
//! Mirrors `io.aeron.archive.ListRecordingsForUriSession` while leaving
//! transport publication to the caller.

use crate::catalog::Catalog;
use crate::list_recordings_session::ListRecordingsEvent;
use crate::recording_descriptor::RecordingDescriptor;

const MAX_SCANS_PER_WORK_CYCLE: usize = 64;

pub struct ListRecordingsForUriSession<'a> {
    correlation_id: i64,
    next_recording_id: i64,
    count: usize,
    sent: usize,
    channel_fragment: String,
    stream_id: i32,
    catalog: &'a Catalog,
    done: bool,
}

impl<'a> ListRecordingsForUriSession<'a> {
    pub fn new(
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
        channel_fragment: impl Into<String>,
        stream_id: i32,
        catalog: &'a Catalog,
    ) -> Self {
        Self {
            correlation_id,
            next_recording_id: from_recording_id,
            count: record_count.max(0) as usize,
            sent: 0,
            channel_fragment: channel_fragment.into(),
            stream_id,
            catalog,
            done: record_count <= 0,
        }
    }

    pub fn do_work<F>(&mut self, mut send_descriptor: F) -> Vec<ListRecordingsEvent>
    where
        F: FnMut(i64, &RecordingDescriptor) -> bool,
    {
        if self.done {
            return Vec::new();
        }

        let mut events = Vec::new();
        let mut scans = 0;
        while self.sent < self.count && scans < MAX_SCANS_PER_WORK_CYCLE {
            let Some(descriptor) = self
                .catalog
                .recordings()
                .iter()
                .find(|descriptor| descriptor.recording_id >= self.next_recording_id)
                .cloned()
            else {
                self.done = true;
                events.push(ListRecordingsEvent::RecordingUnknown {
                    correlation_id: self.correlation_id,
                    recording_id: self.next_recording_id,
                });
                break;
            };

            scans += 1;
            let next_recording_id = descriptor.recording_id.saturating_add(1);
            if self
                .catalog
                .get_valid_recording(descriptor.recording_id)
                .is_none()
                || descriptor.stream_id != self.stream_id
                || !descriptor.original_channel.contains(&self.channel_fragment)
            {
                self.next_recording_id = next_recording_id;
                continue;
            }

            if !send_descriptor(self.correlation_id, &descriptor) {
                break;
            }

            self.next_recording_id = next_recording_id;
            self.sent += 1;
            events.push(ListRecordingsEvent::Descriptor {
                correlation_id: self.correlation_id,
                descriptor,
            });
        }

        if self.sent == self.count {
            self.done = true;
        }
        events
    }

    pub fn abort(&mut self) {
        self.done = true;
    }

    pub fn close(&mut self) {
        self.abort();
    }

    pub fn is_done(&self) -> bool {
        self.done
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_with_matching_recordings() -> Catalog {
        let mut catalog = Catalog::new();
        for (recording_id, stream_id, channel) in [
            (0, 1, "localhost"),
            (1, 1, "channelA"),
            (2, 1, "localhost"),
            (3, 1, "channelB"),
            (4, 1, "localhost"),
        ] {
            catalog.add_recording(RecordingDescriptor::new(
                0,
                0,
                recording_id,
                0,
                0,
                0,
                0,
                0,
                128 * 1024 * 1024,
                4096,
                1024,
                0,
                stream_id,
                channel.into(),
                format!("{channel}?tag=f"),
                "source".into(),
            ));
        }
        catalog
    }

    fn recording_ids(events: Vec<ListRecordingsEvent>) -> Vec<i64> {
        events
            .into_iter()
            .filter_map(|event| match event {
                ListRecordingsEvent::Descriptor { descriptor, .. } => Some(descriptor.recording_id),
                ListRecordingsEvent::RecordingUnknown { .. } => None,
            })
            .collect()
    }

    #[test]
    fn sends_all_matching_descriptors() {
        let catalog = catalog_with_matching_recordings();
        let mut session = ListRecordingsForUriSession::new(1, 0, 3, "localhost", 1, &catalog);

        let events = session.do_work(|_, _| true);

        assert_eq!(recording_ids(events), [0, 2, 4]);
        assert!(session.is_done());
    }

    #[test]
    fn starts_at_id_and_sends_requested_count() {
        let catalog = catalog_with_matching_recordings();
        let mut session = ListRecordingsForUriSession::new(1, 1, 2, "localhost", 1, &catalog);

        let events = session.do_work(|_, _| true);

        assert_eq!(recording_ids(events), [2, 4]);
        assert!(session.is_done());
    }

    #[test]
    fn reports_unknown_after_matching_descriptors_are_exhausted() {
        let catalog = catalog_with_matching_recordings();
        let mut session = ListRecordingsForUriSession::new(1, 1, 5, "localhost", 1, &catalog);

        let events = session.do_work(|_, _| true);

        assert_eq!(recording_ids(events.clone()), [2, 4]);
        assert!(matches!(
            events.last(),
            Some(ListRecordingsEvent::RecordingUnknown {
                correlation_id: 1,
                recording_id: 5
            })
        ));
        assert!(session.is_done());
    }

    #[test]
    fn reports_unknown_when_channel_does_not_match() {
        let catalog = catalog_with_matching_recordings();
        let mut session = ListRecordingsForUriSession::new(1, 1, 3, "notChannel", 1, &catalog);

        let events = session.do_work(|_, _| true);

        assert_eq!(
            events,
            vec![ListRecordingsEvent::RecordingUnknown {
                correlation_id: 1,
                recording_id: 5,
            }]
        );
        assert!(session.is_done());
    }

    #[test]
    fn reports_unknown_when_start_is_after_catalog() {
        let catalog = catalog_with_matching_recordings();
        let mut session = ListRecordingsForUriSession::new(1, 5, 3, "localhost", 1, &catalog);

        let events = session.do_work(|_, _| true);

        assert_eq!(
            events,
            vec![ListRecordingsEvent::RecordingUnknown {
                correlation_id: 1,
                recording_id: 5,
            }]
        );
    }

    #[test]
    fn retries_a_matching_descriptor_when_publication_fails() {
        let catalog = catalog_with_matching_recordings();
        let mut session = ListRecordingsForUriSession::new(1, 0, 1, "localhost", 1, &catalog);

        assert!(session.do_work(|_, _| false).is_empty());
        assert!(!session.is_done());
        assert_eq!(recording_ids(session.do_work(|_, _| true)), [0]);
        assert!(session.is_done());
    }
}
