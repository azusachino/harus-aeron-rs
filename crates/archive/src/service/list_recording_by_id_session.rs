//! Retryable Archive listing for one recording ID.
//!
//! Mirrors `io.aeron.archive.ListRecordingByIdSession` while leaving transport
//! publication to the caller.

use crate::catalog::Catalog;
use crate::recording_descriptor::RecordingDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListRecordingByIdEvent {
    Descriptor {
        correlation_id: i64,
        descriptor: RecordingDescriptor,
    },
    RecordingUnknown {
        correlation_id: i64,
        recording_id: i64,
    },
}

pub struct ListRecordingByIdSession<'a> {
    correlation_id: i64,
    recording_id: i64,
    catalog: &'a Catalog,
    done: bool,
}

impl<'a> ListRecordingByIdSession<'a> {
    pub fn new(correlation_id: i64, recording_id: i64, catalog: &'a Catalog) -> Self {
        Self {
            correlation_id,
            recording_id,
            catalog,
            done: false,
        }
    }

    pub fn do_work<F>(&mut self, mut send_descriptor: F) -> Vec<ListRecordingByIdEvent>
    where
        F: FnMut(i64, &RecordingDescriptor) -> bool,
    {
        if self.done {
            return Vec::new();
        }

        let Some(descriptor) = self.catalog.get_valid_recording(self.recording_id) else {
            self.done = true;
            return vec![ListRecordingByIdEvent::RecordingUnknown {
                correlation_id: self.correlation_id,
                recording_id: self.recording_id,
            }];
        };

        if send_descriptor(self.correlation_id, descriptor) {
            self.done = true;
            vec![ListRecordingByIdEvent::Descriptor {
                correlation_id: self.correlation_id,
                descriptor: descriptor.clone(),
            }]
        } else {
            Vec::new()
        }
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
    use crate::catalog::{Catalog, RecordingState};

    fn catalog_with_three_recordings() -> Catalog {
        let mut catalog = Catalog::new();
        for (recording_id, stream_id) in [(1, 1), (2, 2), (3, 3)] {
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
                format!("channel{recording_id}"),
                format!("channel{recording_id}?tag=f"),
                format!("source{recording_id}"),
            ));
        }
        catalog
    }

    #[test]
    fn sends_descriptor_for_requested_id() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingByIdSession::new(7, 2, &catalog);

        let events = session.do_work(|correlation_id, descriptor| {
            assert_eq!(correlation_id, 7);
            assert_eq!(descriptor.recording_id, 2);
            true
        });

        assert!(matches!(
            events.as_slice(),
            [ListRecordingByIdEvent::Descriptor { correlation_id: 7, descriptor }]
                if descriptor.recording_id == 2
        ));
        assert!(session.is_done());
    }

    #[test]
    fn sends_unknown_for_missing_or_invalid_id() {
        let catalog = catalog_with_three_recordings();
        let mut missing = ListRecordingByIdSession::new(7, 99, &catalog);
        assert_eq!(
            missing.do_work(|_, _| true),
            vec![ListRecordingByIdEvent::RecordingUnknown {
                correlation_id: 7,
                recording_id: 99,
            }]
        );

        assert!(catalog.change_state(2, RecordingState::Invalid));
        let mut invalid = ListRecordingByIdSession::new(8, 2, &catalog);
        assert_eq!(
            invalid.do_work(|_, _| true),
            vec![ListRecordingByIdEvent::RecordingUnknown {
                correlation_id: 8,
                recording_id: 2,
            }]
        );
    }

    #[test]
    fn retries_until_descriptor_send_succeeds() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingByIdSession::new(7, 3, &catalog);
        let mut attempts = 0;

        while !session.is_done() {
            let events = session.do_work(|_, _| {
                attempts += 1;
                attempts == 4
            });
            if attempts < 4 {
                assert!(events.is_empty());
            }
        }

        assert_eq!(attempts, 4);
        assert!(matches!(session.do_work(|_, _| true).as_slice(), []));
    }

    #[test]
    fn reports_unknown_if_invalidated_between_retries() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingByIdSession::new(7, 3, &catalog);

        assert!(session.do_work(|_, _| false).is_empty());
        assert!(!session.is_done());
        assert!(catalog.change_state(3, RecordingState::Invalid));

        assert_eq!(
            session.do_work(|_, _| true),
            vec![ListRecordingByIdEvent::RecordingUnknown {
                correlation_id: 7,
                recording_id: 3,
            }]
        );
        assert!(session.is_done());
    }

    #[test]
    fn close_stops_the_session() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingByIdSession::new(7, 1, &catalog);
        session.close();

        assert!(session.is_done());
        assert!(session.do_work(|_, _| true).is_empty());
    }
}
