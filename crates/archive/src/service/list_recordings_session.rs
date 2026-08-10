//! Incremental catalog listing for Archive control responses.

use crate::catalog::Catalog;
use crate::recording_descriptor::RecordingDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListRecordingsEvent {
    Descriptor {
        correlation_id: i64,
        descriptor: RecordingDescriptor,
    },
    RecordingUnknown {
        correlation_id: i64,
        recording_id: i64,
    },
}

pub struct ListRecordingsSession<'a> {
    correlation_id: i64,
    next_recording_id: i64,
    remaining: usize,
    catalog: &'a Catalog,
    done: bool,
}

impl<'a> ListRecordingsSession<'a> {
    pub fn new(
        correlation_id: i64,
        from_recording_id: i64,
        record_count: i32,
        catalog: &'a Catalog,
    ) -> Self {
        Self {
            correlation_id,
            next_recording_id: from_recording_id,
            remaining: record_count.max(0) as usize,
            catalog,
            done: record_count <= 0,
        }
    }

    pub fn do_work(&mut self) -> Vec<ListRecordingsEvent> {
        if self.done {
            return Vec::new();
        }

        let mut events = Vec::with_capacity(self.remaining);
        while self.remaining > 0 {
            let recording_id = self.next_recording_id;
            let Some(descriptor) = self.catalog.get_recording(recording_id).cloned() else {
                events.push(ListRecordingsEvent::RecordingUnknown {
                    correlation_id: self.correlation_id,
                    recording_id,
                });
                self.done = true;
                break;
            };

            events.push(ListRecordingsEvent::Descriptor {
                correlation_id: self.correlation_id,
                descriptor,
            });
            self.next_recording_id += 1;
            self.remaining -= 1;
        }

        if self.remaining == 0 {
            self.done = true;
        }
        events
    }

    pub fn abort(&mut self) {
        self.done = true;
    }

    pub fn is_done(&self) -> bool {
        self.done
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn sends_all_descriptors() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingsSession::new(7, 1, 3, &catalog);

        let events = session.do_work();

        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|event| matches!(
            event,
            ListRecordingsEvent::Descriptor {
                correlation_id: 7,
                ..
            }
        )));
        assert!(session.is_done());
    }

    #[test]
    fn sends_requested_range() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingsSession::new(7, 1, 2, &catalog);

        let events = session.do_work();

        let ids: Vec<_> = events
            .into_iter()
            .map(|event| match event {
                ListRecordingsEvent::Descriptor { descriptor, .. } => descriptor.recording_id,
                ListRecordingsEvent::RecordingUnknown { .. } => -1,
            })
            .collect();
        assert_eq!(ids, [1, 2]);
    }

    #[test]
    fn reports_unknown_after_available_descriptors() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingsSession::new(7, 2, 3, &catalog);

        let events = session.do_work();

        assert!(matches!(
            events.last(),
            Some(ListRecordingsEvent::RecordingUnknown {
                correlation_id: 7,
                recording_id: 4
            })
        ));
        assert_eq!(events.len(), 3);
        assert!(session.is_done());
    }

    #[test]
    fn reports_unknown_on_first_missing_recording() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingsSession::new(7, 4, 3, &catalog);

        let events = session.do_work();

        assert_eq!(
            events,
            vec![ListRecordingsEvent::RecordingUnknown {
                correlation_id: 7,
                recording_id: 4,
            }]
        );
        assert!(session.is_done());
    }

    #[test]
    fn abort_stops_listing_without_emitting_events() {
        let catalog = catalog_with_three_recordings();
        let mut session = ListRecordingsSession::new(7, 1, 3, &catalog);
        session.abort();

        assert!(session.do_work().is_empty());
        assert!(session.is_done());
    }
}
