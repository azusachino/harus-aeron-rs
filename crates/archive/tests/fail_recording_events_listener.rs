struct FailRecordingEventsListener;

impl FailRecordingEventsListener {
    fn on_start(
        recording_id: i64,
        start_position: i64,
        session_id: i32,
        stream_id: i32,
        channel: &str,
        source_identity: &str,
    ) -> Result<(), String> {
        Err(format!(
            "recordingId={recording_id}, startPosition={start_position}, sessionId={session_id}, streamId={stream_id}, channel={channel}, sourceIdentity={source_identity}"
        ))
    }

    fn on_progress(recording_id: i64, start_position: i64, position: i64) -> Result<(), String> {
        Err(format!(
            "recordingId={recording_id}, startPosition={start_position}, position={position}"
        ))
    }

    fn on_stop(recording_id: i64, start_position: i64, stop_position: i64) -> Result<(), String> {
        Err(format!(
            "recordingId={recording_id}, startPosition={start_position}, stopPosition={stop_position}"
        ))
    }
}

#[test]
fn rejects_start_events_with_all_callback_fields() {
    let error =
        FailRecordingEventsListener::on_start(7, 11, 13, 17, "aeron:ipc", "source").unwrap_err();

    assert_eq!(
        error,
        "recordingId=7, startPosition=11, sessionId=13, streamId=17, channel=aeron:ipc, sourceIdentity=source"
    );
}

#[test]
fn rejects_progress_events_with_positions() {
    assert_eq!(
        FailRecordingEventsListener::on_progress(7, 11, 19).unwrap_err(),
        "recordingId=7, startPosition=11, position=19"
    );
}

#[test]
fn rejects_stop_events_with_positions() {
    assert_eq!(
        FailRecordingEventsListener::on_stop(7, 11, 23).unwrap_err(),
        "recordingId=7, startPosition=11, stopPosition=23"
    );
}
