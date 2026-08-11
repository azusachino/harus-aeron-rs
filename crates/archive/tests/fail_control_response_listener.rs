use aeron_archive::codecs::{ControlResponse, ControlResponseCode};
use aeron_archive::recording_descriptor::RecordingDescriptor;

const NULL_VALUE: i64 = -1;

struct FailControlResponseListener;

impl FailControlResponseListener {
    fn on_response(response: &ControlResponse) -> Result<(), String> {
        if response.correlation_id == NULL_VALUE {
            return Ok(());
        }

        Err(format!(
            "controlSessionId={}, correlationId={}, relevantId={}, responseCode={:?}, errorMessage={}",
            response.control_session_id,
            response.correlation_id,
            response.relevant_id,
            response.code,
            response.error_message,
        ))
    }

    fn on_recording_descriptor(descriptor: &RecordingDescriptor) -> Result<(), String> {
        Err(format!(
            "controlSessionId={}, correlationId={}, recordingId={}, startTimestamp={}, stopTimestamp={}, startPosition={}, stopPosition={}, initialTermId={}, segmentFileLength={}, termBufferLength={}, mtuLength={}, sessionId={}, streamId={}, strippedChannel={}, originalChannel={}, sourceIdentity={}",
            descriptor.control_session_id,
            descriptor.correlation_id,
            descriptor.recording_id,
            descriptor.start_timestamp,
            descriptor.stop_timestamp,
            descriptor.start_position,
            descriptor.stop_position,
            descriptor.initial_term_id,
            descriptor.segment_file_length,
            descriptor.term_buffer_length,
            descriptor.mtu_length,
            descriptor.session_id,
            descriptor.stream_id,
            descriptor.stripped_channel,
            descriptor.original_channel,
            descriptor.source_identity,
        ))
    }
}

fn response(correlation_id: i64) -> ControlResponse {
    ControlResponse {
        control_session_id: 7,
        correlation_id,
        relevant_id: 11,
        code: ControlResponseCode::Error,
        version: 1,
        error_message: "unexpected response".to_string(),
    }
}

fn descriptor() -> RecordingDescriptor {
    RecordingDescriptor::new(
        7,
        13,
        17,
        19,
        23,
        29,
        31,
        37,
        64 * 1024,
        32 * 1024,
        1408,
        41,
        43,
        "aeron:ipc".to_string(),
        "aeron:ipc?endpoint=localhost:40123".to_string(),
        "source".to_string(),
    )
}

#[test]
fn allows_only_null_correlation_responses() {
    assert!(FailControlResponseListener::on_response(&response(NULL_VALUE)).is_ok());

    let error = FailControlResponseListener::on_response(&response(5)).unwrap_err();
    assert!(error.contains("controlSessionId=7"));
    assert!(error.contains("correlationId=5"));
    assert!(error.contains("unexpected response"));
}

#[test]
fn rejects_recording_descriptors_with_full_diagnostics() {
    let error = FailControlResponseListener::on_recording_descriptor(&descriptor()).unwrap_err();

    assert!(error.contains("recordingId=17"));
    assert!(error.contains("startPosition=19"));
    assert!(error.contains("streamId=43"));
    assert!(error.contains("originalChannel=aeron:ipc?endpoint=localhost:40123"));
}
