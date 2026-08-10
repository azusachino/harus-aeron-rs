//! Bounded dispatch for Archive control requests received from an Aeron subscription.

use crate::codecs::{
    BoundedReplayRequest, CloseSessionRequest, CodecError, ConnectRequest, ExtendRecordingRequest,
    ExtendRecordingRequestV2, FindLastMatchingRecordingRequest, ListRecordingRequest,
    ListRecordingSubscriptionsRequest, ListRecordingsForUriRequest, ListRecordingsRequest,
    MaxRecordedPositionRequest, MessageHeader, PurgeRecordingRequest, RecordingPositionRequest,
    ReplayRequest, ReplayTokenRequest, ReplicateRequest2, StartPositionRequest,
    StartRecordingRequest, StartRecordingRequestV2, StopPositionRequest, StopRecordingRequest,
    StopRecordingSubscriptionRequest, StopReplayRequest, TruncateRecordingRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRequest {
    Connect(ConnectRequest),
    CloseSession(CloseSessionRequest),
    StartRecording(StartRecordingRequest),
    StartRecordingV2(StartRecordingRequestV2),
    ExtendRecording(ExtendRecordingRequest),
    ExtendRecordingV2(ExtendRecordingRequestV2),
    TruncateRecording(TruncateRecordingRequest),
    PurgeRecording(PurgeRecordingRequest),
    StopRecording(StopRecordingRequest),
    StopRecordingSubscription(StopRecordingSubscriptionRequest),
    Replay(ReplayRequest),
    BoundedReplay(BoundedReplayRequest),
    Replicate(ReplicateRequest2),
    ReplayToken(ReplayTokenRequest),
    StopReplay(StopReplayRequest),
    ListRecordings(ListRecordingsRequest),
    ListRecordingsForUri(ListRecordingsForUriRequest),
    ListRecording(ListRecordingRequest),
    ListRecordingSubscriptions(ListRecordingSubscriptionsRequest),
    StartPosition(StartPositionRequest),
    RecordingPosition(RecordingPositionRequest),
    StopPosition(StopPositionRequest),
    MaxRecordedPosition(MaxRecordedPositionRequest),
    FindLastMatchingRecording(FindLastMatchingRecordingRequest),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ControlAdapterError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("unsupported Archive control request template: {0}")]
    UnsupportedTemplate(u16),
}

#[derive(Debug, Default)]
pub struct ControlSessionAdapter;

impl ControlSessionAdapter {
    pub fn decode(frame: &[u8]) -> Result<ControlRequest, ControlAdapterError> {
        let header = MessageHeader::decode_from(frame)?;
        match header.template_id {
            ConnectRequest::TEMPLATE_ID => {
                Ok(ControlRequest::Connect(ConnectRequest::decode(frame)?))
            }
            CloseSessionRequest::TEMPLATE_ID => Ok(ControlRequest::CloseSession(
                CloseSessionRequest::decode(frame)?,
            )),
            StartRecordingRequest::TEMPLATE_ID => Ok(ControlRequest::StartRecording(
                StartRecordingRequest::decode(frame)?,
            )),
            ExtendRecordingRequest::TEMPLATE_ID => Ok(ControlRequest::ExtendRecording(
                ExtendRecordingRequest::decode(frame)?,
            )),
            StartRecordingRequestV2::TEMPLATE_ID => Ok(ControlRequest::StartRecordingV2(
                StartRecordingRequestV2::decode(frame)?,
            )),
            ExtendRecordingRequestV2::TEMPLATE_ID => Ok(ControlRequest::ExtendRecordingV2(
                ExtendRecordingRequestV2::decode(frame)?,
            )),
            TruncateRecordingRequest::TEMPLATE_ID => Ok(ControlRequest::TruncateRecording(
                TruncateRecordingRequest::decode(frame)?,
            )),
            PurgeRecordingRequest::TEMPLATE_ID => Ok(ControlRequest::PurgeRecording(
                PurgeRecordingRequest::decode(frame)?,
            )),
            StopRecordingRequest::TEMPLATE_ID => Ok(ControlRequest::StopRecording(
                StopRecordingRequest::decode(frame)?,
            )),
            StopRecordingSubscriptionRequest::TEMPLATE_ID => {
                Ok(ControlRequest::StopRecordingSubscription(
                    StopRecordingSubscriptionRequest::decode(frame)?,
                ))
            }
            ReplayRequest::TEMPLATE_ID => Ok(ControlRequest::Replay(ReplayRequest::decode(frame)?)),
            BoundedReplayRequest::TEMPLATE_ID => Ok(ControlRequest::BoundedReplay(
                BoundedReplayRequest::decode(frame)?,
            )),
            ReplicateRequest2::TEMPLATE_ID => {
                Ok(ControlRequest::Replicate(ReplicateRequest2::decode(frame)?))
            }
            ReplayTokenRequest::TEMPLATE_ID => Ok(ControlRequest::ReplayToken(
                ReplayTokenRequest::decode(frame)?,
            )),
            StopReplayRequest::TEMPLATE_ID => Ok(ControlRequest::StopReplay(
                StopReplayRequest::decode(frame)?,
            )),
            ListRecordingsRequest::TEMPLATE_ID => Ok(ControlRequest::ListRecordings(
                ListRecordingsRequest::decode(frame)?,
            )),
            ListRecordingsForUriRequest::TEMPLATE_ID => Ok(ControlRequest::ListRecordingsForUri(
                ListRecordingsForUriRequest::decode(frame)?,
            )),
            ListRecordingRequest::TEMPLATE_ID => Ok(ControlRequest::ListRecording(
                ListRecordingRequest::decode(frame)?,
            )),
            ListRecordingSubscriptionsRequest::TEMPLATE_ID => {
                Ok(ControlRequest::ListRecordingSubscriptions(
                    ListRecordingSubscriptionsRequest::decode(frame)?,
                ))
            }
            StartPositionRequest::TEMPLATE_ID => Ok(ControlRequest::StartPosition(
                StartPositionRequest::decode(frame)?,
            )),
            RecordingPositionRequest::TEMPLATE_ID => Ok(ControlRequest::RecordingPosition(
                RecordingPositionRequest::decode(frame)?,
            )),
            StopPositionRequest::TEMPLATE_ID => Ok(ControlRequest::StopPosition(
                StopPositionRequest::decode(frame)?,
            )),
            MaxRecordedPositionRequest::TEMPLATE_ID => Ok(ControlRequest::MaxRecordedPosition(
                MaxRecordedPositionRequest::decode(frame)?,
            )),
            FindLastMatchingRecordingRequest::TEMPLATE_ID => {
                Ok(ControlRequest::FindLastMatchingRecording(
                    FindLastMatchingRecordingRequest::decode(frame)?,
                ))
            }
            template_id => Err(ControlAdapterError::UnsupportedTemplate(template_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::{
        PROTOCOL_SEMANTIC_VERSION, ReplayRequest, SourceLocation, StartRecordingRequestV2,
    };

    #[test]
    fn dispatches_by_real_sbe_template_id() {
        let connect = ConnectRequest {
            correlation_id: 1,
            response_stream_id: 101,
            version: PROTOCOL_SEMANTIC_VERSION,
            response_channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ControlSessionAdapter::decode(&connect.encode().unwrap()).unwrap(),
            ControlRequest::Connect(connect)
        );

        let start = StartRecordingRequest {
            control_session_id: 2,
            correlation_id: 3,
            stream_id: 102,
            source_location: SourceLocation::Local,
            channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ControlSessionAdapter::decode(&start.encode().unwrap()).unwrap(),
            ControlRequest::StartRecording(start)
        );

        let start_v2 = StartRecordingRequestV2 {
            control_session_id: 2,
            correlation_id: 4,
            stream_id: 102,
            source_location: SourceLocation::Local,
            auto_stop: true,
            channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ControlSessionAdapter::decode(&start_v2.encode().unwrap()).unwrap(),
            ControlRequest::StartRecordingV2(start_v2)
        );
    }

    #[test]
    fn rejects_unknown_templates_and_truncated_headers() {
        let mut frame = vec![0; MessageHeader::SIZE];
        MessageHeader {
            block_length: 0,
            template_id: 999,
            schema_id: crate::codecs::SBE_SCHEMA_ID,
            version: crate::codecs::SBE_SCHEMA_VERSION,
        }
        .encode_into(&mut frame)
        .unwrap();
        assert_eq!(
            ControlSessionAdapter::decode(&frame),
            Err(ControlAdapterError::UnsupportedTemplate(999))
        );
        assert!(ControlSessionAdapter::decode(&frame[..7]).is_err());
    }

    #[test]
    fn dispatches_replication_request2_with_all_variable_fields() {
        let request = ReplicateRequest2 {
            control_session_id: 928_374,
            correlation_id: 9_382_475,
            src_recording_id: 1_234_234,
            dst_recording_id: 2_532_453_245,
            stop_position: 2_315_345,
            channel_tag_id: 234,
            subscription_tag_id: 235,
            src_control_stream_id: 982_374,
            file_io_max_length: 4096,
            replication_session_id: 17,
            src_control_channel: "src".into(),
            live_destination: "live".into(),
            replication_channel: "replication".into(),
            encoded_credentials: b"some password".to_vec(),
            src_response_channel: "response".into(),
        };

        assert_eq!(
            ControlSessionAdapter::decode(&request.encode().unwrap()).unwrap(),
            ControlRequest::Replicate(request)
        );
    }

    #[test]
    fn dispatches_replay_and_bounded_replay_requests() {
        let replay = ReplayRequest {
            control_session_id: 928_374,
            correlation_id: 9_382_475,
            recording_id: 9_827_345_897,
            position: 982_374,
            length: -1,
            replay_stream_id: 9_832_475,
            file_io_max_length: 4096,
            replay_token: 0,
            replay_channel: "aeron:ipc".into(),
        };
        assert_eq!(
            ControlSessionAdapter::decode(&replay.encode().unwrap()).unwrap(),
            ControlRequest::Replay(replay)
        );

        let bounded = BoundedReplayRequest {
            control_session_id: 928_374,
            correlation_id: 9_382_475,
            recording_id: 9_827_345_897,
            position: 982_374,
            length: -1,
            limit_counter_id: 92_734,
            replay_stream_id: 9_832_475,
            file_io_max_length: 4096,
            replay_token: 0,
            replay_channel: "aeron:ipc?alias=replay".into(),
        };
        assert_eq!(
            ControlSessionAdapter::decode(&bounded.encode().unwrap()).unwrap(),
            ControlRequest::BoundedReplay(bounded)
        );
    }

    #[test]
    fn dispatches_replay_token_request() {
        let request = ReplayTokenRequest {
            control_session_id: 928_374,
            correlation_id: 9_382_475,
            recording_id: 9_827_345_897,
        };

        assert_eq!(
            ControlSessionAdapter::decode(&request.encode().unwrap()).unwrap(),
            ControlRequest::ReplayToken(request)
        );
    }
}
