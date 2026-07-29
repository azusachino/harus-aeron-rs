//! Bounded dispatch for Archive control requests received from an Aeron subscription.

use crate::codecs::{
    CloseSessionRequest, CodecError, ConnectRequest, ExtendRecordingRequest,
    FindLastMatchingRecordingRequest, ListRecordingRequest, ListRecordingSubscriptionsRequest,
    ListRecordingsForUriRequest, ListRecordingsRequest, MaxRecordedPositionRequest, MessageHeader,
    PurgeRecordingRequest, RecordingPositionRequest, ReplayRequest, StartPositionRequest,
    StartRecordingRequest, StopPositionRequest, StopRecordingRequest,
    StopRecordingSubscriptionRequest, StopReplayRequest, TruncateRecordingRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRequest {
    Connect(ConnectRequest),
    CloseSession(CloseSessionRequest),
    StartRecording(StartRecordingRequest),
    ExtendRecording(ExtendRecordingRequest),
    TruncateRecording(TruncateRecordingRequest),
    PurgeRecording(PurgeRecordingRequest),
    StopRecording(StopRecordingRequest),
    StopRecordingSubscription(StopRecordingSubscriptionRequest),
    Replay(ReplayRequest),
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
    use crate::codecs::{PROTOCOL_SEMANTIC_VERSION, SourceLocation};

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
}
