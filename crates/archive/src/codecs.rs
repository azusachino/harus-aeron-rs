//! Archive control protocol SBE / binary codecs.
//!
//! Mirrors `io.aeron.archive.codecs`.

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRequestType {
    Connect = 1,
    StartRecording = 2,
    StopRecording = 3,
    Replay = 4,
    ListRecordings = 5,
    CloseSession = 6,
    ListRecordingsForUri = 7,
    ExtendRecording = 8,
    StopReplay = 9,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlResponseCode {
    Ok = 0,
    Error = 1,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingSignal {
    Start = 0,
    Block = 1,
    Stop = 2,
    Sync = 3,
    Merge = 4,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ControlRequestHeader {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub template_id: i32,
    pub schema_id: i16,
    pub version: i16,
}

impl ControlRequestHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

const _: () = assert!(std::mem::size_of::<ControlRequestHeader>() == 24);

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ControlResponseHeader {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub relevant_id: i64,
    pub code: i32,
    pub _pad: i32,
}

impl ControlResponseHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

const _: () = assert!(std::mem::size_of::<ControlResponseHeader>() == 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_validate_codecs_header_sizes() {
        assert_eq!(ControlRequestHeader::SIZE, 24);
        assert_eq!(ControlResponseHeader::SIZE, 32);
    }
}
