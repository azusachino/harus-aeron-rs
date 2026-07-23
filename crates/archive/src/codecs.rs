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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_validate_control_request_header_size() {
        assert_eq!(ControlRequestHeader::SIZE, 24);
    }
}
