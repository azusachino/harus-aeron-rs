//! Control mode for UDP channel multi-destination cast (MDC) or response channels.
//!
//! Mirrors `io.aeron.driver.media.ControlMode.java`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ControlMode {
    #[default]
    None,
    Dynamic,
    Manual,
    Response,
}

impl ControlMode {
    pub fn is_multi_destination(&self) -> bool {
        matches!(self, ControlMode::Dynamic | ControlMode::Manual)
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "dynamic" => Some(ControlMode::Dynamic),
            "manual" => Some(ControlMode::Manual),
            "response" => Some(ControlMode::Response),
            _ => None,
        }
    }
}
