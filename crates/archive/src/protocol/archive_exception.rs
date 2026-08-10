//! Archive error-code names used in control responses.

pub const GENERIC: i32 = 0;
pub const ACTIVE_LISTING: i32 = 1;
pub const ACTIVE_RECORDING: i32 = 2;
pub const ACTIVE_SUBSCRIPTION: i32 = 3;
pub const UNKNOWN_SUBSCRIPTION: i32 = 4;
pub const UNKNOWN_RECORDING: i32 = 5;
pub const UNKNOWN_REPLAY: i32 = 6;
pub const MAX_REPLAYS: i32 = 7;
pub const MAX_RECORDINGS: i32 = 8;
pub const INVALID_EXTENSION: i32 = 9;
pub const AUTHENTICATION_REJECTED: i32 = 10;
pub const STORAGE_SPACE: i32 = 11;
pub const UNKNOWN_REPLICATION: i32 = 12;
pub const UNAUTHORISED_ACTION: i32 = 13;
pub const REPLICATION_CONNECTION_FAILURE: i32 = 14;
pub const EMPTY_RECORDING: i16 = 15;

pub fn error_code_as_string(error_code: i32) -> String {
    let name = match error_code {
        GENERIC => "GENERIC",
        ACTIVE_LISTING => "ACTIVE_LISTING",
        ACTIVE_RECORDING => "ACTIVE_RECORDING",
        ACTIVE_SUBSCRIPTION => "ACTIVE_SUBSCRIPTION",
        UNKNOWN_SUBSCRIPTION => "UNKNOWN_SUBSCRIPTION",
        UNKNOWN_RECORDING => "UNKNOWN_RECORDING",
        UNKNOWN_REPLAY => "UNKNOWN_REPLAY",
        MAX_REPLAYS => "MAX_REPLAYS",
        MAX_RECORDINGS => "MAX_RECORDINGS",
        INVALID_EXTENSION => "INVALID_EXTENSION",
        AUTHENTICATION_REJECTED => "AUTHENTICATION_REJECTED",
        STORAGE_SPACE => "STORAGE_SPACE",
        UNKNOWN_REPLICATION => "UNKNOWN_REPLICATION",
        UNAUTHORISED_ACTION => "UNAUTHORISED_ACTION",
        REPLICATION_CONNECTION_FAILURE => "REPLICATION_CONNECTION_FAILURE",
        _ => return format!("unknown error code: {error_code}"),
    };
    name.to_owned()
}

#[cfg(test)]
mod tests {
    #[test]
    fn error_code_as_string() {
        for (error_code, expected) in [
            (0, "GENERIC"),
            (1, "ACTIVE_LISTING"),
            (2, "ACTIVE_RECORDING"),
            (3, "ACTIVE_SUBSCRIPTION"),
            (4, "UNKNOWN_SUBSCRIPTION"),
            (5, "UNKNOWN_RECORDING"),
            (6, "UNKNOWN_REPLAY"),
            (7, "MAX_REPLAYS"),
            (8, "MAX_RECORDINGS"),
            (9, "INVALID_EXTENSION"),
            (10, "AUTHENTICATION_REJECTED"),
            (11, "STORAGE_SPACE"),
            (12, "UNKNOWN_REPLICATION"),
            (13, "UNAUTHORISED_ACTION"),
            (14, "REPLICATION_CONNECTION_FAILURE"),
        ] {
            assert_eq!(super::error_code_as_string(error_code), expected);
        }
    }

    #[test]
    fn unknown_error_codes_have_a_stable_fallback() {
        for error_code in [-1, 1_111_111, 54] {
            assert_eq!(
                super::error_code_as_string(error_code),
                format!("unknown error code: {error_code}")
            );
        }
    }
}
