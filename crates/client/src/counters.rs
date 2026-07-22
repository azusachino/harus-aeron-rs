//! Registry of all counter type IDs and system counter IDs used by Aeron.

// System counter IDs
pub const SYSTEM_COUNTER_ID_BYTES_SENT: i32 = 0;
pub const SYSTEM_COUNTER_ID_BYTES_RECEIVED: i32 = 1;
pub const SYSTEM_COUNTER_ID_RECEIVER_PROXY_FAILS: i32 = 2;
pub const SYSTEM_COUNTER_ID_SENDER_PROXY_FAILS: i32 = 3;
pub const SYSTEM_COUNTER_ID_CONDUCTOR_PROXY_FAILS: i32 = 4;
pub const SYSTEM_COUNTER_ID_NAK_MESSAGES_SENT: i32 = 5;
pub const SYSTEM_COUNTER_ID_NAK_MESSAGES_RECEIVED: i32 = 6;
pub const SYSTEM_COUNTER_ID_STATUS_MESSAGES_SENT: i32 = 7;
pub const SYSTEM_COUNTER_ID_STATUS_MESSAGES_RECEIVED: i32 = 8;
pub const SYSTEM_COUNTER_ID_HEARTBEATS_SENT: i32 = 9;
pub const SYSTEM_COUNTER_ID_HEARTBEATS_RECEIVED: i32 = 10;
pub const SYSTEM_COUNTER_ID_RETRANSMITTED_BYTES: i32 = 11;
pub const SYSTEM_COUNTER_ID_FLOW_CONTROL_UNDER_RUNS: i32 = 12;
pub const SYSTEM_COUNTER_ID_FLOW_CONTROL_OVER_RUNS: i32 = 13;
pub const SYSTEM_COUNTER_ID_INVALID_PACKETS: i32 = 14;
pub const SYSTEM_COUNTER_ID_DRIVER_EXCEPTIONS: i32 = 15;
pub const SYSTEM_COUNTER_ID_DATA_LOSS_EVENTS: i32 = 16;
pub const SYSTEM_COUNTER_ID_UNBLOCKED_PUB: i32 = 17;
pub const SYSTEM_COUNTER_ID_UNBLOCKED_COMMANDS: i32 = 18;
pub const SYSTEM_COUNTER_ID_POSSIBLE_TTL_ASYMMETRY: i32 = 19;
pub const SYSTEM_COUNTER_ID_CONTROLLABLE_IDLE_IDLE: i32 = 20;
pub const SYSTEM_COUNTER_ID_LOSS_REPORT_CHANGE: i32 = 21;

// Counter Type IDs
pub const DRIVER_SUBSCRIBER_POSITION_TYPE_ID: i32 = 1;
pub const DRIVER_PUBLISHER_POSITION_TYPE_ID: i32 = 2;
pub const DRIVER_PUBLISHER_LIMIT_TYPE_ID: i32 = 3;
pub const DRIVER_SENDER_POSITION_TYPE_ID: i32 = 4;
pub const DRIVER_RECEIVER_HWM_TYPE_ID: i32 = 5;
pub const DRIVER_RECV_CHANNEL_STATUS_TYPE_ID: i32 = 6;
pub const DRIVER_SEND_CHANNEL_STATUS_TYPE_ID: i32 = 7;
pub const DRIVER_SYSTEM_COUNTER_TYPE_ID: i32 = 8;
pub const DRIVER_PUBLISHER_HEARTBEAT_TYPE_ID: i32 = 9;
pub const DRIVER_SUBSCRIBER_HEARTBEAT_TYPE_ID: i32 = 10;
pub const DRIVER_SENDER_BIND_ADDRESS_TYPE_ID: i32 = 11;
pub const DRIVER_RECEIVER_BIND_ADDRESS_TYPE_ID: i32 = 12;
pub const DRIVER_RECEIVER_DESTINATION_TYPE_ID: i32 = 13;
pub const CLIENT_HEARTBEAT_TYPE_ID: i32 = 14;
pub const PER_IMAGE_TYPE_ID: i32 = 15;
pub const ARCHIVE_CONTROL_SESSION_TYPE_ID: i32 = 100;
pub const ARCHIVE_RECORDING_POSITION_TYPE_ID: i32 = 101;
pub const CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID: i32 = 200;

/// Check if a counter type ID is reserved for core Aeron client/driver counters.
pub fn is_aeron_counter(type_id: i32) -> bool {
    (0..100).contains(&type_id)
}

/// Check if a counter type ID is reserved for Aeron Archive counters.
pub fn is_archive_counter(type_id: i32) -> bool {
    (100..200).contains(&type_id)
}

/// Check if a counter type ID is reserved for Aeron Cluster counters.
pub fn is_cluster_counter(type_id: i32) -> bool {
    (200..300).contains(&type_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_not_have_overlapping_counter_type_ids() {
        let type_ids = [
            DRIVER_SUBSCRIBER_POSITION_TYPE_ID,
            DRIVER_PUBLISHER_POSITION_TYPE_ID,
            DRIVER_PUBLISHER_LIMIT_TYPE_ID,
            DRIVER_SENDER_POSITION_TYPE_ID,
            DRIVER_RECEIVER_HWM_TYPE_ID,
            DRIVER_RECV_CHANNEL_STATUS_TYPE_ID,
            DRIVER_SEND_CHANNEL_STATUS_TYPE_ID,
            DRIVER_SYSTEM_COUNTER_TYPE_ID,
            DRIVER_PUBLISHER_HEARTBEAT_TYPE_ID,
            DRIVER_SUBSCRIBER_HEARTBEAT_TYPE_ID,
            DRIVER_SENDER_BIND_ADDRESS_TYPE_ID,
            DRIVER_RECEIVER_BIND_ADDRESS_TYPE_ID,
            DRIVER_RECEIVER_DESTINATION_TYPE_ID,
            CLIENT_HEARTBEAT_TYPE_ID,
            PER_IMAGE_TYPE_ID,
            ARCHIVE_CONTROL_SESSION_TYPE_ID,
            ARCHIVE_RECORDING_POSITION_TYPE_ID,
            CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
        ];

        let mut seen = std::collections::HashSet::new();
        for id in type_ids {
            assert!(seen.insert(id), "Duplicate type ID found: {}", id);
        }
    }

    #[test]
    fn test_counter_ranges() {
        assert!(is_aeron_counter(1));
        assert!(is_archive_counter(100));
        assert!(is_cluster_counter(200));
    }
}
