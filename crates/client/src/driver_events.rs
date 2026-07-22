//! Media-driver event codecs and event identifiers used by [`ClientConductor`](crate::conductor::ClientConductor).

use agrona::AtomicBuffer;

pub const ON_ERROR: i32 = 0x0F01;
pub const ON_AVAILABLE_IMAGE: i32 = 0x0F02;
pub const ON_PUBLICATION_READY: i32 = 0x0F03;
pub const ON_OPERATION_SUCCESS: i32 = 0x0F04;
pub const ON_UNAVAILABLE_IMAGE: i32 = 0x0F05;
pub const ON_SUBSCRIPTION_READY: i32 = 0x0F07;
pub const ON_CLIENT_TIMEOUT: i32 = 0x0F0A;

const I64: usize = 8;
const I32: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverEvent {
    PublicationReady {
        correlation_id: i64,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        publication_limit_counter_id: i32,
        channel_status_counter_id: i32,
        log_file_name: String,
    },
    SubscriptionReady {
        correlation_id: i64,
        channel_status_counter_id: i32,
    },
    AvailableImage {
        correlation_id: i64,
        session_id: i32,
        stream_id: i32,
        subscription_registration_id: i64,
        subscriber_position_id: i32,
        log_file_name: String,
        source_identity: String,
    },
    UnavailableImage {
        correlation_id: i64,
        subscription_registration_id: i64,
    },
    OperationSucceeded {
        correlation_id: i64,
    },
    Error {
        correlation_id: i64,
        code: i32,
        message: String,
    },
    ClientTimeout {
        client_id: i64,
    },
}

pub fn decode(
    message_type: i32,
    buffer: &AtomicBuffer,
    offset: usize,
    length: usize,
) -> crate::error::Result<DriverEvent> {
    let event = match message_type {
        ON_PUBLICATION_READY => DriverEvent::PublicationReady {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            registration_id: get_i64(buffer, offset, I64, length)?,
            session_id: get_i32(buffer, offset, I64 * 2, length)?,
            stream_id: get_i32(buffer, offset, I64 * 2 + I32, length)?,
            publication_limit_counter_id: get_i32(buffer, offset, I64 * 2 + I32 * 2, length)?,
            channel_status_counter_id: get_i32(buffer, offset, I64 * 2 + I32 * 3, length)?,
            log_file_name: get_string(buffer, offset, I64 * 2 + I32 * 4, length)?,
        },
        ON_SUBSCRIPTION_READY => DriverEvent::SubscriptionReady {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            channel_status_counter_id: get_i32(buffer, offset, I64, length)?,
        },
        ON_AVAILABLE_IMAGE => {
            let log_offset = I64 + I32 * 2 + I64 + I32;
            let log_file_name = get_string(buffer, offset, log_offset, length)?;
            let source_offset = log_offset + I32 + align4(log_file_name.len());
            DriverEvent::AvailableImage {
                correlation_id: get_i64(buffer, offset, 0, length)?,
                session_id: get_i32(buffer, offset, I64, length)?,
                stream_id: get_i32(buffer, offset, I64 + I32, length)?,
                subscription_registration_id: get_i64(buffer, offset, I64 + I32 * 2, length)?,
                subscriber_position_id: get_i32(buffer, offset, I64 * 2 + I32 * 2, length)?,
                log_file_name,
                source_identity: get_string(buffer, offset, source_offset, length)?,
            }
        }
        ON_UNAVAILABLE_IMAGE => DriverEvent::UnavailableImage {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            subscription_registration_id: get_i64(buffer, offset, I64, length)?,
        },
        ON_OPERATION_SUCCESS => DriverEvent::OperationSucceeded {
            correlation_id: get_i64(buffer, offset, 0, length)?,
        },
        ON_ERROR => DriverEvent::Error {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            code: get_i32(buffer, offset, I64, length)?,
            message: get_string(buffer, offset, I64 + I32, length)?,
        },
        ON_CLIENT_TIMEOUT => DriverEvent::ClientTimeout {
            client_id: get_i64(buffer, offset, 0, length)?,
        },
        _ => {
            return Err(crate::error::AeronError::IllegalArgument(format!(
                "unknown driver event type {message_type}"
            )));
        }
    };
    Ok(event)
}

fn get_i64(
    buffer: &AtomicBuffer,
    offset: usize,
    field: usize,
    length: usize,
) -> crate::error::Result<i64> {
    if field + I64 > length {
        return Err(crate::error::AeronError::IllegalArgument(
            "truncated driver event".into(),
        ));
    }
    Ok(buffer.get_i64(offset + field))
}

fn get_i32(
    buffer: &AtomicBuffer,
    offset: usize,
    field: usize,
    length: usize,
) -> crate::error::Result<i32> {
    if field + I32 > length {
        return Err(crate::error::AeronError::IllegalArgument(
            "truncated driver event".into(),
        ));
    }
    Ok(buffer.get_i32(offset + field))
}

fn get_string(
    buffer: &AtomicBuffer,
    offset: usize,
    field: usize,
    length: usize,
) -> crate::error::Result<String> {
    if field + I32 > length {
        return Err(crate::error::AeronError::IllegalArgument(
            "truncated driver event string".into(),
        ));
    }
    let string_length = buffer.get_i32(offset + field);
    if string_length < 0 || field + I32 + string_length as usize > length {
        return Err(crate::error::AeronError::IllegalArgument(
            "invalid driver event string length".into(),
        ));
    }
    let mut bytes = vec![0u8; string_length as usize];
    buffer.try_get_bytes(offset + field + I32, &mut bytes)?;
    String::from_utf8(bytes).map_err(|_| {
        crate::error::AeronError::IllegalArgument("driver event string is not UTF-8".into())
    })
}

fn align4(value: usize) -> usize {
    (value + 3) & !3
}

#[cfg(test)]
mod tests {
    use super::*;
    use agrona::AtomicBuffer;

    #[test]
    fn decodes_subscription_ready() {
        let mut bytes = [0u8; 12];
        let buffer = AtomicBuffer::new(&mut bytes);
        buffer.put_i64(0, 42);
        buffer.put_i32(8, 7);
        assert_eq!(
            decode(ON_SUBSCRIPTION_READY, &buffer, 0, 12).unwrap(),
            DriverEvent::SubscriptionReady {
                correlation_id: 42,
                channel_status_counter_id: 7
            }
        );
    }

    #[test]
    fn rejects_truncated_event() {
        let mut bytes = [0u8; 8];
        let buffer = AtomicBuffer::new(&mut bytes);
        assert!(decode(ON_SUBSCRIPTION_READY, &buffer, 0, 8).is_err());
    }
}
