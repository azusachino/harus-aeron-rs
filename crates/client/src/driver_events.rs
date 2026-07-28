//! Media-driver event codecs and event identifiers used by [`ClientConductor`](crate::conductor::ClientConductor).

use agrona::AtomicBuffer;

pub const ON_ERROR: i32 = 0x0F01;
pub const ON_AVAILABLE_IMAGE: i32 = 0x0F02;
pub const ON_PUBLICATION_READY: i32 = 0x0F03;
pub const ON_OPERATION_SUCCESS: i32 = 0x0F04;
pub const ON_UNAVAILABLE_IMAGE: i32 = 0x0F05;
pub const ON_SUBSCRIPTION_READY: i32 = 0x0F07;
pub const ON_CLIENT_TIMEOUT: i32 = 0x0F0A;
pub const ON_EXCLUSIVE_PUBLICATION_READY: i32 = 0x0F06;
pub const ON_COUNTER_READY: i32 = 0x0F08;
pub const ON_UNAVAILABLE_COUNTER: i32 = 0x0F09;
pub const ON_STATIC_COUNTER: i32 = 0x0F0B;
pub const ON_PUBLICATION_ERROR: i32 = 0x0F0C;
pub const ON_NEXT_AVAILABLE_SESSION_ID: i32 = 0x0F0D;

pub const CONTROL_PROTOCOL_MAJOR_VERSION: u8 = 1;
pub const CONTROL_PROTOCOL_MINOR_VERSION: u8 = 0;
pub const CONTROL_PROTOCOL_PATCH_VERSION: u8 = 0;
pub const CONTROL_PROTOCOL_SEMANTIC_VERSION: i32 = ((CONTROL_PROTOCOL_MAJOR_VERSION as i32) << 24)
    | ((CONTROL_PROTOCOL_MINOR_VERSION as i32) << 16)
    | ((CONTROL_PROTOCOL_PATCH_VERSION as i32) << 8);

const I64: usize = 8;
const I32: usize = 4;

/// Response flyweight for `ON_PUBLICATION_READY` and `ON_EXCLUSIVE_PUBLICATION_READY`.
#[derive(Clone)]
pub struct PublicationBuffersReadyFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> PublicationBuffersReadyFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn set_correlation_id(&self, value: i64) {
        self.buffer.put_i64(self.offset, value);
    }
    pub fn registration_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + 8)
    }
    pub fn set_registration_id(&self, value: i64) {
        self.buffer.put_i64(self.offset + 8, value);
    }
    pub fn session_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 16)
    }
    pub fn set_session_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 16, value);
    }
    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 20)
    }
    pub fn set_stream_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 20, value);
    }
    pub fn publication_limit_counter_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 24)
    }
    pub fn set_publication_limit_counter_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 24, value);
    }
    pub fn channel_status_counter_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 28)
    }
    pub fn set_channel_status_counter_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 28, value);
    }
    pub fn log_file_name(&self) -> crate::error::Result<String> {
        get_string(
            &self.buffer,
            self.offset,
            32,
            self.buffer.capacity().saturating_sub(self.offset),
        )
    }
    pub fn set_log_file_name(&self, value: &str) {
        put_string(&self.buffer, self.offset, 32, value);
    }
}

/// Response flyweight for `ON_SUBSCRIPTION_READY`.
#[derive(Clone)]
pub struct SubscriptionReadyFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> SubscriptionReadyFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn set_correlation_id(&self, value: i64) {
        self.buffer.put_i64(self.offset, value);
    }
    pub fn channel_status_counter_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 8)
    }
    pub fn set_channel_status_counter_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 8, value);
    }
}

/// Response flyweight for `ON_AVAILABLE_IMAGE`.
#[derive(Clone)]
pub struct ImageBuffersReadyFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> ImageBuffersReadyFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn set_correlation_id(&self, value: i64) {
        self.buffer.put_i64(self.offset, value);
    }
    pub fn session_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 8)
    }
    pub fn set_session_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 8, value);
    }
    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 12)
    }
    pub fn set_stream_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 12, value);
    }
    pub fn subscription_registration_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + 16)
    }
    pub fn set_subscription_registration_id(&self, value: i64) {
        self.buffer.put_i64(self.offset + 16, value);
    }
    pub fn subscriber_position_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 24)
    }
    pub fn set_subscriber_position_id(&self, value: i32) {
        self.buffer.put_i32(self.offset + 24, value);
    }
    pub fn log_file_name(&self) -> crate::error::Result<String> {
        get_string(
            &self.buffer,
            self.offset,
            28,
            self.buffer.capacity().saturating_sub(self.offset),
        )
    }
    pub fn set_log_file_name(&self, value: &str) {
        put_string(&self.buffer, self.offset, 28, value);
    }
    pub fn source_identity(&self) -> crate::error::Result<String> {
        let name = self.log_file_name()?;
        let source_offset = 32 + align4(name.len());
        get_string(
            &self.buffer,
            self.offset,
            source_offset,
            self.buffer.capacity().saturating_sub(self.offset),
        )
    }
    pub fn set_source_identity(&self, value: &str) {
        let name = self.log_file_name().unwrap_or_default();
        put_string(&self.buffer, self.offset, 32 + align4(name.len()), value);
    }
}

#[derive(Clone)]
pub struct ErrorResponseFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> ErrorResponseFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn offending_command_correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn set_offending_command_correlation_id(&self, value: i64) {
        self.buffer.put_i64(self.offset, value);
    }
    pub fn error_code(&self) -> i32 {
        self.buffer.get_i32(self.offset + 8)
    }
    pub fn set_error_code(&self, value: i32) {
        self.buffer.put_i32(self.offset + 8, value);
    }
    pub fn error_message(&self) -> crate::error::Result<String> {
        get_string(
            &self.buffer,
            self.offset,
            12,
            self.buffer.capacity().saturating_sub(self.offset),
        )
    }
    pub fn set_error_message(&self, value: &str) {
        put_string(&self.buffer, self.offset, 12, value);
    }
}

#[derive(Clone)]
pub struct OperationSucceededFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> OperationSucceededFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn set_correlation_id(&self, value: i64) {
        self.buffer.put_i64(self.offset, value);
    }
}

#[derive(Clone)]
pub struct ClientTimeoutFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> ClientTimeoutFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn set_client_id(&self, value: i64) {
        self.buffer.put_i64(self.offset, value);
    }
}

#[derive(Clone)]
pub struct NextAvailableSessionIdFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> NextAvailableSessionIdFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn next_session_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 8)
    }
}

/// Shared response flyweight for counter available/unavailable notifications.
#[derive(Clone)]
pub struct CounterUpdateFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> CounterUpdateFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn counter_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + 8)
    }
}

/// Flyweight for the correlation and subscription id in an unavailable-image event.
#[derive(Clone)]
pub struct ImageMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}
impl<'a> ImageMessageFlyweight<'a> {
    pub fn wrap(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }
    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset)
    }
    pub fn subscription_registration_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + 8)
    }
}

fn put_string(buffer: &AtomicBuffer, offset: usize, field: usize, value: &str) {
    buffer.put_i32(offset + field, value.len() as i32);
    buffer.put_bytes(offset + field + I32, value.as_bytes());
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverEvent {
    PublicationReady {
        exclusive: bool,
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
    NextAvailableSessionId {
        correlation_id: i64,
        session_id: i32,
    },
    CounterReady {
        correlation_id: i64,
        counter_id: i32,
    },
    CounterUnavailable {
        correlation_id: i64,
        counter_id: i32,
    },
}

/// Demultiplexes the media driver's broadcast stream into typed client events.
pub struct DriverEventsAdapter<'a> {
    receiver: agrona::BroadcastReceiver<'a>,
}

impl<'a> DriverEventsAdapter<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> crate::error::Result<Self> {
        Ok(Self {
            receiver: agrona::BroadcastReceiver::new(buffer)?,
        })
    }

    pub fn receive(&mut self) -> crate::error::Result<Vec<DriverEvent>> {
        let mut events = Vec::new();
        loop {
            let mut event = None;
            if !self
                .receiver
                .receive_next(|message_type, buffer, offset, length| {
                    event = Some(decode(message_type, buffer, offset, length));
                })?
            {
                break;
            }
            events.push(event.ok_or_else(|| {
                crate::error::AeronError::IllegalState("driver event missing payload".into())
            })??);
        }
        Ok(events)
    }
}

pub fn decode(
    message_type: i32,
    buffer: &AtomicBuffer,
    offset: usize,
    length: usize,
) -> crate::error::Result<DriverEvent> {
    let event = match message_type {
        ON_PUBLICATION_READY | ON_EXCLUSIVE_PUBLICATION_READY => DriverEvent::PublicationReady {
            exclusive: message_type == ON_EXCLUSIVE_PUBLICATION_READY,
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
        ON_NEXT_AVAILABLE_SESSION_ID => DriverEvent::NextAvailableSessionId {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            session_id: get_i32(buffer, offset, I64, length)?,
        },
        ON_COUNTER_READY | ON_STATIC_COUNTER => DriverEvent::CounterReady {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            counter_id: get_i32(buffer, offset, I64, length)?,
        },
        ON_UNAVAILABLE_COUNTER => DriverEvent::CounterUnavailable {
            correlation_id: get_i64(buffer, offset, 0, length)?,
            counter_id: get_i32(buffer, offset, I64, length)?,
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
