//! Command message flyweights for driver control protocol commands.

use agrona::AtomicBuffer;

pub const CORRELATED_MESSAGE_HEADER_LENGTH: usize = 16;
pub const CLIENT_ID_OFFSET: usize = 0;
pub const CORRELATION_ID_OFFSET: usize = 8;

/// Base flyweight for correlated request messages (`client_id`, `correlation_id`).
#[derive(Clone)]
pub struct CorrelatedMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> CorrelatedMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn header_length(&self) -> usize {
        CORRELATED_MESSAGE_HEADER_LENGTH
    }
}

pub const COUNTER_TYPE_ID_OFFSET: usize = 16;
pub const KEY_LENGTH_OFFSET: usize = 20;
pub const KEY_BUFFER_OFFSET: usize = 24;

/// Flyweight for `ADD_COUNTER` / `REMOVE_COUNTER` control messages.
#[derive(Clone)]
pub struct CounterMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> CounterMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn type_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + COUNTER_TYPE_ID_OFFSET)
    }

    pub fn set_type_id(&self, type_id: i32) {
        self.buffer
            .put_i32(self.offset + COUNTER_TYPE_ID_OFFSET, type_id);
    }

    pub fn key_buffer_offset(&self) -> usize {
        KEY_BUFFER_OFFSET
    }

    pub fn key_buffer_length(&self) -> usize {
        self.buffer.get_i32(self.offset + KEY_LENGTH_OFFSET) as usize
    }

    pub fn set_key_buffer(&self, key_bytes: &[u8]) {
        let key_len = key_bytes.len();
        self.buffer
            .put_i32(self.offset + KEY_LENGTH_OFFSET, key_len as i32);
        if key_len > 0 {
            self.buffer
                .put_bytes(self.offset + KEY_BUFFER_OFFSET, key_bytes);
        }
    }

    fn label_length_offset(&self) -> usize {
        KEY_BUFFER_OFFSET + ((self.key_buffer_length() + 3) & !3)
    }

    pub fn label_buffer_offset(&self) -> usize {
        self.label_length_offset() + 4
    }

    pub fn label_buffer_length(&self) -> usize {
        let label_len_offset = self.label_length_offset();
        self.buffer.get_i32(self.offset + label_len_offset) as usize
    }

    pub fn set_label(&self, label: &str) {
        let label_len_offset = self.label_length_offset();
        let label_bytes = label.as_bytes();
        self.buffer
            .put_i32(self.offset + label_len_offset, label_bytes.len() as i32);
        if !label_bytes.is_empty() {
            self.buffer
                .put_bytes(self.offset + label_len_offset + 4, label_bytes);
        }
    }

    pub fn length(&self) -> usize {
        self.label_buffer_offset() + self.label_buffer_length()
    }

    pub fn compute_length(key_length: usize, label_length: usize) -> usize {
        KEY_BUFFER_OFFSET + ((key_length + 3) & !3) + 4 + label_length
    }
}

pub const TOKEN_LENGTH_OFFSET: usize = 16;
pub const TOKEN_BUFFER_OFFSET: usize = 20;

/// Flyweight for `TERMINATE_DRIVER` control messages.
#[derive(Clone)]
pub struct TerminateDriverFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> TerminateDriverFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn token_buffer_offset(&self) -> usize {
        TOKEN_BUFFER_OFFSET
    }

    pub fn token_buffer_length(&self) -> usize {
        self.buffer.get_i32(self.offset + TOKEN_LENGTH_OFFSET) as usize
    }

    pub fn set_token_buffer(&self, token_bytes: &[u8]) {
        let token_len = token_bytes.len();
        self.buffer
            .put_i32(self.offset + TOKEN_LENGTH_OFFSET, token_len as i32);
        if token_len > 0 {
            self.buffer
                .put_bytes(self.offset + TOKEN_BUFFER_OFFSET, token_bytes);
        }
    }

    pub fn length(&self) -> usize {
        TOKEN_BUFFER_OFFSET + self.token_buffer_length()
    }
}

pub const STREAM_ID_OFFSET: usize = 16;
pub const PUB_CHANNEL_LENGTH_OFFSET: usize = 20;
pub const PUB_CHANNEL_OFFSET: usize = 24;

/// Flyweight for `ADD_PUBLICATION` / `ADD_EXCLUSIVE_PUBLICATION`.
#[derive(Clone)]
pub struct PublicationMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> PublicationMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + STREAM_ID_OFFSET)
    }

    pub fn set_stream_id(&self, stream_id: i32) {
        self.buffer
            .put_i32(self.offset + STREAM_ID_OFFSET, stream_id);
    }

    pub fn channel(&self) -> String {
        let len = self.buffer.get_i32(self.offset + PUB_CHANNEL_LENGTH_OFFSET) as usize;
        let mut bytes = vec![0u8; len];
        self.buffer
            .get_bytes(self.offset + PUB_CHANNEL_OFFSET, &mut bytes);
        String::from_utf8_lossy(&bytes).to_string()
    }

    pub fn set_channel(&self, channel: &str) {
        let bytes = channel.as_bytes();
        self.buffer
            .put_i32(self.offset + PUB_CHANNEL_LENGTH_OFFSET, bytes.len() as i32);
        if !bytes.is_empty() {
            self.buffer
                .put_bytes(self.offset + PUB_CHANNEL_OFFSET, bytes);
        }
    }

    pub fn length(&self) -> usize {
        let len = self.buffer.get_i32(self.offset + PUB_CHANNEL_LENGTH_OFFSET) as usize;
        PUB_CHANNEL_OFFSET + len
    }

    pub fn compute_length(channel_len: usize) -> usize {
        PUB_CHANNEL_OFFSET + channel_len
    }
}

pub const REGISTRATION_ID_OFFSET: usize = 16;
pub const REMOVE_PUB_FLAGS_OFFSET: usize = 24;

/// Flyweight for `REMOVE_PUBLICATION`, `REMOVE_SUBSCRIPTION`, `REMOVE_COUNTER`.
#[derive(Clone)]
pub struct RemoveMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> RemoveMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn registration_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + REGISTRATION_ID_OFFSET)
    }

    pub fn set_registration_id(&self, registration_id: i64) {
        self.buffer
            .put_i64(self.offset + REGISTRATION_ID_OFFSET, registration_id);
    }

    pub fn revoke(&self) -> bool {
        (self.buffer.get_i64(self.offset + REMOVE_PUB_FLAGS_OFFSET) & 0x1) != 0
    }

    pub fn set_revoke(&self, revoke: bool) {
        let flag = if revoke { 1i64 } else { 0i64 };
        self.buffer
            .put_i64(self.offset + REMOVE_PUB_FLAGS_OFFSET, flag);
    }

    pub fn length() -> usize {
        24
    }

    pub fn remove_publication_length() -> usize {
        32
    }
}

pub const SUB_REG_CORRELATION_ID_OFFSET: usize = 16;
pub const SUB_STREAM_ID_OFFSET: usize = 24;
pub const SUB_CHANNEL_LENGTH_OFFSET: usize = 28;
pub const SUB_CHANNEL_OFFSET: usize = 32;

/// Flyweight for `ADD_SUBSCRIPTION`.
#[derive(Clone)]
pub struct SubscriptionMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> SubscriptionMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn registration_correlation_id(&self) -> i64 {
        self.buffer
            .get_i64(self.offset + SUB_REG_CORRELATION_ID_OFFSET)
    }

    pub fn set_registration_correlation_id(&self, reg_id: i64) {
        self.buffer
            .put_i64(self.offset + SUB_REG_CORRELATION_ID_OFFSET, reg_id);
    }

    pub fn stream_id(&self) -> i32 {
        self.buffer.get_i32(self.offset + SUB_STREAM_ID_OFFSET)
    }

    pub fn set_stream_id(&self, stream_id: i32) {
        self.buffer
            .put_i32(self.offset + SUB_STREAM_ID_OFFSET, stream_id);
    }

    pub fn channel(&self) -> String {
        let len = self.buffer.get_i32(self.offset + SUB_CHANNEL_LENGTH_OFFSET) as usize;
        let mut bytes = vec![0u8; len];
        self.buffer
            .get_bytes(self.offset + SUB_CHANNEL_OFFSET, &mut bytes);
        String::from_utf8_lossy(&bytes).to_string()
    }

    pub fn set_channel(&self, channel: &str) {
        let bytes = channel.as_bytes();
        self.buffer
            .put_i32(self.offset + SUB_CHANNEL_LENGTH_OFFSET, bytes.len() as i32);
        if !bytes.is_empty() {
            self.buffer
                .put_bytes(self.offset + SUB_CHANNEL_OFFSET, bytes);
        }
    }

    pub fn length(&self) -> usize {
        let len = self.buffer.get_i32(self.offset + SUB_CHANNEL_LENGTH_OFFSET) as usize;
        SUB_CHANNEL_OFFSET + len
    }

    pub fn compute_length(channel_len: usize) -> usize {
        SUB_CHANNEL_OFFSET + channel_len
    }
}

pub const DEST_REG_CORRELATION_ID_OFFSET: usize = 16;
pub const DEST_CHANNEL_LENGTH_OFFSET: usize = 24;
pub const DEST_CHANNEL_OFFSET: usize = 28;

/// Flyweight for `ADD_DESTINATION` / `REMOVE_DESTINATION`.
#[derive(Clone)]
pub struct DestinationMessageFlyweight<'a> {
    buffer: AtomicBuffer<'a>,
    offset: usize,
}

impl<'a> DestinationMessageFlyweight<'a> {
    pub fn new(buffer: AtomicBuffer<'a>, offset: usize) -> Self {
        Self { buffer, offset }
    }

    pub fn client_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CLIENT_ID_OFFSET)
    }

    pub fn set_client_id(&self, client_id: i64) {
        self.buffer
            .put_i64(self.offset + CLIENT_ID_OFFSET, client_id);
    }

    pub fn correlation_id(&self) -> i64 {
        self.buffer.get_i64(self.offset + CORRELATION_ID_OFFSET)
    }

    pub fn set_correlation_id(&self, correlation_id: i64) {
        self.buffer
            .put_i64(self.offset + CORRELATION_ID_OFFSET, correlation_id);
    }

    pub fn registration_correlation_id(&self) -> i64 {
        self.buffer
            .get_i64(self.offset + DEST_REG_CORRELATION_ID_OFFSET)
    }

    pub fn set_registration_correlation_id(&self, reg_id: i64) {
        self.buffer
            .put_i64(self.offset + DEST_REG_CORRELATION_ID_OFFSET, reg_id);
    }

    pub fn channel(&self) -> String {
        let len = self
            .buffer
            .get_i32(self.offset + DEST_CHANNEL_LENGTH_OFFSET) as usize;
        let mut bytes = vec![0u8; len];
        self.buffer
            .get_bytes(self.offset + DEST_CHANNEL_OFFSET, &mut bytes);
        String::from_utf8_lossy(&bytes).to_string()
    }

    pub fn set_channel(&self, channel: &str) {
        let bytes = channel.as_bytes();
        self.buffer
            .put_i32(self.offset + DEST_CHANNEL_LENGTH_OFFSET, bytes.len() as i32);
        if !bytes.is_empty() {
            self.buffer
                .put_bytes(self.offset + DEST_CHANNEL_OFFSET, bytes);
        }
    }

    pub fn length(&self) -> usize {
        let len = self
            .buffer
            .get_i32(self.offset + DEST_CHANNEL_LENGTH_OFFSET) as usize;
        DEST_CHANNEL_OFFSET + len
    }

    pub fn compute_length(channel_len: usize) -> usize {
        DEST_CHANNEL_OFFSET + channel_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_message_flyweight() {
        let mut bytes = vec![0u8; 128];
        let buffer = AtomicBuffer::new(&mut bytes);

        let flyweight = CounterMessageFlyweight::new(buffer, 24);
        flyweight.set_client_id(100);
        flyweight.set_correlation_id(200);
        flyweight.set_type_id(77);

        let key_data = [1u8; 8];
        flyweight.set_key_buffer(&key_data);

        assert_eq!(flyweight.client_id(), 100);
        assert_eq!(flyweight.correlation_id(), 200);
        assert_eq!(flyweight.type_id(), 77);
        assert_eq!(flyweight.key_buffer_length(), 8);
        assert_eq!(flyweight.key_buffer_offset(), KEY_BUFFER_OFFSET);

        flyweight.set_label("test_counter");
        assert_eq!(flyweight.label_buffer_length(), 12);
        assert_eq!(flyweight.label_buffer_offset(), KEY_BUFFER_OFFSET + 8 + 4);
        assert_eq!(flyweight.length(), KEY_BUFFER_OFFSET + 8 + 4 + 12);
    }

    // Generated by test/wire_contracts/CounterVector.java with Aeron 1.54.0.
    #[test]
    fn counter_odd_key_matches_java_vector() {
        let mut bytes = [0u8; 35];
        let counter = CounterMessageFlyweight::new(AtomicBuffer::new(&mut bytes), 0);
        counter.set_client_id(0x0102030405060708);
        counter.set_correlation_id(0x1112131415161718);
        counter.set_type_id(0x21222324);
        counter.set_key_buffer(&[0x31, 0x32, 0x33]);
        counter.set_label("abc");
        assert_eq!(counter.length(), 35);
        assert_eq!(CounterMessageFlyweight::compute_length(3, 3), 35);
        assert_eq!(
            bytes,
            [
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13,
                0x12, 0x11, 0x24, 0x23, 0x22, 0x21, 0x03, 0, 0, 0, 0x31, 0x32, 0x33, 0, 0x03, 0, 0,
                0, 0x61, 0x62, 0x63,
            ]
        );
    }

    #[test]
    fn test_terminate_driver_flyweight() {
        let mut bytes = vec![0u8; 128];
        let buffer = AtomicBuffer::new(&mut bytes);

        let flyweight = TerminateDriverFlyweight::new(buffer, 24);
        flyweight.set_client_id(500);
        flyweight.set_correlation_id(600);

        let token_data = [2u8; 16];
        flyweight.set_token_buffer(&token_data);

        assert_eq!(flyweight.client_id(), 500);
        assert_eq!(flyweight.correlation_id(), 600);
        assert_eq!(flyweight.token_buffer_length(), 16);
        assert_eq!(flyweight.token_buffer_offset(), TOKEN_BUFFER_OFFSET);
        assert_eq!(flyweight.length(), TOKEN_BUFFER_OFFSET + 16);
    }
}
