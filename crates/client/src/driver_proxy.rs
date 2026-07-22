//! Proxy for sending commands to the Aeron Media Driver via a ring buffer.

use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::command::{
    DestinationMessageFlyweight, PublicationMessageFlyweight, RemoveMessageFlyweight,
    SubscriptionMessageFlyweight, TerminateDriverFlyweight,
};
use crate::error::{AeronError, Result};

pub const ADD_PUBLICATION: i32 = 0x01;
pub const REMOVE_PUBLICATION: i32 = 0x02;
pub const ADD_EXCLUSIVE_PUBLICATION: i32 = 0x03;
pub const ADD_SUBSCRIPTION: i32 = 0x04;
pub const REMOVE_SUBSCRIPTION: i32 = 0x05;
pub const CLIENT_KEEPALIVE: i32 = 0x06;
pub const ADD_DESTINATION: i32 = 0x07;
pub const REMOVE_DESTINATION: i32 = 0x08;
pub const ADD_COUNTER: i32 = 0x09;
pub const REMOVE_COUNTER: i32 = 0x0A;
pub const CLIENT_CLOSE: i32 = 0x0B;
pub const ADD_RCV_DESTINATION: i32 = 0x0C;
pub const REMOVE_RCV_DESTINATION: i32 = 0x0D;
pub const TERMINATE_DRIVER: i32 = 0x0E;

/// Proxy for communicating with the media driver over a ring buffer.
pub struct DriverProxy<'a> {
    to_driver_ring_buffer: ManyToOneRingBuffer<'a>,
    client_id: i64,
    next_correlation_id: AtomicI64,
}

impl<'a> DriverProxy<'a> {
    pub fn new(to_driver_ring_buffer: ManyToOneRingBuffer<'a>, client_id: i64) -> Self {
        Self {
            to_driver_ring_buffer,
            client_id,
            next_correlation_id: AtomicI64::new(1),
        }
    }

    pub fn client_id(&self) -> i64 {
        self.client_id
    }

    pub fn next_correlation_id(&self) -> i64 {
        self.next_correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn add_publication(&self, channel: &str, stream_id: i32) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = PublicationMessageFlyweight::compute_length(channel.len());
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = PublicationMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_stream_id(stream_id);
        flyweight.set_channel(channel);

        let written = self
            .to_driver_ring_buffer
            .write(ADD_PUBLICATION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write add publication command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    pub fn add_exclusive_publication(&self, channel: &str, stream_id: i32) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = PublicationMessageFlyweight::compute_length(channel.len());
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = PublicationMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_stream_id(stream_id);
        flyweight.set_channel(channel);

        let written =
            self.to_driver_ring_buffer
                .write(ADD_EXCLUSIVE_PUBLICATION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write add exclusive publication command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    pub fn remove_publication(&self, registration_id: i64, revoke: bool) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = RemoveMessageFlyweight::remove_publication_length();
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = RemoveMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_registration_id(registration_id);
        flyweight.set_revoke(revoke);

        let written =
            self.to_driver_ring_buffer
                .write(REMOVE_PUBLICATION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write remove publication command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    pub fn add_subscription(&self, channel: &str, stream_id: i32) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = SubscriptionMessageFlyweight::compute_length(channel.len());
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = SubscriptionMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_registration_correlation_id(-1);
        flyweight.set_stream_id(stream_id);
        flyweight.set_channel(channel);

        let written = self
            .to_driver_ring_buffer
            .write(ADD_SUBSCRIPTION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write add subscription command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    pub fn remove_subscription(&self, registration_id: i64) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = RemoveMessageFlyweight::length();
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = RemoveMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_registration_id(registration_id);

        let written =
            self.to_driver_ring_buffer
                .write(REMOVE_SUBSCRIPTION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write remove subscription command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    /// Tell the media driver that this client is closing.
    pub fn client_close(&self) -> Result<()> {
        let mut buf = vec![0u8; crate::command::CORRELATED_MESSAGE_HEADER_LENGTH];
        let length = buf.len();
        let atomic_buf = AtomicBuffer::new(&mut buf);
        let flyweight = crate::command::CorrelatedMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(-1);
        let written = self
            .to_driver_ring_buffer
            .write(CLIENT_CLOSE, &atomic_buf, 0, length)?;
        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write client close command".to_string(),
            ));
        }
        Ok(())
    }

    pub fn add_destination(&self, registration_id: i64, endpoint_channel: &str) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = DestinationMessageFlyweight::compute_length(endpoint_channel.len());
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = DestinationMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_registration_correlation_id(registration_id);
        flyweight.set_channel(endpoint_channel);

        let written = self
            .to_driver_ring_buffer
            .write(ADD_DESTINATION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write add destination command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    pub fn remove_destination(&self, registration_id: i64, endpoint_channel: &str) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = DestinationMessageFlyweight::compute_length(endpoint_channel.len());
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = DestinationMessageFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_registration_correlation_id(registration_id);
        flyweight.set_channel(endpoint_channel);

        let written =
            self.to_driver_ring_buffer
                .write(REMOVE_DESTINATION, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write remove destination command".to_string(),
            ));
        }

        Ok(correlation_id)
    }

    pub fn terminate_driver(&self, token_bytes: &[u8]) -> Result<i64> {
        let correlation_id = self.next_correlation_id();
        let length = 20 + token_bytes.len();
        let mut buf = vec![0u8; length];
        let atomic_buf = AtomicBuffer::new(&mut buf);

        let flyweight = TerminateDriverFlyweight::new(atomic_buf, 0);
        flyweight.set_client_id(self.client_id);
        flyweight.set_correlation_id(correlation_id);
        flyweight.set_token_buffer(token_bytes);

        let written = self
            .to_driver_ring_buffer
            .write(TERMINATE_DRIVER, &atomic_buf, 0, length)?;

        if !written {
            return Err(AeronError::CommandWriteFailed(
                "failed to write terminate driver command".to_string(),
            ));
        }

        Ok(correlation_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agrona::ring_buffer::TRAILER_LENGTH;

    const CHANNEL: &str = "aeron:udp?endpoint=localhost:40124";
    const STREAM_ID: i32 = 1001;
    const CLIENT_ID: i64 = 7;

    #[test]
    fn thread_sends_add_publication_message() {
        let mut ring_buf_bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let atomic_buf = AtomicBuffer::new(&mut ring_buf_bytes);
        let ring_buffer = ManyToOneRingBuffer::new(atomic_buf).unwrap();

        let proxy = DriverProxy::new(ring_buffer.clone(), CLIENT_ID);
        let corr_id = proxy.add_publication(CHANNEL, STREAM_ID).unwrap();
        assert!(corr_id > 0);

        let mut read_count = 0;
        let messages_read = ring_buffer.read(|msg_type_id, buffer, index, _length| {
            read_count += 1;
            assert_eq!(msg_type_id, ADD_PUBLICATION);
            let flyweight = PublicationMessageFlyweight::new(*buffer, index);
            assert_eq!(flyweight.client_id(), CLIENT_ID);
            assert_eq!(flyweight.stream_id(), STREAM_ID);
            assert_eq!(flyweight.channel(), CHANNEL);
        });

        assert_eq!(messages_read, 1);
        assert_eq!(read_count, 1);
    }

    #[test]
    fn thread_sends_remove_publication_message() {
        let mut ring_buf_bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let atomic_buf = AtomicBuffer::new(&mut ring_buf_bytes);
        let ring_buffer = ManyToOneRingBuffer::new(atomic_buf).unwrap();

        let proxy = DriverProxy::new(ring_buffer.clone(), CLIENT_ID);
        proxy.remove_publication(3, false).unwrap();

        let mut read_count = 0;
        let messages_read = ring_buffer.read(|msg_type_id, buffer, index, _length| {
            read_count += 1;
            assert_eq!(msg_type_id, REMOVE_PUBLICATION);
            let flyweight = RemoveMessageFlyweight::new(*buffer, index);
            assert_eq!(flyweight.client_id(), CLIENT_ID);
            assert_eq!(flyweight.registration_id(), 3);
            assert!(!flyweight.revoke());
        });

        assert_eq!(messages_read, 1);
        assert_eq!(read_count, 1);
    }

    #[test]
    fn thread_sends_remove_subscription_message() {
        let mut ring_buf_bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let atomic_buf = AtomicBuffer::new(&mut ring_buf_bytes);
        let ring_buffer = ManyToOneRingBuffer::new(atomic_buf).unwrap();

        let proxy = DriverProxy::new(ring_buffer.clone(), CLIENT_ID);
        proxy.remove_subscription(3).unwrap();

        let mut read_count = 0;
        let messages_read = ring_buffer.read(|msg_type_id, buffer, index, _length| {
            read_count += 1;
            assert_eq!(msg_type_id, REMOVE_SUBSCRIPTION);
            let flyweight = RemoveMessageFlyweight::new(*buffer, index);
            assert_eq!(flyweight.client_id(), CLIENT_ID);
            assert_eq!(flyweight.registration_id(), 3);
        });

        assert_eq!(messages_read, 1);
        assert_eq!(read_count, 1);
    }
}
