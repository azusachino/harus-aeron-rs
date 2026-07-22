//! Client-side command lifecycle coordinator.

use std::collections::HashSet;

use crate::driver_proxy::DriverProxy;
use crate::error::{AeronError, Result};

/// Tracks client registrations and forwards their lifecycle commands to the driver.
pub struct ClientConductor<'a> {
    driver_proxy: DriverProxy<'a>,
    publication_registrations: HashSet<i64>,
    subscription_registrations: HashSet<i64>,
    is_closed: bool,
}

impl<'a> ClientConductor<'a> {
    pub fn new(driver_proxy: DriverProxy<'a>) -> Self {
        Self {
            driver_proxy,
            publication_registrations: HashSet::new(),
            subscription_registrations: HashSet::new(),
            is_closed: false,
        }
    }

    pub fn driver_proxy(&self) -> &DriverProxy<'a> {
        &self.driver_proxy
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn add_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64> {
        self.ensure_open()?;
        let registration_id = self.driver_proxy.add_publication(channel, stream_id)?;
        self.publication_registrations.insert(registration_id);
        Ok(registration_id)
    }

    pub fn add_subscription(&mut self, channel: &str, stream_id: i32) -> Result<i64> {
        self.ensure_open()?;
        let registration_id = self.driver_proxy.add_subscription(channel, stream_id)?;
        self.subscription_registrations.insert(registration_id);
        Ok(registration_id)
    }

    pub fn remove_publication(&mut self, registration_id: i64) -> Result<bool> {
        self.ensure_open()?;
        if self.publication_registrations.remove(&registration_id) {
            self.driver_proxy
                .remove_publication(registration_id, false)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn remove_subscription(&mut self, registration_id: i64) -> Result<bool> {
        self.ensure_open()?;
        if self.subscription_registrations.remove(&registration_id) {
            self.driver_proxy.remove_subscription(registration_id)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn publication_count(&self) -> usize {
        self.publication_registrations.len()
    }

    pub fn subscription_count(&self) -> usize {
        self.subscription_registrations.len()
    }

    pub fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }

        for registration_id in self.publication_registrations.drain() {
            self.driver_proxy
                .remove_publication(registration_id, false)?;
        }
        for registration_id in self.subscription_registrations.drain() {
            self.driver_proxy.remove_subscription(registration_id)?;
        }
        self.is_closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_closed {
            Err(AeronError::IllegalState(
                "ClientConductor is closed".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{
        PublicationMessageFlyweight, RemoveMessageFlyweight, SubscriptionMessageFlyweight,
    };
    use crate::driver_proxy::{
        ADD_PUBLICATION, ADD_SUBSCRIPTION, REMOVE_PUBLICATION, REMOVE_SUBSCRIPTION,
    };
    use agrona::AtomicBuffer;
    use agrona::ring_buffer::TRAILER_LENGTH;

    const CHANNEL: &str = "aeron:ipc";
    const CLIENT_ID: i64 = 42;

    fn conductor<'a>(
        bytes: &'a mut [u8],
    ) -> (
        ClientConductor<'a>,
        agrona::ring_buffer::mpsc::ManyToOneRingBuffer<'a>,
    ) {
        let buffer = ManyToOneRingBuffer::new(AtomicBuffer::new(bytes)).unwrap();
        let reader = buffer.clone();
        (
            ClientConductor::new(DriverProxy::new(buffer, CLIENT_ID)),
            reader,
        )
    }

    use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

    #[test]
    fn forwards_registration_and_removal_commands() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, reader) = conductor(&mut bytes);
        let publication_id = conductor.add_publication(CHANNEL, 7).unwrap();
        let subscription_id = conductor.add_subscription(CHANNEL, 8).unwrap();
        assert_eq!(conductor.publication_count(), 1);
        assert_eq!(conductor.subscription_count(), 1);

        assert!(conductor.remove_publication(publication_id).unwrap());
        assert!(conductor.remove_subscription(subscription_id).unwrap());
        assert_eq!(conductor.publication_count(), 0);
        assert_eq!(conductor.subscription_count(), 0);

        let mut types = Vec::new();
        reader.read(|message_type, buffer, index, _| {
            types.push(message_type);
            match message_type {
                ADD_PUBLICATION => assert_eq!(
                    PublicationMessageFlyweight::new(*buffer, index).channel(),
                    CHANNEL
                ),
                ADD_SUBSCRIPTION => assert_eq!(
                    SubscriptionMessageFlyweight::new(*buffer, index).channel(),
                    CHANNEL
                ),
                REMOVE_PUBLICATION | REMOVE_SUBSCRIPTION => assert_eq!(
                    RemoveMessageFlyweight::new(*buffer, index).client_id(),
                    CLIENT_ID
                ),
                _ => panic!("unexpected command"),
            }
        });
        assert_eq!(
            types,
            vec![
                ADD_PUBLICATION,
                ADD_SUBSCRIPTION,
                REMOVE_PUBLICATION,
                REMOVE_SUBSCRIPTION
            ]
        );
    }

    #[test]
    fn close_is_idempotent_and_rejects_new_commands() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, _) = conductor(&mut bytes);
        conductor.close().unwrap();
        conductor.close().unwrap();
        assert!(conductor.is_closed());
        assert!(matches!(
            conductor.add_subscription(CHANNEL, 1),
            Err(AeronError::IllegalState(_))
        ));
    }
}
