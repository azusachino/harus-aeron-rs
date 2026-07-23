//! DriverConductor command processor and resource manager.
//!
//! Mirrors `io.aeron.driver.DriverConductor.java`.

use std::collections::HashMap;

use crate::configuration::Configuration;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct PublicationEntry {
    pub registration_id: i64,
    pub stream_id: i32,
    pub channel: String,
}

#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub registration_id: i64,
    pub stream_id: i32,
    pub channel: String,
}

pub struct DriverConductor {
    next_registration_id: i64,
    publications: HashMap<i64, PublicationEntry>,
    subscriptions: HashMap<i64, SubscriptionEntry>,
    configuration: Configuration,
}

impl DriverConductor {
    pub fn new(configuration: Configuration) -> Self {
        Self {
            next_registration_id: 1,
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            configuration,
        }
    }

    pub fn do_work(&mut self) -> Result<usize> {
        let work_count = 0;
        // Process client command ring buffer and timeouts
        Ok(work_count)
    }

    pub fn on_add_publication(&mut self, stream_id: i32, channel: &str) -> Result<i64> {
        let reg_id = self.next_registration_id;
        self.next_registration_id += 1;

        self.publications.insert(
            reg_id,
            PublicationEntry {
                registration_id: reg_id,
                stream_id,
                channel: channel.to_string(),
            },
        );

        Ok(reg_id)
    }

    pub fn on_add_subscription(&mut self, stream_id: i32, channel: &str) -> Result<i64> {
        let reg_id = self.next_registration_id;
        self.next_registration_id += 1;

        self.subscriptions.insert(
            reg_id,
            SubscriptionEntry {
                registration_id: reg_id,
                stream_id,
                channel: channel.to_string(),
            },
        );

        Ok(reg_id)
    }

    pub fn on_remove_publication(&mut self, registration_id: i64) -> Result<()> {
        self.publications.remove(&registration_id);
        Ok(())
    }

    pub fn on_remove_subscription(&mut self, registration_id: i64) -> Result<()> {
        self.subscriptions.remove(&registration_id);
        Ok(())
    }

    pub fn publications(&self) -> &HashMap<i64, PublicationEntry> {
        &self.publications
    }

    pub fn subscriptions(&self) -> &HashMap<i64, SubscriptionEntry> {
        &self.subscriptions
    }

    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_add_and_remove_publication_and_subscription() {
        let config = Configuration::default();
        let mut conductor = DriverConductor::new(config);

        let pub_id = conductor
            .on_add_publication(1001, "aeron:udp?endpoint=localhost:40123")
            .unwrap();
        let sub_id = conductor
            .on_add_subscription(1001, "aeron:udp?endpoint=localhost:40123")
            .unwrap();

        assert_eq!(conductor.publications().len(), 1);
        assert_eq!(conductor.subscriptions().len(), 1);

        conductor.on_remove_publication(pub_id).unwrap();
        conductor.on_remove_subscription(sub_id).unwrap();

        assert_eq!(conductor.publications().len(), 0);
        assert_eq!(conductor.subscriptions().len(), 0);
    }
}
