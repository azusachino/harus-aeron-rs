//! MediaDriver main orchestrator launching Conductor, Sender, and Receiver agents.
//!
//! Mirrors `io.aeron.driver.MediaDriver.java`.

use crate::conductor::DriverConductor;
use crate::configuration::Configuration;
use crate::error::Result;
use crate::receiver::Receiver;
use crate::sender::Sender;

pub struct MediaDriver {
    configuration: Configuration,
    conductor: DriverConductor,
    sender: Sender,
    receiver: Receiver,
}

impl MediaDriver {
    pub fn launch(configuration: Configuration) -> Result<Self> {
        let conductor = DriverConductor::new(configuration.clone());
        let sender = Sender::new();
        let receiver = Receiver::new();

        Ok(Self {
            configuration,
            conductor,
            sender,
            receiver,
        })
    }

    pub fn do_work(&mut self) -> Result<usize> {
        let conductor_work = self.conductor.do_work()?;
        let sender_work = self.sender.do_work()?;
        let receiver_work = self.receiver.do_work()?;

        Ok(conductor_work + sender_work + receiver_work)
    }

    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    pub fn conductor(&self) -> &DriverConductor {
        &self.conductor
    }

    pub fn conductor_mut(&mut self) -> &mut DriverConductor {
        &mut self.conductor
    }

    pub fn sender(&self) -> &Sender {
        &self.sender
    }

    pub fn sender_mut(&mut self) -> &mut Sender {
        &mut self.sender
    }

    pub fn receiver(&self) -> &Receiver {
        &self.receiver
    }

    pub fn receiver_mut(&mut self) -> &mut Receiver {
        &mut self.receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_launch_media_driver_and_step_duty_cycle() {
        let config = Configuration::default();
        let mut driver = MediaDriver::launch(config).unwrap();
        let work = driver.do_work().unwrap();
        assert_eq!(work, 0);
    }
}
