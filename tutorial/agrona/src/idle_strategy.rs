//! Learner stub: Idle strategy implementations.

use std::time::Duration;

pub trait IdleStrategy {
    fn idle_count(&mut self, work_count: usize);
    fn idle(&mut self);
    fn reset(&mut self);
}

#[derive(Debug, Default, Copy, Clone)]
pub struct BusySpinIdleStrategy;

impl BusySpinIdleStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl IdleStrategy for BusySpinIdleStrategy {
    fn idle_count(&mut self, _work_count: usize) {
        todo!("implement BusySpinIdleStrategy::idle_count")
    }

    fn idle(&mut self) {
        todo!("implement BusySpinIdleStrategy::idle")
    }

    fn reset(&mut self) {
        todo!("implement BusySpinIdleStrategy::reset")
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct YieldingIdleStrategy;

impl YieldingIdleStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl IdleStrategy for YieldingIdleStrategy {
    fn idle_count(&mut self, _work_count: usize) {
        todo!("implement YieldingIdleStrategy::idle_count")
    }

    fn idle(&mut self) {
        todo!("implement YieldingIdleStrategy::idle")
    }

    fn reset(&mut self) {
        todo!("implement YieldingIdleStrategy::reset")
    }
}

#[derive(Debug, Copy, Clone)]
pub struct SleepingIdleStrategy {
    duration: Duration,
}

impl SleepingIdleStrategy {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl IdleStrategy for SleepingIdleStrategy {
    fn idle_count(&mut self, _work_count: usize) {
        todo!("implement SleepingIdleStrategy::idle_count")
    }

    fn idle(&mut self) {
        todo!("implement SleepingIdleStrategy::idle")
    }

    fn reset(&mut self) {
        todo!("implement SleepingIdleStrategy::reset")
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct NoOpIdleStrategy;

impl NoOpIdleStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl IdleStrategy for NoOpIdleStrategy {
    fn idle_count(&mut self, _work_count: usize) {
        todo!("implement NoOpIdleStrategy::idle_count")
    }

    fn idle(&mut self) {
        todo!("implement NoOpIdleStrategy::idle")
    }

    fn reset(&mut self) {
        todo!("implement NoOpIdleStrategy::reset")
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BackoffIdleStrategy {
    spin_spins: u64,
    yield_yields: u64,
    min_park_period: Duration,
    max_park_period: Duration,
    state: u8,
    spins: u64,
    yields: u64,
    park_period: Duration,
}

impl BackoffIdleStrategy {
    pub fn new(
        spin_spins: u64,
        yield_yields: u64,
        min_park_period: Duration,
        max_park_period: Duration,
    ) -> Self {
        Self {
            spin_spins,
            yield_yields,
            min_park_period,
            max_park_period,
            state: 0,
            spins: 0,
            yields: 0,
            park_period: min_park_period,
        }
    }
}

impl IdleStrategy for BackoffIdleStrategy {
    fn idle_count(&mut self, _work_count: usize) {
        todo!("implement BackoffIdleStrategy::idle_count")
    }

    fn idle(&mut self) {
        todo!("implement BackoffIdleStrategy::idle")
    }

    fn reset(&mut self) {
        todo!("implement BackoffIdleStrategy::reset")
    }
}
