//! Idle strategies to control CPU usage when no work is available.
//!
//! Mirrors the `org.agrona.concurrent.IdleStrategy` interface and its standard
//! implementations from Java Agrona.

use std::hint::spin_loop;
use std::thread::{sleep, yield_now};
use std::time::Duration;

/// Strategy for idling a thread to control CPU usage when no work is available.
pub trait IdleStrategy {
    /// Perform the idle action based on the amount of work done.
    ///
    /// If `work_count > 0`, the strategy resets.
    /// If `work_count == 0`, the strategy performs its idle action.
    fn idle(&mut self, work_count: usize);

    /// Perform the idle action assuming no work was done.
    fn idle_no_work(&mut self);

    /// Reset the internal state in preparation for the next spin cycle.
    fn reset(&mut self);
}

/// Busy spin idle strategy that uses CPU spin loop hint.
///
/// Under the hood, this calls [`spin_loop`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BusySpinIdleStrategy;

impl BusySpinIdleStrategy {
    /// Create a new busy spin idle strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl IdleStrategy for BusySpinIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: usize) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle_no_work();
        }
    }

    #[inline]
    fn idle_no_work(&mut self) {
        spin_loop();
    }

    #[inline]
    fn reset(&mut self) {}
}

/// Yielding idle strategy that yields the current thread's remaining time slice.
///
/// Under the hood, this calls [`yield_now`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct YieldingIdleStrategy;

impl YieldingIdleStrategy {
    /// Create a new yielding idle strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl IdleStrategy for YieldingIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: usize) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle_no_work();
        }
    }

    #[inline]
    fn idle_no_work(&mut self) {
        yield_now();
    }

    #[inline]
    fn reset(&mut self) {}
}

/// Sleeping idle strategy that sleeps for a fixed duration.
///
/// Under the hood, this calls [`sleep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SleepingIdleStrategy {
    sleep_duration: Duration,
}

impl SleepingIdleStrategy {
    /// Create a new sleeping idle strategy with the specified duration.
    pub const fn new(sleep_duration: Duration) -> Self {
        Self { sleep_duration }
    }
}

impl IdleStrategy for SleepingIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: usize) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle_no_work();
        }
    }

    #[inline]
    fn idle_no_work(&mut self) {
        sleep(self.sleep_duration);
    }

    #[inline]
    fn reset(&mut self) {}
}

/// No-op idle strategy that does absolutely nothing.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoOpIdleStrategy;

impl NoOpIdleStrategy {
    /// Create a new no-op idle strategy.
    pub const fn new() -> Self {
        Self
    }
}

impl IdleStrategy for NoOpIdleStrategy {
    #[inline]
    fn idle(&mut self, _work_count: usize) {}

    #[inline]
    fn idle_no_work(&mut self) {}

    #[inline]
    fn reset(&mut self) {}
}

/// Progressive back-off idle strategy that spins, then yields, then parks (sleeps)
/// with an exponentially increasing duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffIdleStrategy {
    max_spins: usize,
    max_yields: usize,
    min_park_period: Duration,
    max_park_period: Duration,
    state: BackoffState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackoffState {
    NotIdle,
    Spinning(usize),
    Yielding(usize),
    Parking(Duration),
}

impl BackoffIdleStrategy {
    /// Create a new back-off idle strategy.
    pub const fn new(
        max_spins: usize,
        max_yields: usize,
        min_park_period: Duration,
        max_park_period: Duration,
    ) -> Self {
        Self {
            max_spins,
            max_yields,
            min_park_period,
            max_park_period,
            state: BackoffState::NotIdle,
        }
    }
}

impl Default for BackoffIdleStrategy {
    fn default() -> Self {
        Self::new(10, 20, Duration::from_nanos(1), Duration::from_millis(1))
    }
}

impl IdleStrategy for BackoffIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: usize) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle_no_work();
        }
    }

    #[inline]
    fn idle_no_work(&mut self) {
        match self.state {
            BackoffState::NotIdle => {
                self.state = BackoffState::Spinning(1);
                spin_loop();
            }
            BackoffState::Spinning(spins) => {
                if spins < self.max_spins {
                    self.state = BackoffState::Spinning(spins + 1);
                    spin_loop();
                } else {
                    self.state = BackoffState::Yielding(1);
                    yield_now();
                }
            }
            BackoffState::Yielding(yields) => {
                if yields < self.max_yields {
                    self.state = BackoffState::Yielding(yields + 1);
                    yield_now();
                } else {
                    let park_period = self.min_park_period;
                    self.state = BackoffState::Parking(park_period);
                    sleep(park_period);
                }
            }
            BackoffState::Parking(park_period) => {
                sleep(park_period);
                let next_park_period = std::cmp::min(park_period * 2, self.max_park_period);
                self.state = BackoffState::Parking(next_park_period);
            }
        }
    }

    #[inline]
    fn reset(&mut self) {
        self.state = BackoffState::NotIdle;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_busy_spin_idle_strategy() {
        let mut strategy = BusySpinIdleStrategy::new();
        strategy.idle(0);
        strategy.idle(1);
        strategy.reset();
    }

    #[test]
    fn test_yielding_idle_strategy() {
        let mut strategy = YieldingIdleStrategy::new();
        strategy.idle(0);
        strategy.idle(1);
        strategy.reset();
    }

    #[test]
    fn test_sleeping_idle_strategy() {
        let mut strategy = SleepingIdleStrategy::new(Duration::from_micros(1));
        strategy.idle(0);
        strategy.idle(1);
        strategy.reset();
    }

    #[test]
    fn test_no_op_idle_strategy() {
        let mut strategy = NoOpIdleStrategy::new();
        strategy.idle(0);
        strategy.idle(1);
        strategy.reset();
    }

    #[test]
    fn test_backoff_idle_strategy_lifecycle() {
        let mut strategy =
            BackoffIdleStrategy::new(2, 2, Duration::from_nanos(1), Duration::from_micros(10));
        assert_eq!(strategy.state, BackoffState::NotIdle);

        // First idle: move to Spinning(1)
        strategy.idle(0);
        assert_eq!(strategy.state, BackoffState::Spinning(1));

        // Second idle: move to Spinning(2) (reaches max_spins = 2)
        strategy.idle(0);
        assert_eq!(strategy.state, BackoffState::Spinning(2));

        // Third idle: transition to Yielding(1)
        strategy.idle(0);
        assert_eq!(strategy.state, BackoffState::Yielding(1));

        // Fourth idle: move to Yielding(2) (reaches max_yields = 2)
        strategy.idle(0);
        assert_eq!(strategy.state, BackoffState::Yielding(2));

        // Fifth idle: transition to Parking(min_park_period)
        strategy.idle(0);
        assert_eq!(
            strategy.state,
            BackoffState::Parking(Duration::from_nanos(1))
        );

        // Sixth idle: back off (doubled)
        strategy.idle(0);
        assert_eq!(
            strategy.state,
            BackoffState::Parking(Duration::from_nanos(2))
        );

        // Reset: returns to NotIdle
        strategy.reset();
        assert_eq!(strategy.state, BackoffState::NotIdle);

        // Calling idle with work > 0 resets strategy
        strategy.idle(0);
        assert_eq!(strategy.state, BackoffState::Spinning(1));
        strategy.idle(1);
        assert_eq!(strategy.state, BackoffState::NotIdle);
    }
}
