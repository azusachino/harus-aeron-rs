//! Small cluster-clock primitive used by deterministic tests.

use std::sync::atomic::{AtomicI64, Ordering};

pub struct OffsetMillisecondClusterClock<F>
where
    F: Fn() -> i64,
{
    time_source: F,
    offset_ms: AtomicI64,
}

impl<F> OffsetMillisecondClusterClock<F>
where
    F: Fn() -> i64,
{
    pub fn new(time_source: F) -> Self {
        Self {
            time_source,
            offset_ms: AtomicI64::new(0),
        }
    }

    pub fn time(&self) -> i64 {
        (self.time_source)() + self.offset_ms.load(Ordering::Acquire)
    }

    pub fn add_offset(&self, delta_ms: i64) {
        self.offset_ms.fetch_add(delta_ms, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn should_offset_delegate_time() {
        let time = Cell::new(1000);
        let clock = OffsetMillisecondClusterClock::new(|| time.get());
        assert_eq!(clock.time(), 1000);
        clock.add_offset(7);
        assert_eq!(clock.time(), 1007);
        time.set(time.get() + 1);
        assert_eq!(clock.time(), 1008);
        clock.add_offset(3);
        time.set(time.get() + 1);
        assert_eq!(clock.time(), 1012);
    }
}
