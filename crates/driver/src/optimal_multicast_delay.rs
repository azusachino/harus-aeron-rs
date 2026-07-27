//! Optimal multicast feedback delay generation.
//!
//! Mirrors `io.aeron.driver.OptimalMulticastDelayGenerator` and RFC 5401's optimal multicast
//! feedback distribution. The expensive constants are precomputed once per generator.

use std::sync::atomic::{AtomicU64, Ordering};

static RANDOM_STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

/// Randomized feedback delay for multicast NAKs.
#[derive(Debug, Clone, Copy)]
pub struct OptimalMulticastDelayGenerator {
    rand_max: f64,
    base_x: f64,
    constant_t: f64,
    factor_t: f64,
    max_backoff_ns: f64,
}

impl OptimalMulticastDelayGenerator {
    pub fn new(max_backoff_ns: f64, group_size: f64) -> Self {
        let lambda = group_size.ln() + 1.0;
        Self {
            rand_max: lambda / max_backoff_ns,
            base_x: lambda / (max_backoff_ns * (lambda.exp() - 1.0)),
            constant_t: max_backoff_ns / lambda,
            factor_t: (lambda.exp() - 1.0) * (max_backoff_ns / lambda),
            max_backoff_ns,
        }
    }

    /// Generate a delay in nanoseconds, bounded by the configured maximum.
    pub fn generate_delay_ns(&self) -> i64 {
        self.generate_new_optimal_delay() as i64
    }

    pub fn should_feedback_immediately(&self) -> bool {
        false
    }

    /// Generate a new delay in nanoseconds using the optimal multicast distribution.
    pub fn generate_new_optimal_delay(&self) -> f64 {
        let x = Self::uniform_random(self.rand_max) + self.base_x;
        let delay = self.constant_t * (x * self.factor_t).ln();
        delay.clamp(0.0, self.max_backoff_ns)
    }

    /// Generate a uniform value in `[0, max)` using a lock-free xorshift state.
    pub fn uniform_random(max: f64) -> f64 {
        let mut state = RANDOM_STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let unit = state.wrapping_mul(0x2545_F491_4F6C_DD1D) as f64 / u64::MAX as f64;
        unit * max
    }
}

#[cfg(test)]
mod tests {
    //! Faithful port of
    //! vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/OptimalMulticastDelayGeneratorTest.java

    use super::*;

    #[test]
    fn should_not_exceed_max_backoff() {
        let max_backoff = 60_000_000.0;
        let generator = OptimalMulticastDelayGenerator::new(max_backoff, 10.0);

        for _ in 0..100_000 {
            assert!(generator.generate_new_optimal_delay() <= max_backoff);
        }
    }

    #[test]
    fn should_not_feedback_immediately() {
        let generator = OptimalMulticastDelayGenerator::new(60_000_000.0, 10.0);
        assert!(!generator.should_feedback_immediately());
    }
}
