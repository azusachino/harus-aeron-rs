//! Utilities for checking timestamp deadlines.

/// Check if a deadline timestamp has been reached relative to the current time.
#[inline]
pub fn has_reached_deadline(now: i64, deadline: i64) -> bool {
    deadline.wrapping_sub(now) < 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_correctly_reach_deadline() {
        assert!(!has_reached_deadline(1, 2));
        assert!(!has_reached_deadline(2, 2));
        assert!(has_reached_deadline(3, 2));
        assert!(!has_reached_deadline(i64::MAX, i64::MAX));
        assert!(!has_reached_deadline(i64::MAX, i64::MAX.wrapping_add(1)));
        assert!(!has_reached_deadline(i64::MIN, i64::MIN.wrapping_add(1)));
        assert!(!has_reached_deadline(-1, -1 + i64::MAX));
        assert!(has_reached_deadline(-1, i64::MAX));
    }
}
