use std::collections::HashMap;

/// Drops a configured number of gaps at power-of-two offsets, once per stream/session pair.
pub struct MultiGapLossGenerator {
    term_id: i32,
    gap_radix_bits: u32,
    gap_radix_mask: i32,
    gap_length: i32,
    last_gap_limit: i32,
    maximum_dropped_offset: HashMap<(i32, i32), i32>,
}

impl MultiGapLossGenerator {
    pub fn new(term_id: i32, gap_radix: i32, gap_length: i32, total_gaps: i32) -> Self {
        let actual_gap_radix = (gap_radix.max(1) as u32).next_power_of_two() as i32;
        assert!(
            gap_length < actual_gap_radix,
            "gap_length must be smaller than gap_radix"
        );
        Self {
            term_id,
            gap_radix_bits: actual_gap_radix.trailing_zeros(),
            gap_radix_mask: !(actual_gap_radix - 1),
            gap_length,
            last_gap_limit: total_gaps
                .saturating_mul(actual_gap_radix)
                .saturating_add(gap_length),
            maximum_dropped_offset: HashMap::new(),
        }
    }

    pub fn should_drop_frame(
        &mut self,
        stream_id: i32,
        session_id: i32,
        term_id: i32,
        term_offset: i32,
        length: i32,
    ) -> bool {
        if self.term_id != term_id || term_offset > self.last_gap_limit {
            return false;
        }
        let maximum_dropped_offset = self
            .maximum_dropped_offset
            .entry((stream_id, session_id))
            .or_insert(term_offset);
        if *maximum_dropped_offset > term_offset {
            return false;
        }
        let frame_limit = term_offset.saturating_add(length);
        if term_offset != 0 && term_offset.trailing_zeros() >= self.gap_radix_bits {
            *maximum_dropped_offset = frame_limit;
            return true;
        }

        let previous_gap_offset = term_offset & self.gap_radix_mask;
        let previous_gap_limit = previous_gap_offset.saturating_add(self.gap_length);
        if previous_gap_offset > 0 && term_offset < previous_gap_limit {
            *maximum_dropped_offset = frame_limit;
            return true;
        }

        let next_gap_offset = ((term_offset >> self.gap_radix_bits) + 1) << self.gap_radix_bits;
        let next_gap_limit = next_gap_offset.saturating_add(self.gap_length);
        if frame_limit > next_gap_offset && term_offset < next_gap_limit {
            *maximum_dropped_offset = frame_limit;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::MultiGapLossGenerator;

    #[test]
    fn drops_multiple_gaps_and_not_retransmissions() {
        let mut generator = MultiGapLossGenerator::new(0, 16, 8, 4);
        assert!(!generator.should_drop_frame(123, 456, 0, 0, 8));
        assert!(!generator.should_drop_frame(123, 456, 0, 8, 8));
        assert!(generator.should_drop_frame(123, 456, 0, 16, 8));
        assert!(!generator.should_drop_frame(123, 456, 0, 16, 8));
        assert!(generator.should_drop_frame(123, 456, 0, 32, 8));
    }

    #[test]
    fn drops_gap_crossings_for_independent_streams() {
        let mut generator = MultiGapLossGenerator::new(0, 8, 4, 1);
        assert!(!generator.should_drop_frame(123, 456, 0, 0, 8));
        assert!(generator.should_drop_frame(123, 456, 0, 8, 8));
        assert!(!generator.should_drop_frame(123, 456, 0, 16, 8));
        assert!(!generator.should_drop_frame(124, 456, 0, 0, 8));
        assert!(generator.should_drop_frame(124, 456, 0, 8, 8));
    }
}
