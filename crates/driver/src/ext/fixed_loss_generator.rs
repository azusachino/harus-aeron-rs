use std::collections::HashMap;

/// Drops one configured byte range per stream/session pair and ignores retransmissions.
pub struct FixedLossGenerator {
    term_id: i32,
    term_offset: i32,
    length: i32,
    maximum_dropped_offset: HashMap<(i32, i32), i32>,
}

impl FixedLossGenerator {
    pub fn new(term_id: i32, term_offset: i32, length: i32) -> Self {
        Self {
            term_id,
            term_offset,
            length,
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
        if self.term_id != term_id {
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
        let drop_limit = self.term_offset.saturating_add(self.length);
        if frame_limit > self.term_offset && term_offset < drop_limit {
            *maximum_dropped_offset = frame_limit;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::FixedLossGenerator;

    #[test]
    fn drops_once_and_ignores_retransmissions() {
        let mut generator = FixedLossGenerator::new(0, 50, 1408);
        assert!(generator.should_drop_frame(123, 456, 0, 0, 1408));
        assert!(generator.should_drop_frame(123, 456, 0, 1408, 1408));
        assert!(!generator.should_drop_frame(123, 456, 0, 0, 1408));
        assert!(generator.should_drop_frame(124, 456, 0, 0, 1408));
    }

    #[test]
    fn only_drops_matching_term_and_overlapping_frames() {
        let mut generator = FixedLossGenerator::new(1, 2000, 1408);
        assert!(!generator.should_drop_frame(123, 456, 0, 1408, 1408));
        assert!(!generator.should_drop_frame(123, 456, 1, 0, 1408));
        assert!(generator.should_drop_frame(123, 456, 1, 1408, 1408));
        assert!(generator.should_drop_frame(123, 456, 1, 2 * 1408, 1408));
        assert!(!generator.should_drop_frame(123, 456, 1, 3 * 1408, 1408));
    }
}
