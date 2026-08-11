//! Ordered recording-id to catalog-offset index.

use std::fmt;

pub const DEFAULT_INDEX_SIZE: usize = 10;
pub const NULL_VALUE: i64 = -1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogIndexError {
    NegativeRecordingId(i64),
    NegativeRecordingOffset(i64),
    NonMonotonicRecordingId { recording_id: i64, last: i64 },
}

impl fmt::Display for CatalogIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeRecordingId(recording_id) => {
                write!(formatter, "recordingId {recording_id} must not be negative")
            }
            Self::NegativeRecordingOffset(offset) => {
                write!(formatter, "recordingOffset {offset} must not be negative")
            }
            Self::NonMonotonicRecordingId { recording_id, last } => write!(
                formatter,
                "recordingId {recording_id} is less than or equal to the last recordingId {last}"
            ),
        }
    }
}

impl std::error::Error for CatalogIndexError {}

#[derive(Debug, Clone)]
pub struct CatalogIndex {
    index: Vec<i64>,
    size: usize,
}

impl Default for CatalogIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogIndex {
    pub fn new() -> Self {
        Self {
            index: vec![0; DEFAULT_INDEX_SIZE * 2],
            size: 0,
        }
    }

    pub fn index(&self) -> &[i64] {
        &self.index
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn add(
        &mut self,
        recording_id: i64,
        recording_offset: i64,
    ) -> Result<(), CatalogIndexError> {
        if recording_id < 0 {
            return Err(CatalogIndexError::NegativeRecordingId(recording_id));
        }
        if recording_offset < 0 {
            return Err(CatalogIndexError::NegativeRecordingOffset(recording_offset));
        }
        if self
            .last_recording_id()
            .is_some_and(|last| recording_id <= last)
        {
            return Err(CatalogIndexError::NonMonotonicRecordingId {
                recording_id,
                last: self.last_recording_id().unwrap_or_default(),
            });
        }
        if self.size == self.index.len() / 2 {
            let capacity = self.index.len() / 2;
            self.index.resize((capacity + capacity / 2) * 2, 0);
        }
        let offset = self.size * 2;
        self.index[offset] = recording_id;
        self.index[offset + 1] = recording_offset;
        self.size += 1;
        Ok(())
    }

    pub fn recording_offset(&self, recording_id: i64) -> Result<i64, CatalogIndexError> {
        if recording_id < 0 {
            return Err(CatalogIndexError::NegativeRecordingId(recording_id));
        }
        Ok(self
            .find(recording_id)
            .map_or(NULL_VALUE, |index| self.index[index * 2 + 1]))
    }

    pub fn remove(&mut self, recording_id: i64) -> Result<i64, CatalogIndexError> {
        if recording_id < 0 {
            return Err(CatalogIndexError::NegativeRecordingId(recording_id));
        }
        let Some(index) = self.find(recording_id) else {
            return Ok(NULL_VALUE);
        };
        let offset = self.index[index * 2 + 1];
        let used = self.size * 2;
        self.index.copy_within((index + 1) * 2..used, index * 2);
        self.index[used - 2..used].fill(0);
        self.size -= 1;
        Ok(offset)
    }

    fn last_recording_id(&self) -> Option<i64> {
        (self.size > 0).then(|| self.index[(self.size - 1) * 2])
    }

    fn find(&self, recording_id: i64) -> Option<usize> {
        let mut low = 0;
        let mut high = self.size;
        while low < high {
            let middle = low + (high - low) / 2;
            match self.index[middle * 2].cmp(&recording_id) {
                std::cmp::Ordering::Equal => return Some(middle),
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_index_capacity_is_ten_entries() {
        assert_eq!(CatalogIndex::new().index(), &[0; DEFAULT_INDEX_SIZE * 2]);
    }

    #[test]
    fn size_returns_zero_for_an_empty_index() {
        assert_eq!(CatalogIndex::new().size(), 0);
    }

    #[test]
    fn add_rejects_negative_recording_id() {
        assert!(matches!(
            CatalogIndex::new().add(-1, 0),
            Err(CatalogIndexError::NegativeRecordingId(-1))
        ));
    }

    #[test]
    fn add_rejects_negative_recording_offset() {
        assert!(matches!(
            CatalogIndex::new().add(1024, i64::MIN),
            Err(CatalogIndexError::NegativeRecordingOffset(i64::MIN))
        ));
    }

    #[test]
    fn add_one_recording() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(3, 100).unwrap();

        let mut expected = vec![0; DEFAULT_INDEX_SIZE * 2];
        expected[0..2].copy_from_slice(&[3, 100]);
        assert_eq!(catalog_index.index(), expected);
        assert_eq!(catalog_index.size(), 1);
    }

    #[test]
    fn add_appends_to_the_end_of_the_index() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(6, 50).unwrap();
        catalog_index.add(21, 64).unwrap();

        let mut expected = vec![0; DEFAULT_INDEX_SIZE * 2];
        expected[0..4].copy_from_slice(&[6, 50, 21, 64]);
        assert_eq!(catalog_index.index(), expected);
        assert_eq!(catalog_index.size(), 2);
    }

    #[test]
    fn add_rejects_non_monotonic_recording_id() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(100, 0).unwrap();

        assert_eq!(
            catalog_index.add(5, 200),
            Err(CatalogIndexError::NonMonotonicRecordingId {
                recording_id: 5,
                last: 100
            })
        );
        assert_eq!(
            catalog_index.add(100, 200),
            Err(CatalogIndexError::NonMonotonicRecordingId {
                recording_id: 100,
                last: 100
            })
        );
    }

    #[test]
    fn add_expands_index_when_full() {
        let mut catalog_index = CatalogIndex::new();
        for recording_id in 0..DEFAULT_INDEX_SIZE as i64 {
            catalog_index.add(recording_id, recording_id).unwrap();
        }
        catalog_index.add(i64::MAX, i64::MAX).unwrap();

        assert_eq!(catalog_index.index().len(), 30);
        assert_eq!(catalog_index.index()[20..22], [i64::MAX, i64::MAX]);
        assert_eq!(catalog_index.size(), DEFAULT_INDEX_SIZE + 1);
    }

    #[test]
    fn recording_offset_rejects_negative_recording_id() {
        assert!(matches!(
            CatalogIndex::new().recording_offset(-100),
            Err(CatalogIndexError::NegativeRecordingId(-100))
        ));
    }

    #[test]
    fn recording_offset_returns_null_for_an_empty_index() {
        let catalog_index = CatalogIndex::new();
        for recording_id in [0, 1, DEFAULT_INDEX_SIZE as i64, i64::MAX] {
            assert_eq!(catalog_index.recording_offset(recording_id), Ok(NULL_VALUE));
        }
    }

    #[test]
    fn recording_offset_returns_null_for_a_missing_single_entry() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(200, 0).unwrap();

        for recording_id in [0, 199, 201, i64::MAX] {
            assert_eq!(catalog_index.recording_offset(recording_id), Ok(NULL_VALUE));
        }
    }

    #[test]
    fn recording_offset_returns_null_for_missing_multiple_entries() {
        let mut catalog_index = CatalogIndex::new();
        for recording_id in 10..100 {
            catalog_index.add(recording_id, recording_id).unwrap();
        }

        for recording_id in [9, 101, 1_000_000_000, 4_002_662_252, i64::MAX] {
            assert_eq!(catalog_index.recording_offset(recording_id), Ok(NULL_VALUE));
        }
    }

    #[test]
    fn recording_offset_returns_associated_offset() {
        let mut catalog_index = CatalogIndex::new();
        for (recording_id, recording_offset) in [
            (0, 0),
            (1, 1),
            (2, 3),
            (100, 777),
            (1000, 2000),
            (1001, 2002),
        ] {
            catalog_index.add(recording_id, recording_offset).unwrap();
        }

        for (recording_id, recording_offset) in [
            (0, 0),
            (1, 1),
            (2, 3),
            (100, 777),
            (1000, 2000),
            (1001, 2002),
        ] {
            assert_eq!(
                catalog_index.recording_offset(recording_id),
                Ok(recording_offset)
            );
        }
    }

    #[test]
    fn recording_offset_reads_a_single_element() {
        for recording_id in [0, 100, i64::MAX] {
            let mut catalog_index = CatalogIndex::new();
            catalog_index
                .add(recording_id, recording_id.wrapping_mul(3))
                .unwrap();
            assert_eq!(
                catalog_index.recording_offset(recording_id),
                Ok(recording_id.wrapping_mul(3))
            );
        }
    }

    #[test]
    fn recording_offset_reads_first_element() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(0, 10).unwrap();
        catalog_index.add(1, 20).unwrap();
        catalog_index.add(2, 30).unwrap();
        assert_eq!(catalog_index.recording_offset(0), Ok(10));
    }

    #[test]
    fn recording_offset_reads_last_element() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(1, 10).unwrap();
        catalog_index.add(5, 20).unwrap();
        catalog_index.add(12, 30).unwrap();
        assert_eq!(catalog_index.recording_offset(12), Ok(30));
    }

    #[test]
    fn recording_offset_reads_last_element_when_full() {
        let mut catalog_index = CatalogIndex::new();
        for recording_id in 0..DEFAULT_INDEX_SIZE as i64 {
            catalog_index
                .add(recording_id * 2, recording_id + 1)
                .unwrap();
        }
        assert_eq!(
            catalog_index.recording_offset((DEFAULT_INDEX_SIZE - 1) as i64 * 2),
            Ok(DEFAULT_INDEX_SIZE as i64)
        );
    }

    #[test]
    fn remove_rejects_negative_recording_id() {
        assert!(matches!(
            CatalogIndex::new().remove(-1),
            Err(CatalogIndexError::NegativeRecordingId(-1))
        ));
    }

    #[test]
    fn remove_returns_null_when_empty() {
        assert_eq!(CatalogIndex::new().remove(1), Ok(NULL_VALUE));
    }

    #[test]
    fn remove_returns_null_for_a_missing_recording() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(1, 0).unwrap();
        catalog_index.add(20, 10).unwrap();

        assert_eq!(catalog_index.remove(7), Ok(NULL_VALUE));
        assert_eq!(catalog_index.size(), 2);
    }

    #[test]
    fn remove_the_only_entry() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(0, 1024).unwrap();

        assert_eq!(catalog_index.remove(0), Ok(1024));
        assert_eq!(catalog_index.size(), 0);
        assert_eq!(catalog_index.recording_offset(0), Ok(NULL_VALUE));
    }

    #[test]
    fn remove_first_element() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(0, 500).unwrap();
        catalog_index.add(1, 1000).unwrap();
        catalog_index.add(2, 1500).unwrap();

        assert_eq!(catalog_index.remove(0), Ok(500));
        assert_eq!(catalog_index.size(), 2);
        assert_eq!(catalog_index.recording_offset(0), Ok(NULL_VALUE));
        assert_eq!(catalog_index.recording_offset(1), Ok(1000));
        assert_eq!(catalog_index.recording_offset(2), Ok(1500));
    }

    #[test]
    fn remove_last_element() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(0, 500).unwrap();
        catalog_index.add(1, 1000).unwrap();
        catalog_index.add(2, 1500).unwrap();

        assert_eq!(catalog_index.remove(2), Ok(1500));
        assert_eq!(catalog_index.size(), 2);
        assert_eq!(catalog_index.recording_offset(2), Ok(NULL_VALUE));
        assert_eq!(catalog_index.recording_offset(0), Ok(500));
        assert_eq!(catalog_index.recording_offset(1), Ok(1000));
    }

    #[test]
    fn remove_middle_element() {
        let mut catalog_index = CatalogIndex::new();
        catalog_index.add(0, 500).unwrap();
        catalog_index.add(10, 1000).unwrap();
        catalog_index.add(20, 1500).unwrap();
        catalog_index.add(30, 7777).unwrap();

        assert_eq!(catalog_index.remove(10), Ok(1000));
        assert_eq!(catalog_index.size(), 3);
        assert_eq!(catalog_index.recording_offset(10), Ok(NULL_VALUE));
        assert_eq!(catalog_index.recording_offset(0), Ok(500));
        assert_eq!(catalog_index.recording_offset(20), Ok(1500));
        assert_eq!(catalog_index.recording_offset(30), Ok(7777));
    }

    #[test]
    fn remove_last_element_from_a_full_index() {
        let mut catalog_index = CatalogIndex::new();
        for recording_id in 1..=DEFAULT_INDEX_SIZE as i64 {
            catalog_index.add(recording_id, recording_id).unwrap();
        }

        assert_eq!(
            catalog_index.remove(DEFAULT_INDEX_SIZE as i64),
            Ok(DEFAULT_INDEX_SIZE as i64)
        );
        assert_eq!(catalog_index.size(), DEFAULT_INDEX_SIZE - 1);
        assert_eq!(
            catalog_index.recording_offset(DEFAULT_INDEX_SIZE as i64),
            Ok(NULL_VALUE)
        );
    }
}
