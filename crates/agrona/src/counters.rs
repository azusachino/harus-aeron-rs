//! Counter allocation and tracking over [`AtomicBuffer`] instances.
//!
//! Mirrors `org.agrona.concurrent.status.CountersManager` and
//! `org.agrona.concurrent.status.CountersReader` from Java Agrona.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::{AgronaError, Result};

// Record layout mirrors vendor `aeron_counters_manager.h` / `aeronc.h` (release/1.50.x).
//
// Metadata record (512 B): state@0, type_id@4, free_for_reuse_deadline_ms@8,
//   key@16 [112 B], label_length@128, label@132 [380 B].
// Value record (128 B, two cache lines): counter_value@0, registration_id@8,
//   owner_id@16, reference_id@24, padding.

/// Length of each metadata record in bytes (vendor: 2 * cache line).
pub const METADATA_LENGTH: usize = 512;
/// Length of each counter value record in bytes (vendor: 2 cache lines, false-sharing pad).
pub const COUNTER_LENGTH: usize = 128;

/// Size of the key region in a metadata record.
pub const MAX_KEY_LENGTH: usize = 112;
/// Size of the label region (4-byte length prefix + up to 380 characters).
pub const MAX_LABEL_LENGTH: usize = 384;

/// State indicating the counter record slot is unused.
pub const RECORD_UNUSED: i32 = 0;
/// State indicating the counter record slot is allocated.
pub const RECORD_ALLOCATED: i32 = 1;
/// State indicating the counter record slot has been reclaimed for reuse.
pub const RECORD_RECLAIMED: i32 = -1;

/// Offset of state indicator in metadata record.
pub const STATE_OFFSET: usize = 0;
/// Offset of type ID in metadata record.
pub const TYPE_ID_OFFSET: usize = 4;
/// Offset of the free-for-reuse deadline (ms) in metadata record.
pub const FREE_FOR_REUSE_DEADLINE_OFFSET: usize = 8;
/// Offset of key length and data in metadata record.
pub const KEY_OFFSET: usize = 16;
/// Offset of label length and string in metadata record.
pub const LABEL_OFFSET: usize = KEY_OFFSET + MAX_KEY_LENGTH;

/// Offset of the counter value within a value record.
pub const COUNTER_VALUE_OFFSET: usize = 0;
/// Offset of the registration id within a value record.
pub const REGISTRATION_ID_OFFSET: usize = 8;
/// Offset of the owner id within a value record.
pub const OWNER_ID_OFFSET: usize = 16;
/// Offset of the reference id within a value record.
pub const REFERENCE_ID_OFFSET: usize = 24;

/// A handle to an individual 64-bit atomic counter within a values buffer.
#[derive(Clone)]
pub struct AtomicCounter<'a> {
    values_buffer: AtomicBuffer<'a>,
    counter_id: i32,
    offset: usize,
}

impl<'a> AtomicCounter<'a> {
    /// Create a new `AtomicCounter` handle.
    pub fn new(values_buffer: AtomicBuffer<'a>, counter_id: i32) -> Self {
        let offset = counter_id as usize * COUNTER_LENGTH;
        Self {
            values_buffer,
            counter_id,
            offset,
        }
    }

    /// Get the counter ID.
    pub fn id(&self) -> i32 {
        self.counter_id
    }

    /// Get the current value of the counter using volatile read.
    pub fn get(&self) -> i64 {
        self.values_buffer.get_i64_volatile(self.offset)
    }

    /// Set the value of the counter using volatile store.
    pub fn set(&self, value: i64) {
        self.values_buffer.put_i64_volatile(self.offset, value);
    }

    /// Set the value of the counter using ordered (release) store.
    pub fn set_ordered(&self, value: i64) {
        self.values_buffer.put_i64_ordered(self.offset, value);
    }

    /// Atomically add `delta` to the counter and return the previous value.
    pub fn add(&self, delta: i64) -> i64 {
        self.values_buffer.get_and_add_i64(self.offset, delta)
    }

    /// Atomically increment the counter by 1 and return the previous value.
    pub fn increment(&self) -> i64 {
        self.add(1)
    }

    /// Compare and set (CAS) the counter value.
    pub fn compare_and_set(&self, expected: i64, update: i64) -> bool {
        self.values_buffer
            .compare_and_set_i64(self.offset, expected, update)
    }
}

/// Manages counter allocation and metadata tracking over a pair of buffers.
#[derive(Clone)]
pub struct CountersManager<'a> {
    metadata_buffer: AtomicBuffer<'a>,
    values_buffer: AtomicBuffer<'a>,
    max_counters: usize,
}

impl<'a> CountersManager<'a> {
    /// Create a new `CountersManager` wrapping metadata and values buffers.
    pub fn new(metadata_buffer: AtomicBuffer<'a>, values_buffer: AtomicBuffer<'a>) -> Result<Self> {
        let max_metadata_counters = metadata_buffer.capacity() / METADATA_LENGTH;
        let max_values_counters = values_buffer.capacity() / COUNTER_LENGTH;
        let max_counters = std::cmp::min(max_metadata_counters, max_values_counters);

        if max_counters == 0 {
            return Err(AgronaError::OutOfBounds {
                offset: 0,
                length: COUNTER_LENGTH,
                capacity: values_buffer.capacity(),
            });
        }

        Ok(Self {
            metadata_buffer,
            values_buffer,
            max_counters,
        })
    }

    /// Returns the maximum number of counters that can be allocated in these buffers.
    pub fn max_counters(&self) -> usize {
        self.max_counters
    }

    /// Allocate a new counter with the given type ID, key bytes, and label text.
    ///
    /// Returns the allocated counter ID.
    pub fn allocate(&self, type_id: i32, key: &[u8], label: &str) -> Result<i32> {
        if key.len() > MAX_KEY_LENGTH - 4 {
            return Err(AgronaError::OutOfBounds {
                offset: 0,
                length: key.len(),
                capacity: MAX_KEY_LENGTH - 4,
            });
        }
        if label.len() > MAX_LABEL_LENGTH - 4 {
            return Err(AgronaError::OutOfBounds {
                offset: 0,
                length: label.len(),
                capacity: MAX_LABEL_LENGTH - 4,
            });
        }

        for id in 0..self.max_counters {
            let meta_offset = id * METADATA_LENGTH;
            let state = self
                .metadata_buffer
                .get_i32_volatile(meta_offset + STATE_OFFSET);

            if state == RECORD_UNUSED || state == RECORD_RECLAIMED {
                // Write metadata fields
                self.metadata_buffer
                    .put_i32(meta_offset + TYPE_ID_OFFSET, type_id);

                // Write key (length + bytes)
                self.metadata_buffer
                    .put_i32(meta_offset + KEY_OFFSET, key.len() as i32);
                self.metadata_buffer
                    .try_put_bytes(meta_offset + KEY_OFFSET + 4, key)?;

                // Write label (length + bytes)
                self.metadata_buffer
                    .put_i32(meta_offset + LABEL_OFFSET, label.len() as i32);
                self.metadata_buffer
                    .try_put_bytes(meta_offset + LABEL_OFFSET + 4, label.as_bytes())?;

                // Reset counter value to 0
                let val_offset = id * COUNTER_LENGTH;
                self.values_buffer.put_i64_volatile(val_offset, 0);

                // Commit allocation state
                self.metadata_buffer
                    .put_i32_ordered(meta_offset + STATE_OFFSET, RECORD_ALLOCATED);

                return Ok(id as i32);
            }
        }

        Err(AgronaError::OutOfBounds {
            offset: self.max_counters,
            length: 1,
            capacity: self.max_counters,
        })
    }

    /// Free / reclaim an allocated counter by ID.
    pub fn free(&self, counter_id: i32) -> Result<()> {
        let id = counter_id as usize;
        if id >= self.max_counters {
            return Err(AgronaError::OutOfBounds {
                offset: id,
                length: 1,
                capacity: self.max_counters,
            });
        }
        let meta_offset = id * METADATA_LENGTH;
        self.metadata_buffer
            .put_i32_ordered(meta_offset + STATE_OFFSET, RECORD_RECLAIMED);
        Ok(())
    }

    /// Get the state of a counter by ID.
    pub fn counter_state(&self, counter_id: i32) -> Result<i32> {
        let id = counter_id as usize;
        if id >= self.max_counters {
            return Err(AgronaError::OutOfBounds {
                offset: id,
                length: 1,
                capacity: self.max_counters,
            });
        }
        let meta_offset = id * METADATA_LENGTH;
        Ok(self
            .metadata_buffer
            .get_i32_volatile(meta_offset + STATE_OFFSET))
    }

    /// Iterate over all currently allocated counters.
    pub fn for_each<F>(&self, mut consumer: F)
    where
        F: FnMut(i32, i32, &[u8], &str),
    {
        for id in 0..self.max_counters {
            let meta_offset = id * METADATA_LENGTH;
            let state = self
                .metadata_buffer
                .get_i32_volatile(meta_offset + STATE_OFFSET);

            if state == RECORD_ALLOCATED {
                let type_id = self.metadata_buffer.get_i32(meta_offset + TYPE_ID_OFFSET);

                let key_len = self.metadata_buffer.get_i32(meta_offset + KEY_OFFSET) as usize;
                let mut key_bytes = vec![0u8; key_len];
                let _ = self
                    .metadata_buffer
                    .try_get_bytes(meta_offset + KEY_OFFSET + 4, &mut key_bytes);

                let label_len = self.metadata_buffer.get_i32(meta_offset + LABEL_OFFSET) as usize;
                let mut label_bytes = vec![0u8; label_len];
                let _ = self
                    .metadata_buffer
                    .try_get_bytes(meta_offset + LABEL_OFFSET + 4, &mut label_bytes);
                let label = String::from_utf8_lossy(&label_bytes);

                consumer(id as i32, type_id, &key_bytes, &label);
            }
        }
    }

    /// Get a handle to an [`AtomicCounter`] for the given counter ID.
    #[allow(clippy::clone_on_copy)]
    pub fn counter(&self, counter_id: i32) -> AtomicCounter<'a> {
        AtomicCounter::new(self.values_buffer.clone(), counter_id)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn test_counters_manager_allocation_and_atomic_counter() {
        let mut meta_backing = [0u8; METADATA_LENGTH * 4];
        let mut val_backing = [0u8; COUNTER_LENGTH * 4];

        let meta_buf = AtomicBuffer::new(&mut meta_backing);
        let val_buf = AtomicBuffer::new(&mut val_backing);

        let manager = CountersManager::new(meta_buf, val_buf).unwrap();
        assert_eq!(manager.max_counters(), 4);

        // Allocate counter 0
        let c0_id = manager.allocate(100, b"key0", "bytes_received").unwrap();
        assert_eq!(c0_id, 0);

        // Allocate counter 1
        let c1_id = manager.allocate(101, b"key1", "bytes_sent").unwrap();
        assert_eq!(c1_id, 1);

        // Access via AtomicCounter handle
        let c0 = manager.counter(c0_id);
        assert_eq!(c0.get(), 0);
        c0.set(500);
        assert_eq!(c0.get(), 500);
        assert_eq!(c0.add(100), 500);
        assert_eq!(c0.get(), 600);
        assert_eq!(c0.increment(), 600);
        assert_eq!(c0.get(), 601);
        assert!(c0.compare_and_set(601, 1000));
        assert_eq!(c0.get(), 1000);

        // Verify iteration over allocated counters
        let mut counters_found = Vec::new();
        manager.for_each(|id, type_id, key, label| {
            counters_found.push((id, type_id, key.to_vec(), label.to_string()));
        });

        assert_eq!(counters_found.len(), 2);
        assert_eq!(
            counters_found[0],
            (0, 100, b"key0".to_vec(), "bytes_received".to_string())
        );
        assert_eq!(
            counters_found[1],
            (1, 101, b"key1".to_vec(), "bytes_sent".to_string())
        );

        // Free counter 0 and re-allocate
        manager.free(c0_id).unwrap();
        assert_eq!(manager.counter_state(c0_id).unwrap(), RECORD_RECLAIMED);

        let c2_id = manager
            .allocate(102, b"key2", "reallocated_counter")
            .unwrap();
        // Re-allocated counter should reuse slot 0
        assert_eq!(c2_id, 0);
    }
}
