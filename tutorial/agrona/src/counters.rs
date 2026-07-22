//! Learner stub: CountersManager and AtomicCounter implementations.

use crate::atomic_buffer::AtomicBuffer;
use crate::error::Result;

pub const METADATA_LENGTH: usize = 1024;
pub const COUNTER_LENGTH: usize = 64;
pub const MAX_KEY_LENGTH: usize = 512;
pub const MAX_LABEL_LENGTH: usize = 460;

pub const RECORD_UNUSED: i32 = 0;
pub const RECORD_ALLOCATED: i32 = 1;
pub const RECORD_RECLAIMED: i32 = -1;

pub const STATE_OFFSET: usize = 0;
pub const TYPE_ID_OFFSET: usize = 4;
pub const KEY_OFFSET: usize = 8;
pub const LABEL_OFFSET: usize = KEY_OFFSET + MAX_KEY_LENGTH;

#[derive(Clone)]
pub struct AtomicCounter<'a> {
    values_buffer: AtomicBuffer<'a>,
    counter_id: i32,
    offset: usize,
}

impl<'a> AtomicCounter<'a> {
    pub fn new(values_buffer: AtomicBuffer<'a>, counter_id: i32) -> Self {
        let offset = counter_id as usize * COUNTER_LENGTH;
        Self {
            values_buffer,
            counter_id,
            offset,
        }
    }

    pub fn id(&self) -> i32 {
        self.counter_id
    }

    pub fn get(&self) -> i64 {
        todo!("implement AtomicCounter::get")
    }

    pub fn set(&self, _value: i64) {
        todo!("implement AtomicCounter::set")
    }

    pub fn set_ordered(&self, _value: i64) {
        todo!("implement AtomicCounter::set_ordered")
    }

    pub fn add(&self, _delta: i64) -> i64 {
        todo!("implement AtomicCounter::add")
    }

    pub fn increment(&self) -> i64 {
        todo!("implement AtomicCounter::increment")
    }

    pub fn compare_and_set(&self, _expected: i64, _update: i64) -> bool {
        todo!("implement AtomicCounter::compare_and_set")
    }
}

#[derive(Clone)]
pub struct CountersManager<'a> {
    metadata_buffer: AtomicBuffer<'a>,
    values_buffer: AtomicBuffer<'a>,
    max_counters: usize,
}

impl<'a> CountersManager<'a> {
    pub fn new(_metadata_buffer: AtomicBuffer<'a>, _values_buffer: AtomicBuffer<'a>) -> Result<Self> {
        todo!("implement CountersManager::new")
    }

    pub fn max_counters(&self) -> usize {
        todo!("implement CountersManager::max_counters")
    }

    pub fn allocate(&self, _type_id: i32, _key: &[u8], _label: &str) -> Result<i32> {
        todo!("implement CountersManager::allocate")
    }

    pub fn free(&self, _counter_id: i32) -> Result<()> {
        todo!("implement CountersManager::free")
    }

    pub fn counter_state(&self, _counter_id: i32) -> Result<i32> {
        todo!("implement CountersManager::counter_state")
    }

    pub fn for_each<F>(&self, _consumer: F)
    where
        F: FnMut(i32, i32, &[u8], &str),
    {
        todo!("implement CountersManager::for_each")
    }

    pub fn counter(&self, counter_id: i32) -> AtomicCounter<'a> {
        AtomicCounter::new(self.values_buffer, counter_id)
    }
}
