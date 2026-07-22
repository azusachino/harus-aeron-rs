//! Learner stub: AtomicBuffer implementation.

use crate::error::Result;

#[derive(Copy, Clone)]
pub struct AtomicBuffer<'a> {
    ptr: *mut u8,
    len: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

unsafe impl<'a> Send for AtomicBuffer<'a> {}
unsafe impl<'a> Sync for AtomicBuffer<'a> {}

impl<'a> AtomicBuffer<'a> {
    pub fn new(_slice: &'a mut [u8]) -> Self {
        todo!("implement AtomicBuffer::new")
    }

    pub fn from_slice(_slice: &'a [u8]) -> Self {
        todo!("implement AtomicBuffer::from_slice")
    }

    pub unsafe fn from_raw_parts(_ptr: *mut u8, _len: usize) -> Self {
        todo!("implement AtomicBuffer::from_raw_parts")
    }

    pub fn capacity(&self) -> usize {
        todo!("implement AtomicBuffer::capacity")
    }

    pub fn wrap(&mut self, _slice: &'a mut [u8]) {
        todo!("implement AtomicBuffer::wrap")
    }

    pub fn wrap_slice(&mut self, _slice: &'a [u8]) {
        todo!("implement AtomicBuffer::wrap_slice")
    }

    pub fn as_slice(&self) -> &[u8] {
        todo!("implement AtomicBuffer::as_slice")
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        todo!("implement AtomicBuffer::as_mut_slice")
    }

    pub fn try_get_i32(&self, _offset: usize) -> Result<i32> {
        todo!("implement AtomicBuffer::try_get_i32")
    }

    pub fn get_i32(&self, _offset: usize) -> i32 {
        todo!("implement AtomicBuffer::get_i32")
    }

    pub fn try_put_i32(&self, _offset: usize, _value: i32) -> Result<()> {
        todo!("implement AtomicBuffer::try_put_i32")
    }

    pub fn put_i32(&self, _offset: usize, _value: i32) {
        todo!("implement AtomicBuffer::put_i32")
    }

    pub fn try_get_i64(&self, _offset: usize) -> Result<i64> {
        todo!("implement AtomicBuffer::try_get_i64")
    }

    pub fn get_i64(&self, _offset: usize) -> i64 {
        todo!("implement AtomicBuffer::get_i64")
    }

    pub fn try_put_i64(&self, _offset: usize, _value: i64) -> Result<()> {
        todo!("implement AtomicBuffer::try_put_i64")
    }

    pub fn put_i64(&self, _offset: usize, _value: i64) {
        todo!("implement AtomicBuffer::put_i64")
    }

    pub fn get_i64_volatile(&self, _offset: usize) -> i64 {
        todo!("implement AtomicBuffer::get_i64_volatile")
    }

    pub fn put_i64_volatile(&self, _offset: usize, _value: i64) {
        todo!("implement AtomicBuffer::put_i64_volatile")
    }

    pub fn put_i64_ordered(&self, _offset: usize, _value: i64) {
        todo!("implement AtomicBuffer::put_i64_ordered")
    }

    pub fn compare_and_set_i64(&self, _offset: usize, _expected: i64, _update: i64) -> bool {
        todo!("implement AtomicBuffer::compare_and_set_i64")
    }

    pub fn get_and_add_i64(&self, _offset: usize, _delta: i64) -> i64 {
        todo!("implement AtomicBuffer::get_and_add_i64")
    }

    pub fn try_get_string_utf8(&self, _offset: usize) -> Result<String> {
        todo!("implement AtomicBuffer::try_get_string_utf8")
    }

    pub fn get_string_utf8(&self, _offset: usize) -> String {
        todo!("implement AtomicBuffer::get_string_utf8")
    }

    pub fn try_put_string_utf8(&self, _offset: usize, _value: &str) -> Result<usize> {
        todo!("implement AtomicBuffer::try_put_string_utf8")
    }

    pub fn put_string_utf8(&self, _offset: usize, _value: &str) -> usize {
        todo!("implement AtomicBuffer::put_string_utf8")
    }

    pub fn try_get_bytes(&self, _offset: usize, _dest: &mut [u8]) -> Result<()> {
        todo!("implement AtomicBuffer::try_get_bytes")
    }

    pub fn get_bytes(&self, _offset: usize, _dest: &mut [u8]) {
        todo!("implement AtomicBuffer::get_bytes")
    }

    pub fn try_put_bytes(&self, _offset: usize, _src: &[u8]) -> Result<()> {
        todo!("implement AtomicBuffer::try_put_bytes")
    }

    pub fn put_bytes(&self, _offset: usize, _src: &[u8]) {
        todo!("implement AtomicBuffer::put_bytes")
    }
}
