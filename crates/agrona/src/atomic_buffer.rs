//! Atomic buffer wrapper providing checked and atomic access over a byte segment.
//!
//! Mirrors `org.agrona.concurrent.AtomicBuffer` and `org.agrona.concurrent.UnsafeBuffer`
//! from Java Agrona. Accesses are strictly little-endian.

#[cfg(not(all(test, loom)))]
use std::sync::atomic::Ordering;

use crate::error::{AgronaError, Result};

#[cfg(not(all(test, loom)))]
#[derive(Copy, Clone)]
pub struct AtomicBuffer<'a> {
    ptr: *mut u8,
    len: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

#[cfg(all(test, loom))]
#[derive(Clone)]
pub struct AtomicBuffer<'a> {
    inner: std::sync::Arc<Vec<loom::sync::atomic::AtomicI64>>,
    _marker: std::marker::PhantomData<&'a ()>,
}

unsafe impl<'a> Send for AtomicBuffer<'a> {}
unsafe impl<'a> Sync for AtomicBuffer<'a> {}

#[cfg(not(all(test, loom)))]
impl<'a> AtomicBuffer<'a> {
    /// Create a new `AtomicBuffer` wrapping a mutable slice.
    pub fn new(slice: &'a mut [u8]) -> Self {
        Self {
            ptr: slice.as_mut_ptr(),
            len: slice.len(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new `AtomicBuffer` wrapping a read-only slice.
    ///
    /// # Safety
    /// Calling mutators on a buffer created from a read-only slice is undefined behavior
    /// unless the underlying memory is mutable (e.g. shared memory). Use with caution.
    pub fn from_slice(slice: &'a [u8]) -> Self {
        Self {
            ptr: slice.as_ptr() as *mut u8,
            len: slice.len(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new `AtomicBuffer` from raw parts.
    ///
    /// # Safety
    /// The caller must ensure that the pointer is valid and represents at least `len` bytes
    /// for the duration of the lifetime `'a`.
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize) -> Self {
        Self {
            ptr,
            len,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the capacity of the buffer in bytes.
    pub fn capacity(&self) -> usize {
        self.len
    }

    /// Re-wrap the buffer over a mutable slice.
    pub fn wrap(&mut self, slice: &'a mut [u8]) {
        self.ptr = slice.as_mut_ptr();
        self.len = slice.len();
    }

    /// Re-wrap the buffer over a read-only slice.
    pub fn wrap_slice(&mut self, slice: &'a [u8]) {
        self.ptr = slice.as_ptr() as *mut u8;
        self.len = slice.len();
    }

    /// Returns a slice view of the underlying memory.
    ///
    /// # Safety
    /// The caller must ensure that no concurrent mutations occur on the buffer while the slice
    /// is alive.
    pub unsafe fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Returns a mutable slice view of the underlying memory.
    ///
    /// # Safety
    /// The caller must ensure that no other references exist to this buffer, and no concurrent
    /// accesses occur.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    #[inline]
    fn check_bounds(&self, offset: usize, length: usize) -> Result<()> {
        if offset.checked_add(length).is_none_or(|end| end > self.len) {
            return Err(AgronaError::OutOfBounds {
                offset,
                length,
                capacity: self.len,
            });
        }
        Ok(())
    }

    #[inline]
    fn check_alignment(&self, offset: usize, required_alignment: usize) -> Result<()> {
        if (self.ptr as usize + offset) % required_alignment != 0 {
            return Err(AgronaError::Alignment {
                offset,
                required_alignment,
            });
        }
        Ok(())
    }

    /// Try to get a slice of bytes from the specified offset.
    pub fn try_get_bytes(&self, offset: usize, dest: &mut [u8]) -> Result<()> {
        self.check_bounds(offset, dest.len())?;
        unsafe {
            let src_ptr = self.ptr.add(offset);
            std::ptr::copy_nonoverlapping(src_ptr, dest.as_mut_ptr(), dest.len());
        }
        Ok(())
    }

    /// Get a slice of bytes from the specified offset. Panics on bounds error.
    pub fn get_bytes(&self, offset: usize, dest: &mut [u8]) {
        self.try_get_bytes(offset, dest)
            .expect("Agrona operation failed");
    }

    /// Try to put a slice of bytes at the specified offset.
    pub fn try_put_bytes(&self, offset: usize, src: &[u8]) -> Result<()> {
        self.check_bounds(offset, src.len())?;
        unsafe {
            let dest_ptr = self.ptr.add(offset);
            std::ptr::copy_nonoverlapping(src.as_ptr(), dest_ptr, src.len());
        }
        Ok(())
    }

    /// Put a slice of bytes at the specified offset. Panics on bounds error.
    pub fn put_bytes(&self, offset: usize, src: &[u8]) {
        self.try_put_bytes(offset, src)
            .expect("Agrona operation failed");
    }

    /// Copy bytes from another AtomicBuffer to this one.
    pub fn try_put_bytes_from(
        &self,
        offset: usize,
        src: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> Result<()> {
        self.check_bounds(offset, length)?;
        src.check_bounds(src_offset, length)?;
        unsafe {
            std::ptr::copy_nonoverlapping(src.ptr.add(src_offset), self.ptr.add(offset), length);
        }
        Ok(())
    }

    /// Copy bytes from another AtomicBuffer to this one. Panics on bounds error.
    pub fn put_bytes_from(
        &self,
        offset: usize,
        src: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) {
        self.try_put_bytes_from(offset, src, src_offset, length)
            .expect("Agrona operation failed");
    }
}

#[cfg(all(test, loom))]
impl<'a> AtomicBuffer<'a> {
    pub fn new_loom(len: usize) -> AtomicBuffer<'static> {
        let num_words = (len + 7) / 8;
        let mut words = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            words.push(loom::sync::atomic::AtomicI64::new(0));
        }
        AtomicBuffer {
            inner: std::sync::Arc::new(words),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn new(slice: &'a mut [u8]) -> Self {
        let len = slice.len();
        let num_words = (len + 7) / 8;
        let mut words = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let mut chunk = [0u8; 8];
            let start = i * 8;
            let end = std::cmp::min(start + 8, len);
            chunk[..end - start].copy_from_slice(&slice[start..end]);
            words.push(loom::sync::atomic::AtomicI64::new(i64::from_le_bytes(
                chunk,
            )));
        }
        Self {
            inner: std::sync::Arc::new(words),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn from_slice(slice: &'a [u8]) -> Self {
        let len = slice.len();
        let num_words = (len + 7) / 8;
        let mut words = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let mut chunk = [0u8; 8];
            let start = i * 8;
            let end = std::cmp::min(start + 8, len);
            chunk[..end - start].copy_from_slice(&slice[start..end]);
            words.push(loom::sync::atomic::AtomicI64::new(i64::from_le_bytes(
                chunk,
            )));
        }
        Self {
            inner: std::sync::Arc::new(words),
            _marker: std::marker::PhantomData,
        }
    }

    pub unsafe fn from_raw_parts(_ptr: *mut u8, len: usize) -> Self {
        Self::new_loom(len)
    }

    pub fn capacity(&self) -> usize {
        self.inner.len() * 8
    }

    #[inline]
    fn check_bounds(&self, offset: usize, length: usize) -> Result<()> {
        let cap = self.capacity();
        if offset.checked_add(length).is_none_or(|end| end > cap) {
            return Err(AgronaError::OutOfBounds {
                offset,
                length,
                capacity: cap,
            });
        }
        Ok(())
    }

    #[inline]
    fn check_alignment(&self, offset: usize, required_alignment: usize) -> Result<()> {
        if offset % required_alignment != 0 {
            return Err(AgronaError::Alignment {
                offset,
                required_alignment,
            });
        }
        Ok(())
    }

    pub fn wrap(&mut self, slice: &'a mut [u8]) {
        *self = Self::new(slice);
    }

    pub fn wrap_slice(&mut self, slice: &'a [u8]) {
        *self = Self::from_slice(slice);
    }

    pub fn as_slice(&self) -> &[u8] {
        panic!("as_slice is not supported under Loom");
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        panic!("as_mut_slice is not supported under Loom");
    }

    pub fn try_get_bytes(&self, offset: usize, dest: &mut [u8]) -> Result<()> {
        self.check_bounds(offset, dest.len())?;
        for (i, b) in dest.iter_mut().enumerate() {
            let word_idx = (offset + i) / 8;
            let byte_shift = ((offset + i) % 8) * 8;
            let word = self.inner[word_idx].load(loom::sync::atomic::Ordering::Relaxed);
            *b = ((word >> byte_shift) & 0xFF) as u8;
        }
        Ok(())
    }

    pub fn get_bytes(&self, offset: usize, dest: &mut [u8]) {
        self.try_get_bytes(offset, dest)
            .expect("Agrona operation failed");
    }

    pub fn try_put_bytes(&self, offset: usize, src: &[u8]) -> Result<()> {
        self.check_bounds(offset, src.len())?;
        for (i, &b) in src.iter().enumerate() {
            let word_idx = (offset + i) / 8;
            let byte_shift = ((offset + i) % 8) * 8;
            let mask = !(0xFFi64 << byte_shift);
            let val = (b as i64) << byte_shift;
            let _ = self.inner[word_idx].fetch_update(
                loom::sync::atomic::Ordering::Relaxed,
                loom::sync::atomic::Ordering::Relaxed,
                |old| Some((old & mask) | val),
            );
        }
        Ok(())
    }

    pub fn put_bytes(&self, offset: usize, src: &[u8]) {
        self.try_put_bytes(offset, src)
            .expect("Agrona operation failed");
    }

    pub fn try_put_bytes_from(
        &self,
        offset: usize,
        src: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) -> Result<()> {
        let mut bytes = vec![0u8; length];
        src.try_get_bytes(src_offset, &mut bytes)?;
        self.try_put_bytes(offset, &bytes)
    }

    pub fn put_bytes_from(
        &self,
        offset: usize,
        src: &AtomicBuffer,
        src_offset: usize,
        length: usize,
    ) {
        self.try_put_bytes_from(offset, src, src_offset, length)
            .expect("Agrona operation failed");
    }
}

#[cfg(not(all(test, loom)))]
macro_rules! impl_primitive {
    ($t:ty, $get_name:ident, $try_get_name:ident, $put_name:ident, $try_put_name:ident, $size:expr) => {
        impl<'a> AtomicBuffer<'a> {
            #[doc = concat!("Try to get a `", stringify!($t), "` value at the specified offset using non-atomic load.")]
            pub fn $try_get_name(&self, offset: usize) -> Result<$t> {
                self.check_bounds(offset, $size)?;
                unsafe {
                    let ptr = self.ptr.add(offset);
                    let bytes = std::ptr::read_unaligned(ptr as *const [u8; $size]);
                    Ok(<$t>::from_le_bytes(bytes))
                }
            }

            #[doc = concat!("Get a `", stringify!($t), "` value at the specified offset using non-atomic load. Panics on bounds error.")]
            pub fn $get_name(&self, offset: usize) -> $t {
                self.$try_get_name(offset).expect("Agrona operation failed")
            }

            #[doc = concat!("Try to put a `", stringify!($t), "` value at the specified offset using non-atomic store.")]
            pub fn $try_put_name(&self, offset: usize, value: $t) -> Result<()> {
                self.check_bounds(offset, $size)?;
                unsafe {
                    let ptr = self.ptr.add(offset);
                    let bytes = value.to_le_bytes();
                    std::ptr::write_unaligned(ptr as *mut [u8; $size], bytes);
                }
                Ok(())
            }

            #[doc = concat!("Put a `", stringify!($t), "` value at the specified offset using non-atomic store. Panics on bounds error.")]
            pub fn $put_name(&self, offset: usize, value: $t) {
                self.$try_put_name(offset, value).expect("Agrona operation failed")
            }
        }
    };
}

#[cfg(all(test, loom))]
macro_rules! impl_primitive {
    ($t:ty, $get_name:ident, $try_get_name:ident, $put_name:ident, $try_put_name:ident, $size:expr) => {
        impl<'a> AtomicBuffer<'a> {
            pub fn $try_get_name(&self, offset: usize) -> Result<$t> {
                self.check_bounds(offset, $size)?;
                let mut bytes = [0u8; $size];
                self.try_get_bytes(offset, &mut bytes)?;
                Ok(<$t>::from_le_bytes(bytes))
            }

            pub fn $get_name(&self, offset: usize) -> $t {
                self.$try_get_name(offset).expect("Agrona operation failed")
            }

            pub fn $try_put_name(&self, offset: usize, value: $t) -> Result<()> {
                self.check_bounds(offset, $size)?;
                let bytes = value.to_le_bytes();
                self.try_put_bytes(offset, &bytes)?;
                Ok(())
            }

            pub fn $put_name(&self, offset: usize, value: $t) {
                self.$try_put_name(offset, value)
                    .expect("Agrona operation failed")
            }
        }
    };
}

#[cfg(not(all(test, loom)))]
macro_rules! impl_atomic_primitive {
    ($t:ty, $atomic_t:ty,
     $get_vol:ident, $try_get_vol:ident,
     $put_vol:ident, $try_put_vol:ident,
     $put_ord:ident, $try_put_ord:ident,
     $size:expr) => {
        impl<'a> AtomicBuffer<'a> {
            #[doc = concat!("Try to get a `", stringify!($t), "` value at the specified offset using volatile (acquire/seq_cst) load.")]
            pub fn $try_get_vol(&self, offset: usize) -> Result<$t> {
                self.check_bounds(offset, $size)?;
                self.check_alignment(offset, $size)?;
                unsafe {
                    let ptr = self.ptr.add(offset) as *const $atomic_t;
                    let val = (*ptr).load(Ordering::SeqCst);
                    Ok(<$t>::from_le(val))
                }
            }

            #[doc = concat!("Get a `", stringify!($t), "` value at the specified offset using volatile load. Panics on bounds/alignment error.")]
            pub fn $get_vol(&self, offset: usize) -> $t {
                self.$try_get_vol(offset).expect("Agrona operation failed")
            }

            #[doc = concat!("Try to put a `", stringify!($t), "` value at the specified offset using volatile (release/seq_cst) store.")]
            pub fn $try_put_vol(&self, offset: usize, value: $t) -> Result<()> {
                self.check_bounds(offset, $size)?;
                self.check_alignment(offset, $size)?;
                unsafe {
                    let ptr = self.ptr.add(offset) as *const $atomic_t;
                    let val_le = value.to_le();
                    (*ptr).store(val_le, Ordering::SeqCst);
                }
                Ok(())
            }

            #[doc = concat!("Put a `", stringify!($t), "` value at the specified offset using volatile store. Panics on bounds/alignment error.")]
            pub fn $put_vol(&self, offset: usize, value: $t) {
                self.$try_put_vol(offset, value).expect("Agrona operation failed")
            }

            #[doc = concat!("Try to put a `", stringify!($t), "` value at the specified offset using ordered (release) store.")]
            pub fn $try_put_ord(&self, offset: usize, value: $t) -> Result<()> {
                self.check_bounds(offset, $size)?;
                self.check_alignment(offset, $size)?;
                unsafe {
                    let ptr = self.ptr.add(offset) as *const $atomic_t;
                    let val_le = value.to_le();
                    (*ptr).store(val_le, Ordering::Release);
                }
                Ok(())
            }

            #[doc = concat!("Put a `", stringify!($t), "` value at the specified offset using ordered store. Panics on bounds/alignment error.")]
            pub fn $put_ord(&self, offset: usize, value: $t) {
                self.$try_put_ord(offset, value).expect("Agrona operation failed")
            }
        }
    };
}

#[cfg(all(test, loom))]
macro_rules! impl_atomic_primitive {
    ($t:ty, $atomic_t:ty,
     $get_vol:ident, $try_get_vol:ident,
     $put_vol:ident, $try_put_vol:ident,
     $put_ord:ident, $try_put_ord:ident,
     $size:expr) => {
        impl<'a> AtomicBuffer<'a> {
            pub fn $try_get_vol(&self, offset: usize) -> Result<$t> {
                self.check_bounds(offset, $size)?;
                if $size == 8 {
                    let idx = offset / 8;
                    let val = self.inner[idx].load(loom::sync::atomic::Ordering::Acquire);
                    let bytes = val.to_le_bytes();
                    Ok(<$t>::from_le_bytes(bytes[..$size].try_into().unwrap()))
                } else if $size == 4 {
                    let idx = offset / 8;
                    let val = self.inner[idx].load(loom::sync::atomic::Ordering::Acquire);
                    let is_low = offset % 8 == 0;
                    let val_32 = if is_low {
                        val as u32
                    } else {
                        (val >> 32) as u32
                    };
                    let bytes = val_32.to_le_bytes();
                    Ok(<$t>::from_le_bytes(bytes[..$size].try_into().unwrap()))
                } else {
                    let mut bytes = [0u8; $size];
                    self.try_get_bytes(offset, &mut bytes)?;
                    Ok(<$t>::from_le_bytes(bytes))
                }
            }

            pub fn $get_vol(&self, offset: usize) -> $t {
                self.$try_get_vol(offset).expect("Agrona operation failed")
            }

            pub fn $try_put_vol(&self, offset: usize, value: $t) -> Result<()> {
                self.check_bounds(offset, $size)?;
                if $size == 8 {
                    let idx = offset / 8;
                    let bytes = value.to_le_bytes();
                    let val_i64 = i64::from_le_bytes(bytes[..8].try_into().unwrap());
                    self.inner[idx].store(val_i64, loom::sync::atomic::Ordering::SeqCst);
                } else if $size == 4 {
                    let idx = offset / 8;
                    let is_low = offset % 8 == 0;
                    let bytes = value.to_le_bytes();
                    let val_u32 = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64;
                    let _ = self.inner[idx].fetch_update(
                        loom::sync::atomic::Ordering::SeqCst,
                        loom::sync::atomic::Ordering::Relaxed,
                        |old| {
                            let old_u64 = old as u64;
                            let new_u64 = if is_low {
                                (old_u64 & 0xFFFF_FFFF_0000_0000) | val_u32
                            } else {
                                (old_u64 & 0x0000_0000_FFFF_FFFF) | (val_u32 << 32)
                            };
                            Some(new_u64 as i64)
                        },
                    );
                } else {
                    let bytes = value.to_le_bytes();
                    self.try_put_bytes(offset, &bytes)?;
                }
                Ok(())
            }

            pub fn $put_vol(&self, offset: usize, value: $t) {
                self.$try_put_vol(offset, value)
                    .expect("Agrona operation failed")
            }

            pub fn $try_put_ord(&self, offset: usize, value: $t) -> Result<()> {
                self.check_bounds(offset, $size)?;
                if $size == 8 {
                    let idx = offset / 8;
                    let bytes = value.to_le_bytes();
                    let val_i64 = i64::from_le_bytes(bytes[..8].try_into().unwrap());
                    self.inner[idx].store(val_i64, loom::sync::atomic::Ordering::Release);
                } else if $size == 4 {
                    let idx = offset / 8;
                    let is_low = offset % 8 == 0;
                    let bytes = value.to_le_bytes();
                    let val_u32 = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64;
                    let _ = self.inner[idx].fetch_update(
                        loom::sync::atomic::Ordering::Release,
                        loom::sync::atomic::Ordering::Relaxed,
                        |old| {
                            let old_u64 = old as u64;
                            let new_u64 = if is_low {
                                (old_u64 & 0xFFFF_FFFF_0000_0000) | val_u32
                            } else {
                                (old_u64 & 0x0000_0000_FFFF_FFFF) | (val_u32 << 32)
                            };
                            Some(new_u64 as i64)
                        },
                    );
                } else {
                    let bytes = value.to_le_bytes();
                    self.try_put_bytes(offset, &bytes)?;
                }
                Ok(())
            }

            pub fn $put_ord(&self, offset: usize, value: $t) {
                self.$try_put_ord(offset, value)
                    .expect("Agrona operation failed")
            }
        }
    };
}

impl_primitive!(i8, get_i8, try_get_i8, put_i8, try_put_i8, 1);
impl_primitive!(u8, get_u8, try_get_u8, put_u8, try_put_u8, 1);
impl_primitive!(i16, get_i16, try_get_i16, put_i16, try_put_i16, 2);
impl_primitive!(u16, get_u16, try_get_u16, put_u16, try_put_u16, 2);
impl_primitive!(i32, get_i32, try_get_i32, put_i32, try_put_i32, 4);
impl_primitive!(u32, get_u32, try_get_u32, put_u32, try_put_u32, 4);
impl_primitive!(i64, get_i64, try_get_i64, put_i64, try_put_i64, 8);
impl_primitive!(u64, get_u64, try_get_u64, put_u64, try_put_u64, 8);
impl_primitive!(f32, get_f32, try_get_f32, put_f32, try_put_f32, 4);
impl_primitive!(f64, get_f64, try_get_f64, put_f64, try_put_f64, 8);

impl_atomic_primitive!(
    i8,
    std::sync::atomic::AtomicI8,
    get_i8_volatile,
    try_get_i8_volatile,
    put_i8_volatile,
    try_put_i8_volatile,
    put_i8_ordered,
    try_put_i8_ordered,
    1
);
impl_atomic_primitive!(
    u8,
    std::sync::atomic::AtomicU8,
    get_u8_volatile,
    try_get_u8_volatile,
    put_u8_volatile,
    try_put_u8_volatile,
    put_u8_ordered,
    try_put_u8_ordered,
    1
);
impl_atomic_primitive!(
    i16,
    std::sync::atomic::AtomicI16,
    get_i16_volatile,
    try_get_i16_volatile,
    put_i16_volatile,
    try_put_i16_volatile,
    put_i16_ordered,
    try_put_i16_ordered,
    2
);
impl_atomic_primitive!(
    u16,
    std::sync::atomic::AtomicU16,
    get_u16_volatile,
    try_get_u16_volatile,
    put_u16_volatile,
    try_put_u16_volatile,
    put_u16_ordered,
    try_put_u16_ordered,
    2
);
impl_atomic_primitive!(
    i32,
    std::sync::atomic::AtomicI32,
    get_i32_volatile,
    try_get_i32_volatile,
    put_i32_volatile,
    try_put_i32_volatile,
    put_i32_ordered,
    try_put_i32_ordered,
    4
);
impl_atomic_primitive!(
    u32,
    std::sync::atomic::AtomicU32,
    get_u32_volatile,
    try_get_u32_volatile,
    put_u32_volatile,
    try_put_u32_volatile,
    put_u32_ordered,
    try_put_u32_ordered,
    4
);
impl_atomic_primitive!(
    i64,
    std::sync::atomic::AtomicI64,
    get_i64_volatile,
    try_get_i64_volatile,
    put_i64_volatile,
    try_put_i64_volatile,
    put_i64_ordered,
    try_put_i64_ordered,
    8
);
impl_atomic_primitive!(
    u64,
    std::sync::atomic::AtomicU64,
    get_u64_volatile,
    try_get_u64_volatile,
    put_u64_volatile,
    try_put_u64_volatile,
    put_u64_ordered,
    try_put_u64_ordered,
    8
);

impl<'a> AtomicBuffer<'a> {
    pub fn try_get_string_utf8(&self, offset: usize) -> Result<String> {
        let length = self.try_get_i32(offset)? as usize;
        self.check_bounds(offset + 4, length)?;
        let mut bytes = vec![0u8; length];
        self.try_get_bytes(offset + 4, &mut bytes)?;
        String::from_utf8(bytes).map_err(AgronaError::InvalidUtf8)
    }

    pub fn get_string_utf8(&self, offset: usize) -> String {
        self.try_get_string_utf8(offset)
            .expect("Agrona operation failed")
    }

    pub fn try_put_string_utf8(&self, offset: usize, value: &str) -> Result<usize> {
        let bytes = value.as_bytes();
        let length = bytes.len();
        self.check_bounds(offset, 4 + length)?;
        self.try_put_i32(offset, length as i32)?;
        self.try_put_bytes(offset + 4, bytes)?;
        Ok(4 + length)
    }

    pub fn put_string_utf8(&self, offset: usize, value: &str) -> usize {
        self.try_put_string_utf8(offset, value)
            .expect("Agrona operation failed")
    }

    /// Try to compare and set (CAS) an `i32` value at the specified offset.
    pub fn try_compare_and_set_i32(
        &self,
        offset: usize,
        expected: i32,
        update: i32,
    ) -> Result<bool> {
        self.check_bounds(offset, 4)?;
        self.check_alignment(offset, 4)?;
        #[cfg(not(all(test, loom)))]
        unsafe {
            let ptr = self.ptr.add(offset) as *const std::sync::atomic::AtomicI32;
            let expected_le = expected.to_le();
            let update_le = update.to_le();
            match (*ptr).compare_exchange(
                expected_le,
                update_le,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            }
        }
        #[cfg(all(test, loom))]
        {
            let idx = offset / 8;
            let is_low = offset % 8 == 0;
            let exp_u32 = expected as u32 as u64;
            let upd_u32 = update as u32 as u64;
            let res = self.inner[idx]
                .fetch_update(
                    loom::sync::atomic::Ordering::AcqRel,
                    loom::sync::atomic::Ordering::Acquire,
                    |old| {
                        let old_u64 = old as u64;
                        let current_val = if is_low {
                            old_u64 & 0xFFFFFFFF
                        } else {
                            old_u64 >> 32
                        };
                        if current_val == exp_u32 {
                            let new_u64 = if is_low {
                                (old_u64 & 0xFFFF_FFFF_0000_0000) | upd_u32
                            } else {
                                (old_u64 & 0x0000_0000_FFFF_FFFF) | (upd_u32 << 32)
                            };
                            Some(new_u64 as i64)
                        } else {
                            None
                        }
                    },
                )
                .is_ok();
            Ok(res)
        }
    }

    /// Compare and set (CAS) an `i32` value. Panics on bounds/alignment error.
    pub fn compare_and_set_i32(&self, offset: usize, expected: i32, update: i32) -> bool {
        self.try_compare_and_set_i32(offset, expected, update)
            .expect("Agrona operation failed")
    }

    /// Try to compare and set (CAS) an `i64` value at the specified offset.
    pub fn try_compare_and_set_i64(
        &self,
        offset: usize,
        expected: i64,
        update: i64,
    ) -> Result<bool> {
        self.check_bounds(offset, 8)?;
        self.check_alignment(offset, 8)?;
        #[cfg(not(all(test, loom)))]
        unsafe {
            let ptr = self.ptr.add(offset) as *const std::sync::atomic::AtomicI64;
            let expected_le = expected.to_le();
            let update_le = update.to_le();
            match (*ptr).compare_exchange(
                expected_le,
                update_le,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            }
        }
        #[cfg(all(test, loom))]
        {
            let idx = offset / 8;
            Ok(self.inner[idx]
                .compare_exchange(
                    expected,
                    update,
                    loom::sync::atomic::Ordering::AcqRel,
                    loom::sync::atomic::Ordering::Acquire,
                )
                .is_ok())
        }
    }

    /// Compare and set (CAS) an `i64` value. Panics on bounds/alignment error.
    pub fn compare_and_set_i64(&self, offset: usize, expected: i64, update: i64) -> bool {
        self.try_compare_and_set_i64(offset, expected, update)
            .expect("Agrona operation failed")
    }

    /// Try to atomically fetch and add an `i32` value at the specified offset.
    ///
    /// Returns the previous value.
    pub fn try_get_and_add_i32(&self, offset: usize, delta: i32) -> Result<i32> {
        self.check_bounds(offset, 4)?;
        self.check_alignment(offset, 4)?;
        #[cfg(not(all(test, loom)))]
        unsafe {
            let ptr = self.ptr.add(offset) as *const std::sync::atomic::AtomicI32;
            let mut current = (*ptr).load(Ordering::SeqCst);
            loop {
                let current_val = i32::from_le(current);
                let next_val = current_val.wrapping_add(delta);
                let next_le = next_val.to_le();
                match (*ptr).compare_exchange_weak(
                    current,
                    next_le,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Ok(current_val),
                    Err(actual) => current = actual,
                }
            }
        }
        #[cfg(all(test, loom))]
        {
            let idx = offset / 8;
            let is_low = offset % 8 == 0;
            let mut prev_val = 0i32;
            let _ = self.inner[idx].fetch_update(
                loom::sync::atomic::Ordering::AcqRel,
                loom::sync::atomic::Ordering::Acquire,
                |old| {
                    let old_u64 = old as u64;
                    let current_i32 = if is_low {
                        old_u64 as i32
                    } else {
                        (old_u64 >> 32) as i32
                    };
                    prev_val = current_i32;
                    let next_i32 = current_i32.wrapping_add(delta);
                    let next_u32 = next_i32 as u32 as u64;
                    let new_u64 = if is_low {
                        (old_u64 & 0xFFFF_FFFF_0000_0000) | next_u32
                    } else {
                        (old_u64 & 0x0000_0000_FFFF_FFFF) | (next_u32 << 32)
                    };
                    Some(new_u64 as i64)
                },
            );
            Ok(prev_val)
        }
    }

    /// Atomically fetch and add an `i32` value. Panics on bounds/alignment error.
    pub fn get_and_add_i32(&self, offset: usize, delta: i32) -> i32 {
        self.try_get_and_add_i32(offset, delta)
            .expect("Agrona operation failed")
    }

    /// Try to atomically fetch and add an `i64` value at the specified offset.
    ///
    /// Returns the previous value.
    pub fn try_get_and_add_i64(&self, offset: usize, delta: i64) -> Result<i64> {
        self.check_bounds(offset, 8)?;
        self.check_alignment(offset, 8)?;
        #[cfg(not(all(test, loom)))]
        unsafe {
            let ptr = self.ptr.add(offset) as *const std::sync::atomic::AtomicI64;
            let mut current = (*ptr).load(Ordering::SeqCst);
            loop {
                let current_val = i64::from_le(current);
                let next_val = current_val.wrapping_add(delta);
                let next_le = next_val.to_le();
                match (*ptr).compare_exchange_weak(
                    current,
                    next_le,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Ok(current_val),
                    Err(actual) => current = actual,
                }
            }
        }
        #[cfg(all(test, loom))]
        {
            let idx = offset / 8;
            Ok(self.inner[idx].fetch_add(delta, loom::sync::atomic::Ordering::AcqRel))
        }
    }

    /// Atomically fetch and add an `i64` value. Panics on bounds/alignment error.
    pub fn get_and_add_i64(&self, offset: usize, delta: i64) -> i64 {
        self.try_get_and_add_i64(offset, delta)
            .expect("Agrona operation failed")
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn test_endianness_and_basic_ops() {
        let mut backing = [0u8; 16];
        {
            let buf = AtomicBuffer::new(&mut backing);
            buf.put_i32(0, 0x12345678);
            assert_eq!(buf.get_i32(0), 0x12345678);
            buf.put_i64(8, 0x0102030405060708);
            assert_eq!(buf.get_i64(8), 0x0102030405060708);
        }
        // Assert little-endian byte ordering in storage after buf is dropped
        assert_eq!(backing[0..4], [0x78, 0x56, 0x34, 0x12]);
        assert_eq!(
            backing[8..16],
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        );
    }

    #[test]
    fn test_volatile_and_ordered_ops() {
        let mut backing = [0u8; 16];
        let buf = AtomicBuffer::new(&mut backing);

        buf.put_i32_volatile(0, 42);
        assert_eq!(buf.get_i32_volatile(0), 42);

        buf.put_i32_ordered(4, 100);
        assert_eq!(buf.get_i32_volatile(4), 100);

        buf.put_i64_volatile(8, 999);
        assert_eq!(buf.get_i64_volatile(8), 999);
    }

    #[test]
    fn test_cas_ops() {
        let mut backing = [0u8; 16];
        let buf = AtomicBuffer::new(&mut backing);

        // Test CAS i32
        assert!(buf.compare_and_set_i32(0, 0, 42));
        assert_eq!(buf.get_i32(0), 42);
        assert!(!buf.compare_and_set_i32(0, 0, 99)); // expected doesn't match
        assert_eq!(buf.get_i32(0), 42);

        // Test CAS i64
        assert!(buf.compare_and_set_i64(8, 0, 9999));
        assert_eq!(buf.get_i64(8), 9999);
    }

    #[test]
    fn test_get_and_add_ops() {
        let mut backing = [0u8; 16];
        let buf = AtomicBuffer::new(&mut backing);

        assert_eq!(buf.get_and_add_i32(0, 10), 0);
        assert_eq!(buf.get_i32(0), 10);
        assert_eq!(buf.get_and_add_i32(0, -5), 10);
        assert_eq!(buf.get_i32(0), 5);

        assert_eq!(buf.get_and_add_i64(8, 100), 0);
        assert_eq!(buf.get_i64(8), 100);
    }

    #[test]
    fn test_bounds_checks() {
        let mut backing = [0u8; 8];
        let buf = AtomicBuffer::new(&mut backing);

        assert!(buf.try_get_i64(0).is_ok());
        assert!(buf.try_get_i64(1).is_err());
        assert!(buf.try_put_i32(5, 42).is_err());
    }

    #[test]
    fn test_alignment_checks() {
        let mut backing = [0u8; 16];
        let buf = AtomicBuffer::new(&mut backing);

        // Let's find an unaligned offset.
        let base_addr = unsafe { buf.as_slice().as_ptr() as usize };
        let mut unaligned_offset = 1;
        if (base_addr + unaligned_offset) % 4 == 0 {
            unaligned_offset = 2;
        }

        assert!(buf.try_get_i32_volatile(unaligned_offset).is_err());
        assert!(buf.try_put_i32_volatile(unaligned_offset, 42).is_err());
        assert!(buf.try_put_i32_ordered(unaligned_offset, 42).is_err());
        assert!(
            buf.try_compare_and_set_i32(unaligned_offset, 0, 42)
                .is_err()
        );
        assert!(buf.try_get_and_add_i32(unaligned_offset, 5).is_err());
    }

    #[test]
    fn test_string_ops() {
        let mut backing = [0u8; 32];
        let buf = AtomicBuffer::new(&mut backing);

        let test_str = "Aeron Rust!";
        let written = buf.put_string_utf8(0, test_str);
        assert_eq!(written, 4 + test_str.len());

        let read_str = buf.get_string_utf8(0);
        assert_eq!(read_str, test_str);
    }
}
