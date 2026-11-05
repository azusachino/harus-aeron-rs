use super::frame_descriptor::HEADER_LENGTH;
use agrona::AtomicBuffer;

/// Number of partitions the log is divided into.
pub const PARTITION_COUNT: i32 = 3;

/// Offset within the metadata where the tail values are stored.
pub const TERM_TAIL_COUNTERS_OFFSET: usize = 0;

/// Offset within the log metadata where the active partition index is stored.
pub const LOG_ACTIVE_TERM_COUNT_OFFSET: usize =
    TERM_TAIL_COUNTERS_OFFSET + (std::mem::size_of::<i64>() * PARTITION_COUNT as usize);

/// Minimum page size (4 KB). Mirrors upstream `LogBufferDescriptor.PAGE_MIN_SIZE`.
pub const LOG_META_DATA_LENGTH: usize = 4096;

pub const TERM_MIN_LENGTH: i32 = 64 * 1024;
pub const TERM_MAX_LENGTH: i32 = 1024 * 1024 * 1024;
pub const PAGE_MIN_SIZE: i32 = 4 * 1024;
pub const PAGE_MAX_SIZE: i32 = 1024 * 1024 * 1024;

// Log metadata field offsets follow vendor `io.aeron.logbuffer.LogBufferDescriptor`, whose
// source is `vendor/aeron/aeron-client/src/main/java/io/aeron/logbuffer/LogBufferDescriptor.java`
// (`PADDING_SIZE = 64`). Do not infer these from the Rust cache-line padding used elsewhere.
pub const LOG_MTU_LENGTH_OFFSET: usize = 272;
pub const LOG_TERM_LENGTH_OFFSET: usize = 276;
pub const LOG_PAGE_SIZE_OFFSET: usize = 280;

/// The MTU length used for the publication associated with this log, in bytes.
pub fn mtu_length(metadata_buffer: &AtomicBuffer) -> i32 {
    metadata_buffer.get_i32(LOG_MTU_LENGTH_OFFSET)
}

/// Set the MTU length used for the publication associated with this log, in bytes.
pub fn set_mtu_length(metadata_buffer: &AtomicBuffer, mtu_length: i32) {
    metadata_buffer.put_i32(LOG_MTU_LENGTH_OFFSET, mtu_length);
}

pub fn term_length(metadata_buffer: &AtomicBuffer) -> i32 {
    metadata_buffer.get_i32(LOG_TERM_LENGTH_OFFSET)
}

pub fn set_term_length(metadata_buffer: &AtomicBuffer, length: i32) {
    metadata_buffer.put_i32(LOG_TERM_LENGTH_OFFSET, length);
}

pub fn page_size(metadata_buffer: &AtomicBuffer) -> i32 {
    metadata_buffer.get_i32(LOG_PAGE_SIZE_OFFSET)
}

pub fn set_page_size(metadata_buffer: &AtomicBuffer, size: i32) {
    metadata_buffer.put_i32(LOG_PAGE_SIZE_OFFSET, size);
}

pub fn check_term_length(term_length: i32) -> std::result::Result<(), String> {
    if term_length < TERM_MIN_LENGTH {
        return Err(format!(
            "Term length less than min length of {}: length={}",
            TERM_MIN_LENGTH, term_length
        ));
    }
    if term_length > TERM_MAX_LENGTH {
        return Err(format!(
            "Term length more than max length of {}: length={}",
            TERM_MAX_LENGTH, term_length
        ));
    }
    if term_length <= 0 || (term_length & (term_length - 1)) != 0 {
        return Err(format!(
            "Term length not a power of 2: length={}",
            term_length
        ));
    }
    Ok(())
}

pub fn check_page_size(page_size: i32) -> std::result::Result<(), String> {
    if page_size < PAGE_MIN_SIZE {
        return Err(format!(
            "Page size less than min size of {}: page size={}",
            PAGE_MIN_SIZE, page_size
        ));
    }
    if page_size > PAGE_MAX_SIZE {
        return Err(format!(
            "Page size more than max size of {}: page size={}",
            PAGE_MAX_SIZE, page_size
        ));
    }
    if page_size <= 0 || (page_size & (page_size - 1)) != 0 {
        return Err(format!(
            "Page size not a power of 2: page size={}",
            page_size
        ));
    }
    Ok(())
}

/// Compute position in absolute number of bytes.
#[inline]
pub fn compute_position(
    active_term_id: i32,
    term_offset: i32,
    position_bits_to_shift: i32,
    initial_term_id: i32,
) -> i64 {
    // Subtract in 32-bit space (matching Java's `int - int`) before widening: this copes with
    // negative `active_term_id` on i32 rollover, which upstream relies on intentionally.
    let term_count = active_term_id.wrapping_sub(initial_term_id) as i64;
    (term_count << position_bits_to_shift) + (term_offset as i64)
}

pub const LOG_IS_CONNECTED_OFFSET: usize = 136;

/// Offset of the shared end-of-stream position marker.
///
/// Matches vendored `LogBufferDescriptor.LOG_END_OF_STREAM_POSITION_OFFSET`.
pub const LOG_END_OF_STREAM_POSITION_OFFSET: usize = 128;

/// Position used until an EOS frame has been observed and rebuilt.
pub const NO_END_OF_STREAM_POSITION: i64 = i64::MAX;

pub fn end_of_stream_position(metadata_buffer: &AtomicBuffer) -> i64 {
    metadata_buffer.get_i64_volatile(LOG_END_OF_STREAM_POSITION_OFFSET)
}

pub fn set_end_of_stream_position(metadata_buffer: &AtomicBuffer, position: i64) {
    metadata_buffer.put_i64_ordered(LOG_END_OF_STREAM_POSITION_OFFSET, position);
}

pub fn is_connected(metadata_buffer: &AtomicBuffer) -> bool {
    metadata_buffer.get_i32_volatile(LOG_IS_CONNECTED_OFFSET) == 1
}

pub fn set_is_connected(metadata_buffer: &AtomicBuffer, is_connected: bool) {
    metadata_buffer.put_i32_volatile(LOG_IS_CONNECTED_OFFSET, if is_connected { 1 } else { 0 });
}

/// Get active term count from metadata buffer with volatile semantics.
pub fn active_term_count(metadata_buffer: &AtomicBuffer) -> i32 {
    metadata_buffer.get_i32_volatile(LOG_ACTIVE_TERM_COUNT_OFFSET)
}

/// Set active term count in metadata buffer with plain put semantics.
pub fn set_active_term_count(metadata_buffer: &AtomicBuffer, term_count: i32) {
    metadata_buffer.put_i32(LOG_ACTIVE_TERM_COUNT_OFFSET, term_count);
}

/// Compare and set active term count in metadata buffer.
pub fn cas_active_term_count(
    metadata_buffer: &AtomicBuffer,
    expected_term_count: i32,
    update_term_count: i32,
) -> bool {
    metadata_buffer.compare_and_set_i32(
        LOG_ACTIVE_TERM_COUNT_OFFSET,
        expected_term_count,
        update_term_count,
    )
}

/// Determine partition index based on term count.
#[inline]
pub fn index_by_term_count(term_count: i64) -> i32 {
    (term_count % PARTITION_COUNT as i64) as i32
}

/// Read term ID from a packed raw tail value.
#[inline]
pub fn term_id(raw_tail: i64) -> i32 {
    (raw_tail >> 32) as i32
}

/// Pack a term ID and term offset into a raw tail value.
#[inline]
pub fn pack_tail(term_id: i32, term_offset: i32) -> i64 {
    ((term_id as i64) << 32) | (term_offset as i64 & 0xFFFF_FFFF)
}

/// Get raw tail for partition with plain get semantics.
pub fn raw_tail(metadata_buffer: &AtomicBuffer, partition_index: usize) -> i64 {
    metadata_buffer
        .get_i64(TERM_TAIL_COUNTERS_OFFSET + (partition_index * std::mem::size_of::<i64>()))
}

/// Set raw tail for partition with plain put semantics.
pub fn set_raw_tail(metadata_buffer: &AtomicBuffer, partition_index: usize, raw_tail: i64) {
    metadata_buffer.put_i64(
        TERM_TAIL_COUNTERS_OFFSET + (partition_index * std::mem::size_of::<i64>()),
        raw_tail,
    );
}

/// Get raw tail for partition with volatile get semantics.
pub fn raw_tail_volatile(metadata_buffer: &AtomicBuffer, partition_index: usize) -> i64 {
    metadata_buffer.get_i64_volatile(
        TERM_TAIL_COUNTERS_OFFSET + (partition_index * std::mem::size_of::<i64>()),
    )
}

/// Set raw tail for partition with volatile put semantics.
pub fn set_raw_tail_volatile(
    metadata_buffer: &AtomicBuffer,
    partition_index: usize,
    raw_tail: i64,
) {
    metadata_buffer.put_i64_volatile(
        TERM_TAIL_COUNTERS_OFFSET + (partition_index * std::mem::size_of::<i64>()),
        raw_tail,
    );
}

/// Compare and set raw tail for partition.
pub fn cas_raw_tail(
    metadata_buffer: &AtomicBuffer,
    partition_index: usize,
    expected_raw_tail: i64,
    update_raw_tail: i64,
) -> bool {
    metadata_buffer.compare_and_set_i64(
        TERM_TAIL_COUNTERS_OFFSET + (partition_index * std::mem::size_of::<i64>()),
        expected_raw_tail,
        update_raw_tail,
    )
}

/// Rotate log and update tail counter for the new term.
pub fn rotate_log(metadata_buffer: &AtomicBuffer, term_count: i32, term_id: i32) -> bool {
    let next_term_id = term_id + 1;
    let next_term_count = term_count + 1;
    let next_index = index_by_term_count(next_term_count as i64) as usize;
    let expected_term_id = next_term_id - PARTITION_COUNT;

    loop {
        let raw_tail_val = raw_tail_volatile(metadata_buffer, next_index);
        if expected_term_id != self::term_id(raw_tail_val) {
            break;
        }
        if cas_raw_tail(
            metadata_buffer,
            next_index,
            raw_tail_val,
            pack_tail(next_term_id, 0),
        ) {
            break;
        }
    }

    cas_active_term_count(metadata_buffer, term_count, next_term_count)
}

/// Offset at which default frame headers begin in the metadata buffer.
pub const LOG_DEFAULT_FRAME_HEADER_OFFSET: usize = 320;

/// Offset of the default-frame-header length field in the log metadata.
pub const LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET: usize = 268;

/// Store the default frame-header template into the log metadata and record its length.
///
/// The publication's `HeaderWriter` copies this template into every frame it appends, so the
/// driver seeds it with the DATA type plus session/stream ids when it creates a log.
pub fn store_default_frame_header(log_metadata_buffer: &AtomicBuffer, header: &AtomicBuffer) {
    log_metadata_buffer.put_i32(LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET, HEADER_LENGTH);
    log_metadata_buffer.put_bytes_from(
        LOG_DEFAULT_FRAME_HEADER_OFFSET,
        header,
        0,
        HEADER_LENGTH as usize,
    );
}

/// Store/apply the default header template to a term buffer at `term_offset`.
pub fn apply_default_header(
    log_metadata_buffer: &AtomicBuffer,
    term_buffer: &AtomicBuffer,
    term_offset: usize,
) {
    term_buffer.put_bytes_from(
        term_offset,
        log_metadata_buffer,
        LOG_DEFAULT_FRAME_HEADER_OFFSET,
        HEADER_LENGTH as usize,
    );
}

/// Mirrors upstream `LogBufferDescriptor.LOG_INITIAL_TERM_ID_OFFSET`
/// (`PADDING_SIZE * 4 + SIZE_OF_LONG` with `PADDING_SIZE = 64`, i.e. `256 + 8 = 264`).
pub const LOG_INITIAL_TERM_ID_OFFSET: usize = 264;

/// Calculate the number of position bits to shift for a given term length.
///
/// Mirrors upstream `LogBufferDescriptor.positionBitsToShift`, which only accepts powers of
/// two from 64 KB to 1 GB inclusive and throws `IllegalArgumentException` otherwise.
///
/// # Panics
/// Panics if `term_length` is not a power of two in `[64 KiB, 1 GiB]`.
#[inline]
pub fn position_bits_to_shift(term_length: usize) -> usize {
    let shift = term_length.trailing_zeros() as usize;
    if !(16..=30).contains(&shift) || (1usize << shift) != term_length {
        panic!("invalid term buffer length: {term_length}");
    }
    shift
}

/// Get initial term ID from log metadata buffer.
pub fn initial_term_id(log_metadata_buffer: &AtomicBuffer) -> i32 {
    log_metadata_buffer.get_i32(LOG_INITIAL_TERM_ID_OFFSET)
}

/// Set initial term ID in log metadata buffer.
pub fn set_initial_term_id(log_metadata_buffer: &AtomicBuffer, initial_term_id: i32) {
    log_metadata_buffer.put_i32(LOG_INITIAL_TERM_ID_OFFSET, initial_term_id);
}

/// Initialise tail counter with a term ID for a given partition index.
pub fn initialise_tail_with_term_id(
    log_metadata_buffer: &AtomicBuffer,
    partition_index: usize,
    term_id: i32,
) {
    set_raw_tail(log_metadata_buffer, partition_index, pack_tail(term_id, 0));
}

/// Calculate partition index by stream position.
#[inline]
pub fn index_by_position(position: i64, position_bits_to_shift: usize) -> usize {
    ((position >> position_bits_to_shift) as usize) % PARTITION_COUNT as usize
}

/// Extract term offset from raw tail, capped at term_length.
#[inline]
pub fn term_offset(raw_tail: i64, term_length: usize) -> usize {
    let tail = (raw_tail as u64 & 0xFFFF_FFFF) as usize;
    tail.min(term_length)
}

/// Get raw tail value for active partition from metadata buffer.
pub fn raw_tail_volatile_active(log_metadata_buffer: &AtomicBuffer) -> i64 {
    let active_index = index_by_term_count(active_term_count(log_metadata_buffer) as i64) as usize;
    raw_tail_volatile(log_metadata_buffer, active_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agrona::AtomicBuffer;

    #[test]
    fn rotate_log_should_cas_active_term_count_even_when_term_id_does_not_match() {
        let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let metadata_buffer = AtomicBuffer::new(&mut bytes);

        let term_id = 5;
        let term_count = 1;
        set_raw_tail(&metadata_buffer, 2, pack_tail(term_id, 1024));
        set_raw_tail(&metadata_buffer, 0, pack_tail(term_id + 1, 2048));
        set_raw_tail(&metadata_buffer, 1, pack_tail(term_id + 2, 4096));
        set_active_term_count(&metadata_buffer, term_count);

        assert!(rotate_log(&metadata_buffer, term_count, term_id));

        assert_eq!(raw_tail(&metadata_buffer, 2), pack_tail(term_id, 1024));
        assert_eq!(raw_tail(&metadata_buffer, 0), pack_tail(term_id + 1, 2048));
        assert_eq!(raw_tail(&metadata_buffer, 1), pack_tail(term_id + 2, 4096));
        assert_eq!(active_term_count(&metadata_buffer), term_count + 1);
    }

    #[test]
    fn rotate_log_should_cas_active_term_count_after_setting_tail_for_the_next_term() {
        let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let metadata_buffer = AtomicBuffer::new(&mut bytes);

        let term_id = 51;
        let term_count = 19;
        set_raw_tail(&metadata_buffer, 1, pack_tail(term_id, 1024));
        set_raw_tail(
            &metadata_buffer,
            2,
            pack_tail(term_id + 1 - PARTITION_COUNT, 2048),
        );
        set_raw_tail(
            &metadata_buffer,
            0,
            pack_tail(term_id + 2 - PARTITION_COUNT, 4096),
        );
        set_active_term_count(&metadata_buffer, term_count);

        assert!(rotate_log(&metadata_buffer, term_count, term_id));

        assert_eq!(raw_tail(&metadata_buffer, 1), pack_tail(term_id, 1024));
        assert_eq!(raw_tail(&metadata_buffer, 2), pack_tail(term_id + 1, 0));
        assert_eq!(
            raw_tail(&metadata_buffer, 0),
            pack_tail(term_id + 2 - PARTITION_COUNT, 4096)
        );
        assert_eq!(active_term_count(&metadata_buffer), term_count + 1);
    }

    #[test]
    fn rotate_log_is_a_no_op_if_neither_tail_nor_active_term_count_can_be_changed() {
        let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let metadata_buffer = AtomicBuffer::new(&mut bytes);

        let term_id = 23;
        let term_count = 42;
        set_raw_tail(&metadata_buffer, 0, pack_tail(term_id, 1024));
        set_raw_tail(&metadata_buffer, 1, pack_tail(term_id + 18, 2048));
        set_raw_tail(&metadata_buffer, 2, pack_tail(term_id - 19, 4096));
        set_active_term_count(&metadata_buffer, term_count);

        assert!(!rotate_log(&metadata_buffer, 3, term_id));

        assert_eq!(raw_tail(&metadata_buffer, 0), pack_tail(term_id, 1024));
        assert_eq!(raw_tail(&metadata_buffer, 1), pack_tail(term_id + 18, 2048));
        assert_eq!(raw_tail(&metadata_buffer, 2), pack_tail(term_id - 19, 4096));
        assert_eq!(active_term_count(&metadata_buffer), term_count);
    }

    #[test]
    fn position_bits_to_shift_accepts_valid_power_of_two_term_lengths() {
        assert_eq!(position_bits_to_shift(64 * 1024), 16);
        assert_eq!(position_bits_to_shift(1024 * 1024), 20);
        assert_eq!(position_bits_to_shift(1024 * 1024 * 1024), 30);
    }

    #[test]
    #[should_panic(expected = "invalid term buffer length")]
    fn position_bits_to_shift_rejects_non_power_of_two() {
        position_bits_to_shift(100);
    }

    #[test]
    #[should_panic(expected = "invalid term buffer length")]
    fn position_bits_to_shift_rejects_out_of_range_power_of_two() {
        position_bits_to_shift(32 * 1024);
    }
}
