//! Three-term log buffer, mmap-backed layout descriptors and utilities.
//!
//! Mirrors upstream `io.aeron.logbuffer.LogBufferDescriptor`, `Header`, and `HeaderWriter`.

use crate::protocol;
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

/// Frame header length (32 bytes).
///
/// Mirrors `DataHeaderFlyweight::HEADER_LENGTH`.
pub const HEADER_LENGTH: i32 = 32;

/// Alignment of frames in the log buffer (32 bytes).
///
/// Mirrors `FrameDescriptor::FRAME_ALIGNMENT`.
pub const FRAME_ALIGNMENT: i32 = 32;

/// Align a value to the specified alignment (must be a power of two).
#[inline]
pub fn align(value: i32, alignment: i32) -> i32 {
    (value + (alignment - 1)) & !(alignment - 1)
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

/// Compute frame length for a message fragmented into chunks of `max_payload_size`.
pub fn compute_fragmented_frame_length(length: i32, max_payload_size: i32) -> i32 {
    let num_max_payloads = length / max_payload_size;
    let remaining_payload = length % max_payload_size;
    let last_frame_length = if remaining_payload > 0 {
        align(remaining_payload + HEADER_LENGTH, FRAME_ALIGNMENT)
    } else {
        0
    };

    (num_max_payloads * (max_payload_size + HEADER_LENGTH)) + last_frame_length
}

/// Represents the header of a data frame for accessing metadata fields.
#[derive(Clone)]
pub struct Header<'a> {
    initial_term_id: i32,
    position_bits_to_shift: i32,
    offset: usize,
    buffer: Option<AtomicBuffer<'a>>,
    fragmented_frame_length: Option<i32>,
    context: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}

impl<'a> Header<'a> {
    /// Construct a header that references a buffer for the log.
    pub fn new(initial_term_id: i32, position_bits_to_shift: i32) -> Self {
        Self {
            initial_term_id,
            position_bits_to_shift,
            offset: 0,
            buffer: None,
            fragmented_frame_length: None,
            context: None,
        }
    }

    /// Construct a header with initial term ID, position bits to shift, and context.
    pub fn with_context(
        initial_term_id: i32,
        position_bits_to_shift: i32,
        context: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self {
            initial_term_id,
            position_bits_to_shift,
            offset: 0,
            buffer: None,
            fragmented_frame_length: None,
            context: Some(context),
        }
    }

    /// Context stored for callback state.
    pub fn context(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.context.as_deref()
    }

    /// Set context.
    pub fn set_context(
        &mut self,
        context: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    ) -> &mut Self {
        self.context = context;
        self
    }

    /// Calculate absolute stream position after reading this frame.
    pub fn position(&self) -> i64 {
        compute_position(
            self.term_id(),
            self.next_term_offset(),
            self.position_bits_to_shift,
            self.initial_term_id,
        )
    }

    pub fn position_bits_to_shift(&self) -> i32 {
        self.position_bits_to_shift
    }

    pub fn set_position_bits_to_shift(&mut self, position_bits_to_shift: i32) -> &mut Self {
        self.position_bits_to_shift = position_bits_to_shift;
        self
    }

    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    pub fn set_initial_term_id(&mut self, initial_term_id: i32) -> &mut Self {
        self.initial_term_id = initial_term_id;
        self
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn set_offset(&mut self, offset: usize) -> &mut Self {
        self.offset = offset;
        self
    }

    pub fn buffer(&self) -> Option<&AtomicBuffer<'a>> {
        self.buffer.as_ref()
    }

    pub fn set_buffer(&mut self, buffer: AtomicBuffer<'a>) -> &mut Self {
        self.buffer = Some(buffer);
        self
    }

    fn buf(&self) -> &AtomicBuffer<'a> {
        self.buffer.as_ref().expect("buffer not set on Header")
    }

    pub fn frame_length(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::FRAME_LENGTH_FIELD_OFFSET)
    }

    pub fn version(&self) -> u8 {
        self.buf()
            .get_u8(self.offset + protocol::VERSION_FIELD_OFFSET)
    }

    pub fn flags(&self) -> u8 {
        self.buf()
            .get_u8(self.offset + protocol::FLAGS_FIELD_OFFSET)
    }

    pub fn type_(&self) -> u16 {
        self.buf()
            .get_i16(self.offset + protocol::TYPE_FIELD_OFFSET) as u16
    }

    pub fn term_offset(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET)
    }

    pub fn session_id(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_SESSION_ID_FIELD_OFFSET)
    }

    pub fn stream_id(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_STREAM_ID_FIELD_OFFSET)
    }

    pub fn term_id(&self) -> i32 {
        self.buf()
            .get_i32(self.offset + protocol::DATA_TERM_ID_FIELD_OFFSET)
    }

    pub fn reserved_value(&self) -> i64 {
        self.buf()
            .get_i64(self.offset + protocol::DATA_RESERVED_VALUE_OFFSET)
    }

    pub fn fragmented_frame_length(&self) -> Option<i32> {
        self.fragmented_frame_length
    }

    pub fn set_fragmented_frame_length(&mut self, length: Option<i32>) -> &mut Self {
        self.fragmented_frame_length = length;
        self
    }

    fn term_occupancy_length(&self) -> i32 {
        self.fragmented_frame_length
            .unwrap_or_else(|| self.frame_length())
    }

    pub fn next_term_offset(&self) -> i32 {
        align(
            self.term_offset() + self.term_occupancy_length(),
            FRAME_ALIGNMENT,
        )
    }
}

/// Utility for applying a header to a message in a term buffer.
///
/// Precomputes packed words for efficiency when appending frames.
/// Note: This is the Little Endian / LE native implementation. Upstream Aeron
/// also provides `NativeBigEndianHeaderWriter` for BE architectures, which is dropped here.
#[derive(Debug, Copy, Clone)]
pub struct HeaderWriter {
    version_flags_type: i64,
    session_id: i64,
    stream_id: i64,
}

impl HeaderWriter {
    /// Construct a new [`HeaderWriter`] initialized from a default header buffer.
    pub fn new(default_header: &AtomicBuffer) -> Self {
        let version_flags_type =
            (default_header.get_i32(protocol::VERSION_FIELD_OFFSET) as i64) << 32;
        let session_id =
            (default_header.get_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET) as i64) << 32;
        let stream_id =
            (default_header.get_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET) as i64) & 0xFFFF_FFFF;

        Self {
            version_flags_type,
            session_id,
            stream_id,
        }
    }

    /// Write a header to the term buffer in Little Endian format.
    pub fn write(&self, term_buffer: &AtomicBuffer, offset: usize, length: i32, term_id: i32) {
        let frame_len_word = self.version_flags_type | (((-length) as i64) & 0xFFFF_FFFF);
        term_buffer.put_i64_ordered(offset + protocol::FRAME_LENGTH_FIELD_OFFSET, frame_len_word);
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);

        let term_offset_word = self.session_id | (offset as i64 & 0xFFFF_FFFF);
        term_buffer.put_i64(
            offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET,
            term_offset_word,
        );

        let stream_id_word = ((term_id as i64) << 32) | self.stream_id;
        term_buffer.put_i64(
            offset + protocol::DATA_STREAM_ID_FIELD_OFFSET,
            stream_id_word,
        );
    }
}

/// Read frame length at `term_offset` using volatile load.
pub fn frame_length_volatile(term_buffer: &AtomicBuffer, term_offset: usize) -> i32 {
    term_buffer.get_i32_volatile(term_offset)
}

/// Check if frame at `term_offset` is a padding frame.
pub fn is_padding_frame(term_buffer: &AtomicBuffer, term_offset: usize) -> bool {
    let frame_type = term_buffer.get_i16(term_offset + protocol::TYPE_FIELD_OFFSET) as u16;
    frame_type == protocol::HDR_TYPE_PAD
}

/// Scans a term buffer for availability of message fragments.
#[derive(Debug, Copy, Clone)]
pub struct TermScanner;

impl TermScanner {
    /// Pack padding and available bytes into an i64.
    #[inline]
    pub fn pack(padding: i32, available: i32) -> i64 {
        ((padding as i64) << 32) | (available as i64)
    }

    /// Extract available bytes count from packed status.
    #[inline]
    pub fn available(result: i64) -> i32 {
        result as i32
    }

    /// Extract padding bytes count from packed status.
    #[inline]
    pub fn padding(result: i64) -> i32 {
        ((result as u64) >> 32) as i32
    }

    /// Scan the term buffer for availability of new message fragments from `offset` up to `max_length` bytes.
    pub fn scan_for_availability(
        term_buffer: &AtomicBuffer,
        offset: usize,
        max_length: usize,
    ) -> i64 {
        let capacity = term_buffer.capacity();
        let limit = max_length.min(capacity.saturating_sub(offset));
        let mut available: i32 = 0;
        let mut padding: i32 = 0;

        loop {
            let term_offset = offset + available as usize;
            let frame_length = frame_length_volatile(term_buffer, term_offset);
            if frame_length <= 0 {
                break;
            }

            let mut aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
            if is_padding_frame(term_buffer, term_offset) {
                padding = aligned_frame_length - HEADER_LENGTH;
                aligned_frame_length = HEADER_LENGTH;
            }

            available += aligned_frame_length;

            if available as usize > limit {
                available = if aligned_frame_length == available {
                    -available
                } else {
                    available - aligned_frame_length
                };
                padding = 0;
                break;
            }

            if padding != 0 || (available as usize) >= limit {
                break;
            }
        }

        Self::pack(padding, available)
    }
}

/// Scans for gaps in the sequence of bytes in a term buffer.
#[derive(Debug, Copy, Clone)]
pub struct TermGapScanner;

impl TermGapScanner {
    /// Scan for gaps from `term_offset` up to `limit_offset`.
    /// Calls `handler(term_id, gap_begin_offset, gap_length)` for any gap found.
    /// Returns the offset of the last contiguous frame.
    pub fn scan_for_gap<F>(
        term_buffer: &AtomicBuffer,
        term_id: i32,
        term_offset: usize,
        limit_offset: usize,
        mut handler: F,
    ) -> usize
    where
        F: FnMut(i32, usize, usize),
    {
        let mut offset = term_offset;
        while offset < limit_offset {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            offset += align(frame_length, FRAME_ALIGNMENT) as usize;
        }

        let gap_begin_offset = offset;
        if offset < limit_offset {
            offset += HEADER_LENGTH as usize;
            while offset < limit_offset {
                if frame_length_volatile(term_buffer, offset) != 0 {
                    break;
                }
                offset += HEADER_LENGTH as usize;
            }

            let gap_length = offset - gap_begin_offset;
            handler(term_id, gap_begin_offset, gap_length);
        }

        gap_begin_offset
    }
}

/// Utility functions for reading a term within a log buffer.
#[derive(Debug, Copy, Clone)]
pub struct TermReader;

impl TermReader {
    /// Pack offset and fragments_read into an i64.
    #[inline]
    pub fn pack(offset: i32, fragments_read: i32) -> i64 {
        ((offset as i64) << 32) | (fragments_read as i64 & 0xFFFF_FFFF)
    }

    /// The number of fragments read from a packed outcome.
    #[inline]
    pub fn fragments_read(read_outcome: i64) -> i32 {
        read_outcome as i32
    }

    /// The offset up to which the term progressed from a packed outcome.
    #[inline]
    pub fn offset(read_outcome: i64) -> i32 {
        ((read_outcome as u64) >> 32) as i32
    }

    /// Reads data from a term in a log buffer and updates `subscriber_position`.
    #[allow(clippy::too_many_arguments)]
    pub fn read_with_position<'a, F, E>(
        term_buffer: &AtomicBuffer<'a>,
        term_offset: usize,
        mut handler: F,
        fragments_limit: usize,
        header: &mut Header<'a>,
        mut _error_handler: E,
        current_position: i64,
        subscriber_position: &agrona::counters::AtomicCounter,
    ) -> usize
    where
        F: FnMut(&AtomicBuffer<'a>, usize, i32, &Header<'a>),
        E: FnMut(Box<dyn std::error::Error>),
    {
        let mut fragments_read = 0;
        let mut offset = term_offset;
        let capacity = term_buffer.capacity();
        header.set_buffer(*term_buffer);

        while fragments_read < fragments_limit && offset < capacity {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let frame_offset = offset;
            offset += align(frame_length, FRAME_ALIGNMENT) as usize;

            if !is_padding_frame(term_buffer, frame_offset) {
                fragments_read += 1;
                header.set_offset(frame_offset);
                handler(
                    term_buffer,
                    frame_offset + HEADER_LENGTH as usize,
                    frame_length - HEADER_LENGTH,
                    header,
                );
            }
        }

        let new_position = current_position + (offset - term_offset) as i64;
        if new_position > current_position {
            subscriber_position.set_ordered(new_position);
        }

        fragments_read
    }

    /// Reads data from a term in a log buffer and returns packed `(offset, fragments_read)`.
    pub fn read<'a, F, E>(
        term_buffer: &AtomicBuffer<'a>,
        term_offset: usize,
        mut handler: F,
        fragments_limit: usize,
        header: &mut Header<'a>,
        mut _error_handler: E,
    ) -> i64
    where
        F: FnMut(&AtomicBuffer<'a>, usize, i32, &Header<'a>),
        E: FnMut(Box<dyn std::error::Error>),
    {
        let mut fragments_read = 0;
        let mut offset = term_offset;
        let capacity = term_buffer.capacity();
        header.set_buffer(*term_buffer);

        while fragments_read < fragments_limit && offset < capacity {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let frame_offset = offset;
            offset += align(frame_length, FRAME_ALIGNMENT) as usize;

            if !is_padding_frame(term_buffer, frame_offset) {
                fragments_read += 1;
                header.set_offset(frame_offset);
                handler(
                    term_buffer,
                    frame_offset + HEADER_LENGTH as usize,
                    frame_length - HEADER_LENGTH,
                    header,
                );
            }
        }

        Self::pack(offset as i32, fragments_read as i32)
    }
}

/// Offset at which default frame headers begin in the metadata buffer.
pub const LOG_DEFAULT_FRAME_HEADER_OFFSET: usize = 320;

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

/// Set frame type at `term_offset`.
pub fn set_frame_type(term_buffer: &AtomicBuffer, term_offset: usize, frame_type: u16) {
    term_buffer.put_i16(term_offset + protocol::TYPE_FIELD_OFFSET, frame_type as i16);
}

/// Set term offset field at `term_offset`.
pub fn set_frame_term_offset(term_buffer: &AtomicBuffer, term_offset: usize) {
    term_buffer.put_i32(
        term_offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET,
        term_offset as i32,
    );
}

/// Set term ID field at `term_offset`.
pub fn set_frame_term_id(term_buffer: &AtomicBuffer, term_offset: usize, term_id: i32) {
    term_buffer.put_i32(term_offset + protocol::DATA_TERM_ID_FIELD_OFFSET, term_id);
}

/// Set frame length at `term_offset` using ordered/release store.
pub fn set_frame_length_ordered(term_buffer: &AtomicBuffer, term_offset: usize, frame_length: i32) {
    term_buffer.put_i32_ordered(
        term_offset + protocol::FRAME_LENGTH_FIELD_OFFSET,
        frame_length,
    );
}

/// Rebuild a term buffer from received frames which can be out-of-order.
#[derive(Debug, Copy, Clone)]
pub struct TermRebuilder;

impl TermRebuilder {
    /// Insert a packet of frames into the log at `term_offset`.
    ///
    /// If the packet has already been inserted, this is a no-op.
    pub fn insert(
        term_buffer: &AtomicBuffer,
        term_offset: usize,
        packet: &AtomicBuffer,
        length: usize,
    ) {
        if term_buffer.get_i32(term_offset) == 0 {
            if length > HEADER_LENGTH as usize {
                term_buffer.put_bytes_from(
                    term_offset + HEADER_LENGTH as usize,
                    packet,
                    HEADER_LENGTH as usize,
                    length - HEADER_LENGTH as usize,
                );
            }

            term_buffer.put_i64(term_offset + 24, packet.get_i64(24));
            term_buffer.put_i64(term_offset + 16, packet.get_i64(16));
            term_buffer.put_i64(term_offset + 8, packet.get_i64(8));

            term_buffer.put_i64_ordered(term_offset, packet.get_i64(0));
        }
    }
}

/// Fills a gap in a term with a padding record.
#[derive(Debug, Copy, Clone)]
pub struct TermGapFiller;

impl TermGapFiller {
    /// Try to gap fill the current term at `gap_offset` if the gap contains no data.
    pub fn try_fill_gap(
        log_metadata_buffer: &AtomicBuffer,
        term_buffer: &AtomicBuffer,
        term_id: i32,
        gap_offset: usize,
        gap_length: usize,
    ) -> bool {
        let mut offset = (gap_offset + gap_length) as isize - FRAME_ALIGNMENT as isize;
        let target = gap_offset as isize;

        while offset >= target {
            if term_buffer.get_i32(offset as usize) != 0 {
                return false;
            }
            offset -= FRAME_ALIGNMENT as isize;
        }

        apply_default_header(log_metadata_buffer, term_buffer, gap_offset);
        set_frame_type(term_buffer, gap_offset, protocol::HDR_TYPE_PAD);
        set_frame_term_offset(term_buffer, gap_offset);
        set_frame_term_id(term_buffer, gap_offset, term_id);
        set_frame_length_ordered(term_buffer, gap_offset, gap_length as i32);

        true
    }
}

/// Offset at which initial term ID is stored in metadata buffer.
///
/// Mirrors upstream `LogBufferDescriptor.LOG_INITIAL_TERM_ID_OFFSET` (`PADDING_SIZE * 4 +
/// SIZE_OF_LONG` = 264). Must NOT collide with `TERM_TAIL_COUNTERS_OFFSET`'s 24-byte span
/// (partition 2's raw tail sits at byte offset 16) or with `LOG_ACTIVE_TERM_COUNT_OFFSET` (24).
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

/// Scan a term buffer for a block of message fragments including padding.
#[derive(Debug, Copy, Clone)]
pub struct TermBlockScanner;

impl TermBlockScanner {
    /// Scan a term buffer for a block of message fragments from `term_offset` up to `limit_offset`.
    pub fn scan(term_buffer: &AtomicBuffer, term_offset: usize, limit_offset: usize) -> usize {
        let mut offset = term_offset;

        while offset < limit_offset {
            let frame_length = frame_length_volatile(term_buffer, offset);
            if frame_length <= 0 {
                break;
            }

            let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT) as usize;

            if is_padding_frame(term_buffer, offset) {
                if term_offset == offset {
                    offset += aligned_frame_length;
                }
                break;
            }

            if offset + aligned_frame_length > limit_offset {
                break;
            }

            offset += aligned_frame_length;
        }

        offset
    }
}

/// Status result of an `unblock` operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnblockStatus {
    NoAction,
    Unblocked,
    UnblockedToEnd,
}

/// Unblocks a term buffer if a publisher has died leaving the log with a partial log entry.
#[derive(Debug, Copy, Clone)]
pub struct TermUnblocker;

impl TermUnblocker {
    /// Attempt to unblock the current term at `blocked_offset`.
    pub fn unblock(
        log_metadata_buffer: &AtomicBuffer,
        term_buffer: &AtomicBuffer,
        blocked_offset: usize,
        tail_offset: usize,
        term_id: i32,
    ) -> UnblockStatus {
        let mut status = UnblockStatus::NoAction;
        let mut frame_length = frame_length_volatile(term_buffer, blocked_offset);

        if frame_length < 0 {
            Self::reset_header(
                log_metadata_buffer,
                term_buffer,
                blocked_offset,
                term_id,
                -frame_length,
            );
            status = UnblockStatus::Unblocked;
        } else if frame_length == 0 {
            let mut current_offset = blocked_offset + FRAME_ALIGNMENT as usize;

            while current_offset < tail_offset {
                frame_length = frame_length_volatile(term_buffer, current_offset);

                if frame_length != 0 {
                    if Self::scan_back_to_confirm_zeroed(
                        term_buffer,
                        current_offset,
                        blocked_offset,
                    ) {
                        let length = (current_offset - blocked_offset) as i32;
                        Self::reset_header(
                            log_metadata_buffer,
                            term_buffer,
                            blocked_offset,
                            term_id,
                            length,
                        );
                        status = UnblockStatus::Unblocked;
                    }
                    break;
                }

                current_offset += FRAME_ALIGNMENT as usize;
            }

            if current_offset == term_buffer.capacity()
                && frame_length_volatile(term_buffer, blocked_offset) == 0
            {
                let length = (current_offset - blocked_offset) as i32;
                Self::reset_header(
                    log_metadata_buffer,
                    term_buffer,
                    blocked_offset,
                    term_id,
                    length,
                );
                status = UnblockStatus::UnblockedToEnd;
            }
        }

        status
    }

    fn reset_header(
        log_metadata_buffer: &AtomicBuffer,
        term_buffer: &AtomicBuffer,
        term_offset: usize,
        term_id: i32,
        frame_length: i32,
    ) {
        apply_default_header(log_metadata_buffer, term_buffer, term_offset);
        set_frame_type(term_buffer, term_offset, protocol::HDR_TYPE_PAD);
        set_frame_term_offset(term_buffer, term_offset);
        set_frame_term_id(term_buffer, term_offset, term_id);
        set_frame_length_ordered(term_buffer, term_offset, frame_length);
    }

    fn scan_back_to_confirm_zeroed(buffer: &AtomicBuffer, from: usize, limit: usize) -> bool {
        let mut i = from as isize - FRAME_ALIGNMENT as isize;
        let target = limit as isize;
        while i >= target {
            if frame_length_volatile(buffer, i as usize) != 0 {
                return false;
            }
            i -= FRAME_ALIGNMENT as isize;
        }
        true
    }
}

/// Provides the functionality to unblock a log at a given position.
#[derive(Debug, Copy, Clone)]
pub struct LogBufferUnblocker;

impl LogBufferUnblocker {
    /// Attempt to unblock a log buffer at `blocked_position`.
    pub fn unblock(
        term_buffers: &[AtomicBuffer],
        log_metadata_buffer: &AtomicBuffer,
        blocked_position: i64,
        term_length: usize,
    ) -> bool {
        let shift = position_bits_to_shift(term_length);
        let blocked_term_count = (blocked_position >> shift) as i32;
        let blocked_offset = (blocked_position as usize) & (term_length - 1);
        let active_term_count_val = active_term_count(log_metadata_buffer);

        if active_term_count_val == (blocked_term_count - 1) && blocked_offset == 0 {
            let current_term_id = term_id(raw_tail_volatile(
                log_metadata_buffer,
                index_by_term_count(active_term_count_val as i64) as usize,
            ));
            rotate_log(log_metadata_buffer, active_term_count_val, current_term_id);
            return true;
        }

        let blocked_index = index_by_term_count(blocked_term_count as i64) as usize;
        let raw_tail_val = raw_tail_volatile(log_metadata_buffer, blocked_index);
        let term_id_val = term_id(raw_tail_val);
        let tail_offset_val = term_offset(raw_tail_val, term_length);
        let term_buffer = &term_buffers[blocked_index];

        match TermUnblocker::unblock(
            log_metadata_buffer,
            term_buffer,
            blocked_offset,
            tail_offset_val,
            term_id_val,
        ) {
            UnblockStatus::NoAction => false,
            UnblockStatus::UnblockedToEnd => {
                rotate_log(log_metadata_buffer, blocked_term_count, term_id_val);
                true
            }
            UnblockStatus::Unblocked => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::DataHeaderFlyweight;
    use std::sync::Arc;

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
    fn should_compute_fragmented_frame_length() {
        let test_cases = [
            (0, 1376, 0),
            (10, 1024, 64),
            (2048, 2048, 2080),
            (4096, 1024, 4224),
            (7997, 992, 8288),
        ];

        for (length, max_payload_size, expected_frame_length) in test_cases {
            assert_eq!(
                compute_fragmented_frame_length(length, max_payload_size),
                expected_frame_length,
                "failed for length={}, max_payload_size={}",
                length,
                max_payload_size
            );
        }
    }

    #[test]
    fn header_constructor_initialized_data() {
        let initial_term_id = 18;
        let context: Arc<dyn std::any::Any + Send + Sync> = Arc::new(999i64);
        let position_bits_to_shift = 2;
        let header = Header::with_context(initial_term_id, position_bits_to_shift, context.clone());

        assert_eq!(header.initial_term_id(), initial_term_id);
        assert_eq!(header.position_bits_to_shift(), position_bits_to_shift);
        assert!(header.context().is_some());
    }

    #[test]
    fn header_position_calculation_the_end_of_the_message_in_the_log() {
        let cases = [
            (0, 2, 100, 1024, 5, 1172i64),
            (42, 16, 13, 4096, 46, 266272i64),
            (1, 30, 1024, 1073741824, 111, 119185343488i64),
        ];

        for (
            initial_term_id,
            position_bits_to_shift,
            frame_length,
            term_offset,
            term_id,
            expected_position,
        ) in cases
        {
            let mut bytes = vec![0u8; 128];
            let buf = AtomicBuffer::new(&mut bytes);
            let flyweight = DataHeaderFlyweight::new(buf);
            flyweight.set_frame_length(frame_length);
            flyweight.set_term_id(term_id);
            flyweight.set_term_offset(term_offset);

            let mut header = Header::new(initial_term_id, position_bits_to_shift);
            header.set_buffer(buf);
            header.set_offset(0);

            assert_eq!(header.position(), expected_position);
            assert_eq!(header.fragmented_frame_length(), None);
        }
    }

    #[test]
    fn header_offset_is_relative_to_the_buffer_start() {
        let mut header = Header::new(42, 3);
        assert_eq!(header.offset(), 0);

        header.set_offset(142);
        assert_eq!(header.offset(), 142);
    }

    #[test]
    fn header_should_read_data_from_the_buffer() {
        let cases = [
            (
                103,
                0x3u8,
                0x1Au8,
                0x6u16,
                2080,
                -46234,
                333,
                5,
                909090909090909i64,
            ),
            (
                512,
                0x1u8,
                0xCu8,
                0x1u16,
                1073741824,
                42,
                -876,
                1543,
                -4632842384627834687i64,
            ),
        ];

        for (
            frame_length,
            version,
            flags,
            header_type,
            term_offset,
            session_id,
            stream_id,
            term_id,
            reserved_value,
        ) in cases
        {
            let mut bytes = vec![0u8; 100];
            let offset = 16;
            let flyweight_buf = AtomicBuffer::new(&mut bytes[offset..offset + 64]);
            let flyweight = DataHeaderFlyweight::new(flyweight_buf);

            flyweight
                .set_frame_length(frame_length)
                .set_version(version)
                .set_flags(flags)
                .set_header_type(header_type)
                .set_term_offset(term_offset)
                .set_session_id(session_id)
                .set_stream_id(stream_id)
                .set_term_id(term_id)
                .set_reserved_value(reserved_value);

            let mut header = Header::new(5, 22);
            let full_buf = AtomicBuffer::new(&mut bytes);
            header.set_buffer(full_buf);
            header.set_offset(offset);

            assert_eq!(header.frame_length(), frame_length);
            assert_eq!(header.flags(), flags);
            assert_eq!(header.type_(), header_type);
            assert_eq!(header.term_offset(), term_offset);
            assert_eq!(header.session_id(), session_id);
            assert_eq!(header.stream_id(), stream_id);
            assert_eq!(header.term_id(), term_id);
            assert_eq!(header.reserved_value(), reserved_value);
        }
    }

    #[test]
    fn header_should_override_initial_term_id() {
        let initial_term_id = -178;
        let new_initial_term_id = 871;
        let mut header = Header::new(initial_term_id, 3);
        assert_eq!(header.initial_term_id(), initial_term_id);

        header.set_initial_term_id(new_initial_term_id);
        assert_eq!(header.initial_term_id(), new_initial_term_id);
    }

    #[test]
    fn header_should_override_position_bits_to_shift() {
        let position_bits_to_shift = -6;
        let new_position_bits_to_shift = 20;
        let mut header = Header::new(42, position_bits_to_shift);
        assert_eq!(header.position_bits_to_shift(), position_bits_to_shift);

        header.set_position_bits_to_shift(new_position_bits_to_shift);
        assert_eq!(header.position_bits_to_shift(), new_position_bits_to_shift);
    }

    #[test]
    fn should_encode_header_using_little_endian_byte_order() {
        let cases = [
            (100, 8u8, 5u8, 9i16, 352, -777, -1000, -33),
            (-99, -2i8 as u8, 7u8, 1i16, 8, 42, 3, 89),
            (123, 0u8, 0u8, 0i16, 0, 0, 0, 0),
            (
                32,
                1u8,
                -128i8 as u8,
                4i16,
                96,
                2147483647,
                -2147483648,
                -2147483648,
            ),
        ];

        for (
            frame_length,
            version,
            flags,
            header_type,
            term_offset,
            session_id,
            stream_id,
            term_id,
        ) in cases
        {
            let mut default_bytes = [0xFFu8; 32];
            let default_buffer = AtomicBuffer::new(&mut default_bytes);

            default_buffer.put_u8(protocol::VERSION_FIELD_OFFSET, version);
            default_buffer.put_u8(protocol::FLAGS_FIELD_OFFSET, flags);
            default_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, header_type);
            default_buffer.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, session_id);
            default_buffer.put_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET, stream_id);

            let mut term_bytes = [0xFFu8; 1024];
            let term_buffer = AtomicBuffer::new(&mut term_bytes);

            let header_writer = HeaderWriter::new(&default_buffer);
            header_writer.write(&term_buffer, term_offset as usize, frame_length, term_id);

            let offset = term_offset as usize;
            assert_eq!(
                term_buffer.get_i32(offset + protocol::FRAME_LENGTH_FIELD_OFFSET),
                -frame_length
            );
            assert_eq!(
                term_buffer.get_u8(offset + protocol::VERSION_FIELD_OFFSET),
                version
            );
            assert_eq!(
                term_buffer.get_u8(offset + protocol::FLAGS_FIELD_OFFSET),
                flags
            );
            assert_eq!(
                term_buffer.get_i16(offset + protocol::TYPE_FIELD_OFFSET),
                header_type
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET),
                term_offset
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
                session_id
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_STREAM_ID_FIELD_OFFSET),
                stream_id
            );
            assert_eq!(
                term_buffer.get_i32(offset + protocol::DATA_TERM_ID_FIELD_OFFSET),
                term_id
            );
        }
    }

    #[test]
    fn should_pack_padding_and_offset_into_resulting_status() {
        let padding = 77;
        let available = 65000;

        let scan_outcome = TermScanner::pack(padding, available);

        assert_eq!(TermScanner::padding(scan_outcome), padding);
        assert_eq!(TermScanner::available(scan_outcome), available);
    }

    #[test]
    fn should_return_zero_on_empty_log() {
        let mut bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let term_buffer = AtomicBuffer::new(&mut bytes);

        let scan_outcome = TermScanner::scan_for_availability(&term_buffer, 0, 1024);
        assert_eq!(TermScanner::available(scan_outcome), 0);
        assert_eq!(TermScanner::padding(scan_outcome), 0);
    }

    #[test]
    fn should_scan_single_message() {
        let msg_length = 1;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
        let frame_offset = 0;

        let mut bytes = vec![0u8; 1024];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(frame_offset, frame_length);
        term_buffer.put_i16(
            frame_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, 1024);
        assert_eq!(TermScanner::available(scan_outcome), aligned_frame_length);
        assert_eq!(TermScanner::padding(scan_outcome), 0);
    }

    #[test]
    fn should_fail_to_scan_message_larger_than_max_length() {
        let msg_length = 1;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
        let max_length = (aligned_frame_length - 1) as usize;
        let frame_offset = 0;

        let mut bytes = vec![0u8; 1024];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(frame_offset, frame_length);
        term_buffer.put_i16(
            frame_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let scan_outcome =
            TermScanner::scan_for_availability(&term_buffer, frame_offset, max_length);
        assert_eq!(TermScanner::available(scan_outcome), -aligned_frame_length);
        assert_eq!(TermScanner::padding(scan_outcome), -1);
    }

    #[test]
    fn should_scan_two_messages_that_fit_in_single_mtu() {
        let msg_length = 100;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
        let frame_offset = 0;

        let mut bytes = vec![0u8; 1024];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(frame_offset, frame_length);
        term_buffer.put_i16(
            frame_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let second_offset = frame_offset + aligned_frame_length as usize;
        term_buffer.put_i32_volatile(second_offset, aligned_frame_length);
        term_buffer.put_i16(
            second_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, 1024);
        assert_eq!(
            TermScanner::available(scan_outcome),
            aligned_frame_length * 2
        );
        assert_eq!(TermScanner::padding(scan_outcome), 0);
    }

    #[test]
    fn should_scan_last_message_in_buffer_plus_padding() {
        let aligned_frame_length = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        let padding_frame_length = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        let term_capacity = 4096;
        let frame_offset = term_capacity - (aligned_frame_length + padding_frame_length) as usize;

        let mut bytes = vec![0u8; term_capacity];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(frame_offset, aligned_frame_length);
        term_buffer.put_i16(
            frame_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let padding_offset = frame_offset + aligned_frame_length as usize;
        term_buffer.put_i32_volatile(padding_offset, padding_frame_length);
        term_buffer.put_i16(
            padding_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_PAD as i16,
        );

        let scan_outcome = TermScanner::scan_for_availability(&term_buffer, frame_offset, 1024);
        assert_eq!(
            TermScanner::available(scan_outcome),
            aligned_frame_length + HEADER_LENGTH
        );
        assert_eq!(
            TermScanner::padding(scan_outcome),
            padding_frame_length - HEADER_LENGTH
        );
    }

    #[test]
    fn should_report_gap_at_beginning_of_buffer() {
        let frame_offset = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT) as usize;
        let high_water_mark = frame_offset + align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize;

        let mut bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(frame_offset, HEADER_LENGTH);

        let mut gaps = Vec::new();
        let result = TermGapScanner::scan_for_gap(
            &term_buffer,
            1,
            0,
            high_water_mark,
            |term_id, offset, length| {
                gaps.push((term_id, offset, length));
            },
        );

        assert_eq!(result, 0);
        assert_eq!(gaps, vec![(1, 0, frame_offset)]);
    }

    #[test]
    fn should_report_single_gap_when_buffer_not_full() {
        let tail = align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize;
        let high_water_mark = (FRAME_ALIGNMENT * 3) as usize;

        let mut bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(
            tail - align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize,
            HEADER_LENGTH,
        );
        term_buffer.put_i32_volatile(tail, 0);
        term_buffer.put_i32_volatile(
            high_water_mark - align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize,
            HEADER_LENGTH,
        );

        let mut gaps = Vec::new();
        let result = TermGapScanner::scan_for_gap(
            &term_buffer,
            1,
            tail,
            high_water_mark,
            |term_id, offset, length| {
                gaps.push((term_id, offset, length));
            },
        );

        assert_eq!(result, tail);
        assert_eq!(
            gaps,
            vec![(1, tail, align(HEADER_LENGTH, FRAME_ALIGNMENT) as usize)]
        );
    }

    #[test]
    fn should_report_no_gap_when_hwm_is_in_padding() {
        let padding_length = (align(HEADER_LENGTH, FRAME_ALIGNMENT) * 2) as usize;
        let capacity = 4096;
        let tail = capacity - padding_length;
        let high_water_mark = capacity - padding_length + HEADER_LENGTH as usize;

        let mut bytes = vec![0u8; capacity];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(tail, padding_length as i32);
        term_buffer.put_i32_volatile(tail + HEADER_LENGTH as usize, 0);

        let mut gaps = Vec::new();
        let result = TermGapScanner::scan_for_gap(
            &term_buffer,
            1,
            tail,
            high_water_mark,
            |term_id, offset, length| {
                gaps.push((term_id, offset, length));
            },
        );

        assert_eq!(result, capacity);
        assert!(gaps.is_empty());
    }

    #[test]
    fn should_report_single_header_gap() {
        let offset = 8192 + 384;
        let mut bytes = vec![0u8; 16384];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(offset, 0);
        term_buffer.put_i32_volatile(offset + HEADER_LENGTH as usize, 128);

        let capacity = term_buffer.capacity();
        let mut gaps = Vec::new();
        let result = TermGapScanner::scan_for_gap(
            &term_buffer,
            1,
            offset,
            capacity,
            |term_id, offset, length| {
                gaps.push((term_id, offset, length));
            },
        );

        assert_eq!(result, offset);
        assert_eq!(gaps, vec![(1, offset, HEADER_LENGTH as usize)]);
    }

    #[test]
    fn should_report_gap_at_the_end_of_the_buffer() {
        let capacity = 4096;
        let offset = capacity - 128;
        let mut bytes = vec![0u8; capacity];
        let term_buffer = AtomicBuffer::new(&mut bytes);
        term_buffer.put_i32_volatile(offset, 0);

        let mut gaps = Vec::new();
        let result = TermGapScanner::scan_for_gap(
            &term_buffer,
            1,
            offset,
            capacity,
            |term_id, offset, length| {
                gaps.push((term_id, offset, length));
            },
        );

        assert_eq!(result, offset);
        assert_eq!(gaps, vec![(1, offset, 128)]);
    }

    #[test]
    fn term_reader_should_pack_and_unpack() {
        let offset = 4096;
        let fragments_read = 5;
        let packed = TermReader::pack(offset, fragments_read);

        assert_eq!(TermReader::offset(packed), offset);
        assert_eq!(TermReader::fragments_read(packed), fragments_read);
    }

    #[test]
    fn should_read_first_message() {
        let msg_length = 1;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
        let term_offset = 0;

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32_volatile(0, frame_length);
        term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

        let mut counter_values = vec![0u8; 1024];
        let values_buffer = AtomicBuffer::new(&mut counter_values);
        let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

        let mut header = Header::new(7, 16);
        let mut fragment_calls = Vec::new();

        let read_outcome = TermReader::read_with_position(
            &term_buffer,
            term_offset,
            |_buf, offset, length, _hdr| {
                fragment_calls.push((offset, length));
            },
            usize::MAX,
            &mut header,
            |_| (),
            0,
            &subscriber_position,
        );

        assert_eq!(read_outcome, 1);
        assert_eq!(fragment_calls, vec![(HEADER_LENGTH as usize, msg_length)]);
        assert_eq!(subscriber_position.get(), aligned_frame_length as i64);
    }

    #[test]
    fn should_not_read_past_tail() {
        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        let mut counter_values = vec![0u8; 1024];
        let values_buffer = AtomicBuffer::new(&mut counter_values);
        let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

        let mut header = Header::new(7, 16);
        let mut fragment_calls = Vec::new();

        let read_outcome = TermReader::read_with_position(
            &term_buffer,
            0,
            |_buf, offset, length, _hdr| {
                fragment_calls.push((offset, length));
            },
            usize::MAX,
            &mut header,
            |_| (),
            0,
            &subscriber_position,
        );

        assert_eq!(read_outcome, 0);
        assert!(fragment_calls.is_empty());
        assert_eq!(subscriber_position.get(), 0);
    }

    #[test]
    fn should_read_one_limited_message() {
        let msg_length = 1;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32_volatile(0, frame_length);
        term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

        let second_offset = aligned_frame_length as usize;
        term_buffer.put_i32_volatile(second_offset, frame_length);
        term_buffer.put_i16(
            second_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let mut counter_values = vec![0u8; 1024];
        let values_buffer = AtomicBuffer::new(&mut counter_values);
        let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

        let mut header = Header::new(7, 16);
        let mut fragment_calls = Vec::new();

        let read_outcome = TermReader::read_with_position(
            &term_buffer,
            0,
            |_buf, offset, length, _hdr| {
                fragment_calls.push((offset, length));
            },
            1,
            &mut header,
            |_| (),
            0,
            &subscriber_position,
        );

        assert_eq!(read_outcome, 1);
        assert_eq!(fragment_calls.len(), 1);
        assert_eq!(subscriber_position.get(), aligned_frame_length as i64);
    }

    #[test]
    fn should_read_multiple_messages() {
        let msg_length = 1;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32_volatile(0, frame_length);
        term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

        let second_offset = aligned_frame_length as usize;
        term_buffer.put_i32_volatile(second_offset, frame_length);
        term_buffer.put_i16(
            second_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_DATA as i16,
        );

        let mut counter_values = vec![0u8; 1024];
        let values_buffer = AtomicBuffer::new(&mut counter_values);
        let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

        let mut header = Header::new(7, 16);
        let mut fragment_calls = Vec::new();

        let read_outcome = TermReader::read_with_position(
            &term_buffer,
            0,
            |_buf, offset, length, _hdr| {
                fragment_calls.push((offset, length));
            },
            usize::MAX,
            &mut header,
            |_| (),
            0,
            &subscriber_position,
        );

        assert_eq!(read_outcome, 2);
        assert_eq!(fragment_calls.len(), 2);
        assert_eq!(subscriber_position.get(), (aligned_frame_length * 2) as i64);
    }

    #[test]
    fn should_not_read_last_message_when_padding() {
        let msg_length = 1;
        let frame_length = HEADER_LENGTH + msg_length;
        let aligned_frame_length = align(frame_length, FRAME_ALIGNMENT);
        let capacity = 4096;
        let frame_offset = capacity - aligned_frame_length as usize;
        let current_position = 1000i64;

        let mut term_bytes = vec![0u8; capacity];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32_volatile(frame_offset, frame_length);
        term_buffer.put_i16(
            frame_offset + protocol::TYPE_FIELD_OFFSET,
            protocol::HDR_TYPE_PAD as i16,
        );

        let mut counter_values = vec![0u8; 1024];
        let values_buffer = AtomicBuffer::new(&mut counter_values);
        let subscriber_position = agrona::counters::AtomicCounter::new(values_buffer, 0);

        let mut header = Header::new(7, 16);
        let mut fragment_calls = Vec::new();

        let read_outcome = TermReader::read_with_position(
            &term_buffer,
            frame_offset,
            |_buf, offset, length, _hdr| {
                fragment_calls.push((offset, length));
            },
            usize::MAX,
            &mut header,
            |_| (),
            current_position,
            &subscriber_position,
        );

        assert_eq!(read_outcome, 0);
        assert!(fragment_calls.is_empty());
        assert_eq!(
            subscriber_position.get(),
            current_position + aligned_frame_length as i64
        );
    }

    #[test]
    fn should_insert_into_empty_buffer() {
        let term_offset = 0;
        let packet_length = 96;
        let mut packet_bytes = vec![0u8; packet_length];
        let packet = AtomicBuffer::new(&mut packet_bytes);
        packet.put_i32(0, packet_length as i32);
        packet.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);
        packet.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, 100);
        packet.put_i32(protocol::DATA_STREAM_ID_FIELD_OFFSET, 200);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        TermRebuilder::insert(&term_buffer, term_offset, &packet, packet_length);

        assert_eq!(term_buffer.get_i32(term_offset), packet_length as i32);
        assert_eq!(
            term_buffer.get_i16(term_offset + protocol::TYPE_FIELD_OFFSET),
            protocol::HDR_TYPE_DATA as i16
        );
        assert_eq!(
            term_buffer.get_i32(term_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            100
        );
        assert_eq!(
            term_buffer.get_i32(term_offset + protocol::DATA_STREAM_ID_FIELD_OFFSET),
            200
        );
    }

    #[test]
    fn should_not_insert_if_already_inserted() {
        let term_offset = 0;
        let packet_length = 96;
        let mut packet_bytes = vec![0u8; packet_length];
        let packet = AtomicBuffer::new(&mut packet_bytes);
        packet.put_i32(0, packet_length as i32);
        packet.put_i32(protocol::DATA_SESSION_ID_FIELD_OFFSET, 100);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32(term_offset, 64);
        term_buffer.put_i32(term_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET, 42);

        TermRebuilder::insert(&term_buffer, term_offset, &packet, packet_length);

        assert_eq!(term_buffer.get_i32(term_offset), 64);
        assert_eq!(
            term_buffer.get_i32(term_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            42
        );
    }

    #[test]
    fn should_fill_gap_at_beginning_of_buffer() {
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);
        meta_buffer.put_i32(
            LOG_DEFAULT_FRAME_HEADER_OFFSET + protocol::DATA_SESSION_ID_FIELD_OFFSET,
            1234,
        );

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        let term_id = 7;
        let gap_offset = 0;
        let gap_length = 64;

        let filled = TermGapFiller::try_fill_gap(
            &meta_buffer,
            &term_buffer,
            term_id,
            gap_offset,
            gap_length,
        );

        assert!(filled);
        assert_eq!(term_buffer.get_i32(gap_offset), gap_length as i32);
        assert_eq!(
            term_buffer.get_i16(gap_offset + protocol::TYPE_FIELD_OFFSET),
            protocol::HDR_TYPE_PAD as i16
        );
        assert_eq!(
            term_buffer.get_i32(gap_offset + protocol::DATA_TERM_OFFSET_FIELD_OFFSET),
            gap_offset as i32
        );
        assert_eq!(
            term_buffer.get_i32(gap_offset + protocol::DATA_TERM_ID_FIELD_OFFSET),
            term_id
        );
        assert_eq!(
            term_buffer.get_i32(gap_offset + protocol::DATA_SESSION_ID_FIELD_OFFSET),
            1234
        );
    }

    #[test]
    fn should_not_fill_gap_when_data_present() {
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32(32, 64);

        let term_id = 7;
        let gap_offset = 0;
        let gap_length = 64;

        let filled = TermGapFiller::try_fill_gap(
            &meta_buffer,
            &term_buffer,
            term_id,
            gap_offset,
            gap_length,
        );

        assert!(!filled);
    }

    #[test]
    fn should_scan_empty_buffer() {
        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        let new_offset = TermBlockScanner::scan(&term_buffer, 0, 4096);
        assert_eq!(new_offset, 0);
    }

    #[test]
    fn should_read_first_message_in_block_scan() {
        let msg_length = 50;
        let aligned_length = align(msg_length, FRAME_ALIGNMENT) as usize;

        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);
        term_buffer.put_i32_volatile(0, msg_length);
        term_buffer.put_i16(protocol::TYPE_FIELD_OFFSET, protocol::HDR_TYPE_DATA as i16);

        let new_offset = TermBlockScanner::scan(&term_buffer, 0, 4096);
        assert_eq!(new_offset, aligned_length);
    }

    #[test]
    fn should_take_no_action_when_message_is_complete() {
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);
        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        term_buffer.put_i32_volatile(0, HEADER_LENGTH);

        let status = TermUnblocker::unblock(&meta_buffer, &term_buffer, 0, 4096, 7);
        assert_eq!(status, UnblockStatus::NoAction);
    }

    #[test]
    fn should_patch_non_committed_message() {
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);
        let mut term_bytes = vec![0u8; 4096];
        let term_buffer = AtomicBuffer::new(&mut term_bytes);

        let msg_length = HEADER_LENGTH * 4;
        term_buffer.put_i32_volatile(0, -msg_length);

        let status = TermUnblocker::unblock(&meta_buffer, &term_buffer, 0, msg_length as usize, 7);
        assert_eq!(status, UnblockStatus::Unblocked);
        assert_eq!(term_buffer.get_i32(0), msg_length);
        assert_eq!(
            term_buffer.get_i16(protocol::TYPE_FIELD_OFFSET),
            protocol::HDR_TYPE_PAD as i16
        );
    }

    #[test]
    fn should_unblock_log_buffer_when_position_has_non_committed_message() {
        let term_length = 65536;
        let mut meta_bytes = vec![0u8; LOG_META_DATA_LENGTH];
        let meta_buffer = AtomicBuffer::new(&mut meta_bytes);

        let initial_term_id_val = 1;
        set_initial_term_id(&meta_buffer, initial_term_id_val);
        initialise_tail_with_term_id(&meta_buffer, 0, initial_term_id_val);

        let shift = position_bits_to_shift(term_length);
        let blocked_offset = (HEADER_LENGTH * 4) as usize;
        let msg_length = HEADER_LENGTH * 4;
        let blocked_position = compute_position(
            initial_term_id_val,
            blocked_offset as i32,
            shift as i32,
            initial_term_id_val,
        );

        let mut term_0_bytes = vec![0u8; term_length];
        let mut term_1_bytes = vec![0u8; term_length];
        let mut term_2_bytes = vec![0u8; term_length];

        let term_buffer_0 = AtomicBuffer::new(&mut term_0_bytes);
        let term_buffer_1 = AtomicBuffer::new(&mut term_1_bytes);
        let term_buffer_2 = AtomicBuffer::new(&mut term_2_bytes);

        term_buffer_0.put_i32_volatile(blocked_offset, -msg_length);

        let term_buffers = [term_buffer_0, term_buffer_1, term_buffer_2];

        let unblocked =
            LogBufferUnblocker::unblock(&term_buffers, &meta_buffer, blocked_position, term_length);
        assert!(unblocked);

        let raw_tail_val = raw_tail_volatile_active(&meta_buffer);
        assert_eq!(
            blocked_position + msg_length as i64,
            compute_position(
                term_id(raw_tail_val),
                (blocked_offset + msg_length as usize) as i32,
                shift as i32,
                initial_term_id_val,
            )
        );
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
