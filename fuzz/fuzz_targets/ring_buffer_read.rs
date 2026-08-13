#![no_main]

use agrona::AtomicBuffer;
use agrona::ring_buffer::{
    HEAD_POSITION_OFFSET, ManyToOneRingBuffer, TAIL_POSITION_OFFSET, TRAILER_LENGTH,
};
use libfuzzer_sys::fuzz_target;

const DATA_CAPACITY: usize = 256;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let mut backing = vec![0u8; DATA_CAPACITY + TRAILER_LENGTH];
    for (index, byte) in backing[..DATA_CAPACITY].iter_mut().enumerate() {
        *byte = data[index % data.len()];
    }

    let buffer = AtomicBuffer::new(&mut backing);
    buffer.put_i64(DATA_CAPACITY + HEAD_POSITION_OFFSET, 0);
    buffer.put_i64(DATA_CAPACITY + TAIL_POSITION_OFFSET, DATA_CAPACITY as i64);

    let ring = ManyToOneRingBuffer::new(buffer).expect("fixed fuzz geometry is valid");
    let _ = ring.read_with_limit(
        |_, message_buffer, offset, length| {
            let readable = length.min(message_buffer.capacity().saturating_sub(offset));
            if readable > 0 {
                let mut message = vec![0u8; readable];
                message_buffer.get_bytes(offset, &mut message);
            }
        },
        32,
    );
});
