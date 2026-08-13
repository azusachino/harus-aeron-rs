#![no_main]

use aeron_client::logbuffer::{Header, TermReader};
use agrona::AtomicBuffer;
use libfuzzer_sys::fuzz_target;

const TERM_CAPACITY: usize = 4096;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let mut bytes = vec![0u8; TERM_CAPACITY];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = data[index % data.len()];
    }

    let term = AtomicBuffer::new(&mut bytes);
    let mut header = Header::new(0, 16);
    let _ = TermReader::read(
        &term,
        0,
        |_buffer, _offset, _length, _header| {},
        32,
        &mut header,
        |_error| {},
    );
});
