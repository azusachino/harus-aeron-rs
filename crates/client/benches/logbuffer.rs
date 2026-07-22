use aeron_client::logbuffer::{
    FRAME_ALIGNMENT, HEADER_LENGTH, Header, TermReader, TermScanner, align,
};
use agrona::AtomicBuffer;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn bench_term_scanner(c: &mut Criterion) {
    let mut group = c.benchmark_group("term_scanner");

    for num_fragments in [10, 100, 1000].iter() {
        let frame_len = HEADER_LENGTH + 64;
        let aligned_len = align(frame_len, FRAME_ALIGNMENT) as usize;
        let total_len = num_fragments * aligned_len;

        let mut bytes = vec![0u8; total_len + 64];
        let term_buffer = AtomicBuffer::new(&mut bytes);

        let mut offset = 0;
        for _ in 0..*num_fragments {
            term_buffer.put_i32_volatile(offset, frame_len);
            offset += aligned_len;
        }

        group.bench_with_input(
            BenchmarkId::new("scan_for_availability", num_fragments),
            num_fragments,
            |b, _| {
                b.iter(|| TermScanner::scan_for_availability(&term_buffer, 0, total_len));
            },
        );
    }

    group.finish();
}

fn bench_term_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("term_reader");

    for fragment_size in [32, 256, 1024].iter() {
        let frame_len = HEADER_LENGTH + fragment_size;
        let aligned_len = align(frame_len, FRAME_ALIGNMENT) as usize;
        let num_fragments = 100;
        let total_len = num_fragments * aligned_len;

        let mut bytes = vec![0u8; total_len + 64];
        let term_buffer = AtomicBuffer::new(&mut bytes);

        let mut offset = 0;
        for _ in 0..num_fragments {
            term_buffer.put_i32_volatile(offset, frame_len);
            offset += aligned_len;
        }

        group.bench_with_input(
            BenchmarkId::new("read_throughput", fragment_size),
            fragment_size,
            |b, _| {
                let mut header = Header::new(1, 16);
                b.iter(|| {
                    TermReader::read(
                        &term_buffer,
                        0,
                        |_buf, _offset, _len, _hdr| {},
                        usize::MAX,
                        &mut header,
                        |_| (),
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_term_scanner, bench_term_reader);
criterion_main!(benches);
