//! Criterion benchmarks for AtomicBuffer operations.

use agrona::atomic_buffer::AtomicBuffer;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_atomic_buffer_primitives(c: &mut Criterion) {
    let mut backing = [0u8; 1024];
    let buf = AtomicBuffer::new(&mut backing);

    c.bench_function("atomic_buffer_put_get_i64", |b| {
        b.iter(|| {
            buf.put_i64(0, black_box(0x123456789ABCDEF0));
            black_box(buf.get_i64(0))
        })
    });

    c.bench_function("atomic_buffer_volatile_i64", |b| {
        b.iter(|| {
            buf.put_i64_volatile(0, black_box(0x123456789ABCDEF0));
            black_box(buf.get_i64_volatile(0))
        })
    });

    c.bench_function("atomic_buffer_cas_i64", |b| {
        let mut val = 0i64;
        b.iter(|| {
            let cur = val;
            val += 1;
            black_box(buf.compare_and_set_i64(0, cur, val))
        })
    });

    c.bench_function("atomic_buffer_get_and_add_i64", |b| {
        b.iter(|| black_box(buf.get_and_add_i64(0, 1)))
    });
}

criterion_group!(benches, bench_atomic_buffer_primitives);
criterion_main!(benches);
