//! Criterion benchmarks for RingBuffer and Broadcast operations.

use agrona::atomic_buffer::AtomicBuffer;
use agrona::broadcast::{BroadcastReceiver, BroadcastTransmitter, TRAILER_LENGTH as BC_TRAILER};
use agrona::ring_buffer::TRAILER_LENGTH as RB_TRAILER;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;
use agrona::ring_buffer::spsc::OneToOneRingBuffer;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_ring_buffers(c: &mut Criterion) {
    c.bench_function("spsc_ring_buffer_write_read_throughput", |b| {
        let mut backing = vec![0u8; 65536 + RB_TRAILER];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = OneToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 64];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_i64(0, 42);

        b.iter(|| {
            rb.write(1, &src, 0, 32).unwrap();
            rb.read(|_msg_type, _buf, _offset, _len| {
                black_box(());
            });
        })
    });

    c.bench_function("mpsc_ring_buffer_write_read_throughput", |b| {
        let mut backing = vec![0u8; 65536 + RB_TRAILER];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut src_backing = [0u8; 64];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_i64(0, 42);

        b.iter(|| {
            rb.write(1, &src, 0, 32).unwrap();
            rb.read(|_msg_type, _buf, _offset, _len| {
                black_box(());
            });
        })
    });

    c.bench_function("broadcast_transmit_receive_throughput", |b| {
        let mut backing = vec![0u8; 65536 + BC_TRAILER];
        let buf = AtomicBuffer::new(&mut backing);
        let tx = BroadcastTransmitter::new(buf.clone()).unwrap();
        let mut rx = BroadcastReceiver::new(buf).unwrap();

        let mut src_backing = [0u8; 64];
        let src = AtomicBuffer::new(&mut src_backing);
        src.put_i64(0, 42);

        b.iter(|| {
            tx.transmit(1, &src, 0, 32).unwrap();
            rx.receive_next(|_msg_type, _buf, _offset, _len| {
                black_box(());
            })
            .unwrap();
        })
    });
}

criterion_group!(benches, bench_ring_buffers);
criterion_main!(benches);
