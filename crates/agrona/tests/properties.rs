//! Property-based tests for Agrona concurrent primitives using proptest.

use agrona::atomic_buffer::AtomicBuffer;
use agrona::broadcast::{BroadcastReceiver, BroadcastTransmitter, TRAILER_LENGTH};
use agrona::ring_buffer::TRAILER_LENGTH as RB_TRAILER_LENGTH;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;
use agrona::ring_buffer::spsc::OneToOneRingBuffer;
use proptest::prelude::*;

proptest! {
    // 1. Primitive Put/Get Roundtrip Property
    #[test]
    fn prop_atomic_buffer_primitives_roundtrip(
        val_i32 in any::<i32>(),
        val_i64 in any::<i64>(),
        val_f64 in any::<f64>(),
        offset in 0usize..64usize,
    ) {
        let mut backing = [0u8; 256];
        let buf = AtomicBuffer::new(&mut backing);

        buf.put_i32(offset, val_i32);
        prop_assert_eq!(buf.get_i32(offset), val_i32);

        buf.put_i64(offset + 8, val_i64);
        prop_assert_eq!(buf.get_i64(offset + 8), val_i64);

        buf.put_f64(offset + 16, val_f64);
        prop_assert_eq!(buf.get_f64(offset + 16), val_f64);
    }

    // 2. String Put/Get Roundtrip Property
    #[test]
    fn prop_atomic_buffer_string_roundtrip(
        s in "\\PC*",
    ) {
        let max_len = s.len() + 8;
        let mut backing = vec![0u8; max_len + 64];
        let buf = AtomicBuffer::new(&mut backing);

        buf.put_string_utf8(16, &s);
        let read_str = buf.get_string_utf8(16);
        prop_assert_eq!(read_str, s);
    }

    // 3. SPSC Ring Buffer Sequence Property
    #[test]
    fn prop_spsc_ring_buffer_fifo(
        payloads in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..64), 1..20),
    ) {
        let mut backing = vec![0u8; 4096 + RB_TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = OneToOneRingBuffer::new(buf).unwrap();

        let mut read_payloads = Vec::new();

        for (i, payload) in payloads.iter().enumerate() {
            let mut src_backing = vec![0u8; payload.len() + 16];
            let src = AtomicBuffer::new(&mut src_backing);
            src.put_bytes(0, payload);

            let msg_type = (i + 1) as i32;
            let written = rb.write(msg_type, &src, 0, payload.len()).unwrap();
            prop_assert!(written);

            let mut data = Vec::new();
            let mut read_type = 0;
            rb.read(|type_id, read_buf, offset, length| {
                read_type = type_id;
                let mut buf_bytes = vec![0u8; length];
                read_buf.get_bytes(offset, &mut buf_bytes);
                data = buf_bytes;
            });

            prop_assert_eq!(read_type, msg_type);
            read_payloads.push(data);
        }

        prop_assert_eq!(&read_payloads, &payloads);
    }

    // 4. MPSC Ring Buffer Sequence Property
    #[test]
    fn prop_mpsc_ring_buffer_fifo(
        payloads in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..32), 1..10),
    ) {
        let mut backing = vec![0u8; 4096 + RB_TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let rb = ManyToOneRingBuffer::new(buf).unwrap();

        let mut read_payloads = Vec::new();

        for (i, payload) in payloads.iter().enumerate() {
            let mut src_backing = vec![0u8; payload.len() + 16];
            let src = AtomicBuffer::new(&mut src_backing);
            src.put_bytes(0, payload);

            let msg_type = (i + 1) as i32;
            let written = rb.write(msg_type, &src, 0, payload.len()).unwrap();
            prop_assert!(written);

            let mut data = Vec::new();
            let mut read_type = 0;
            rb.read(|type_id, read_buf, offset, length| {
                read_type = type_id;
                let mut buf_bytes = vec![0u8; length];
                read_buf.get_bytes(offset, &mut buf_bytes);
                data = buf_bytes;
            });

            prop_assert_eq!(read_type, msg_type);
            read_payloads.push(data);
        }

        prop_assert_eq!(&read_payloads, &payloads);
    }

    // 5. Broadcast Roundtrip Property
    #[test]
    fn prop_broadcast_roundtrip(
        payloads in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..32), 1..10),
    ) {
        let mut backing = vec![0u8; 4096 + TRAILER_LENGTH];
        let buf = AtomicBuffer::new(&mut backing);
        let tx = BroadcastTransmitter::new(buf.clone()).unwrap();
        let mut rx = BroadcastReceiver::new(buf).unwrap();

        let mut read_payloads = Vec::new();

        for (i, payload) in payloads.iter().enumerate() {
            let mut src_backing = vec![0u8; payload.len() + 16];
            let src = AtomicBuffer::new(&mut src_backing);
            src.put_bytes(0, payload);

            let msg_type = (i + 1) as i32;
            tx.transmit(msg_type, &src, 0, payload.len()).unwrap();

            let mut data = Vec::new();
            let mut read_type = 0;
            let rec = rx.receive_next(|type_id, read_buf, offset, length| {
                read_type = type_id;
                let mut buf_bytes = vec![0u8; length];
                read_buf.get_bytes(offset, &mut buf_bytes);
                data = buf_bytes;
            }).unwrap();

            prop_assert!(rec);
            prop_assert_eq!(read_type, msg_type);
            read_payloads.push(data);
        }

        prop_assert_eq!(&read_payloads, &payloads);
    }
}
