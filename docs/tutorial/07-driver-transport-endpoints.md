# Chapter 7: Driver Transport Pollers & Channel Endpoints

This chapter covers the socket transport polling layer and endpoint aggregators for Aeron Media Driver.

## UDP Channel Transport & Pollers

- `crates/driver/src/media/udp_channel_transport.rs`: Non-blocking UDP socket wrapper (`send_to`, `recv_from`, `bind`).
- `crates/driver/src/media/transport_poller.rs`: Non-blocking `DataTransportPoller` and `ControlTransportPoller` duty-cycle pollers for multiplexing datagram read operations across registered channel transports.

## Channel Endpoints

- `crates/driver/src/media/send_channel_endpoint.rs`: Aggregates active network publications over a single UDP transport channel, emitting DATA and SETUP frames while handling SM (Status Message) and NAK feedback.
- `crates/driver/src/media/receive_channel_endpoint.rs`: Aggregates subscriptions on a transport channel, demuxing incoming DATA packets to image stream buffers and transmitting periodic status messages and NAK retransmission requests.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
