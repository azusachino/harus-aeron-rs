# Chapter 7: Driver Transport Pollers & Channel Endpoints

This chapter covers the socket transport polling layer and endpoint aggregators for Aeron Media Driver.

A channel endpoint's real job is turning "a UDP socket that has bytes on it" into "a handful of
small, tagged records that someone else can match against a publication or an image." Look at
`crates/driver/src/media/send_channel_endpoint.rs` and
`crates/driver/src/media/receive_channel_endpoint.rs` and you'll find the endpoint itself owns no
session-keyed map at all — it's simpler than that. `SendChannelEndpoint` holds three flat queues:
`status_messages: Vec<StatusMessage>`, `nak_messages: Vec<NakMessage>`, and
`setup_requests: Vec<(i32, i32)>`; `ReceiveChannelEndpoint` holds `setup_messages:
Vec<SetupMessage>` and `data_packets: Vec<DataPacket>`. Every record in every one of those queues
carries `session_id` and `stream_id` fields — `StatusMessage`, `NakMessage`, `SetupMessage`, and
`DataPacket` all tag themselves this way. `take_status_messages()` / `take_nak_messages()` drain
the queue for the `Sender`/`Receiver` duty cycle (Chapter 8) to match, by `session_id` and
`stream_id`, against the actual `NetworkPublication` or `PublicationImage` it concerns. The
session-id-*keyed map* — `images: HashMap<NetworkImageKey, PublicationImage>` — lives one layer up
in `DriverConductor` (`crates/driver/src/conductor.rs`), not on the endpoint itself; the endpoint's
job stops at "read the socket, tag the record, queue it."

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
