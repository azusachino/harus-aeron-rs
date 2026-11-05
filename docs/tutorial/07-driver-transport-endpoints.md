# Chapter 7: Driver Transport Pollers & Channel Endpoints

This chapter covers the socket transport polling layer and endpoint aggregators for Aeron Media Driver.

A channel endpoint's real job is turning "a UDP socket that has bytes on it" into "a handful of
small, tagged records that someone else can match against a publication or an image." Look at
`crates/driver/src/media/send_channel_endpoint.rs` and
`crates/driver/src/media/receive_channel_endpoint.rs` and you'll find the endpoint itself owns no
session-keyed map at all — it's simpler than that. `SendChannelEndpoint` holds two flat queues:
`status_messages: Vec<StatusMessage>` and `nak_messages: Vec<NakMessage>`; `ReceiveChannelEndpoint` holds `setup_messages:
Vec<SetupMessage>` and `data_packets: Vec<DataPacket>`. Every record in every one of those queues
carries `session_id` and `stream_id` fields — `StatusMessage`, `NakMessage`, `SetupMessage`, and
`DataPacket` all tag themselves this way. `take_status_messages()` / `take_nak_messages()` drain
the queue for the `Sender`/`Receiver` duty cycle (Chapter 8) to match, by `session_id` and
`stream_id`, against the actual `NetworkPublication` or `PublicationImage` it concerns. The
session-id-*keyed map* — `images: HashMap<NetworkImageKey, PublicationImage>` — lives one layer up
in `DriverConductor` (`crates/driver/src/conductor.rs`), not on the endpoint itself; the endpoint's
job stops at "read the socket, tag the record, queue it."

Before the receiver advances a position or queues a packet, it validates every DATA or PAD
frame, including later frames. The conductor repeats the check against the image term length
before rebuilding. A final PAD may advertise more logical bytes than the UDP packet carries;
a zero-length DATA heartbeat carries only its header. Malformed packets do not partially insert
an earlier valid frame. Sender-side SM and NAK fields are checked against the publication term
on the endpoint, and incoming Status Messages are strictly validated against sender-relative
position bounds (half a term behind to 1.5 terms ahead of `sender_position`) before updating
flow control limits. Messages with `SEND_SETUP_FLAG` elicit setup frames without sender-relative
position rejection.

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
