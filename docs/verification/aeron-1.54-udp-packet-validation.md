# Aeron 1.54 UDP packet validation

Compared against tag `1.54.0`, commit `454da1e4e6c7edf3dd6d34878512d3243384992a`.
`aeron-driver/src/main/java/io/aeron/driver/media/UdpChannelTransport.java` checks
version, packet alignment and minimum type sizes; `ReceiveChannelEndpoint.java`
checks SETUP fields. `PublicationImage.validatePacket` validates the whole DATA/PAD
packet against the term before insertion: aligned, contiguous term offsets, minimum
frame lengths, term bounds, and a terminal PAD that may have a logical length beyond
the transmitted bytes. `DataHeaderFlyweight.isHeartbeat` identifies an exactly
32-byte DATA packet with a zero frame length. `PublicationImageTest` exercises
logical padding and heartbeat positions. Rust also checks the version, identity and
term id of every packed frame; Java's image validation explicitly assumes those
fields are sane. This stricter check prevents cross-session partial delivery.

`packet_validation.rs` runs before receiver queues, expected positions, status
messages, and conductor image insertion. The conductor checks again against the
actual image term and validates the entire packet's flow-control range first.
A short final PAD copies only its transmitted bytes but advances by its logical
length; heartbeat does not overwrite the log and EOS heartbeat records its position.
SETUP geometry follows `SetupFlyweight.isValid`. Control packets require a valid
version, complete lengths, and supported SM feedback size; publication processing
checks SM window/offset and NAK offset/length against the term. Sender-relative SM
position bounds are still S4. This implementation accepts SM feedback of zero or
8 bytes; arbitrary application feedback, resolution frames, response setup and
full multicast/receiver identity semantics remain unsupported.

The raw-socket system regression sends an invalid second frame and proves that the
subscription receives neither frame, then sends a valid pair and a header-only PAD
followed by DATA. The property test feeds arbitrary datagrams through preflight.
Existing two-driver, fragmentation and NAK retransmission tests cover adjacent
paths, not complete upstream method or transport parity.
