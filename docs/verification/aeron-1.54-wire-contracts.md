# Aeron 1.54.0 client contract comparison

Reference: `aeron-io/aeron` tag `1.54.0`, commit
`454da1e4e6c7edf3dd6d34878512d3243384992a`; Rust baseline
`80ceec49caaeb976988130e07b5d903de01cd9f9`. Comparisons below are source
inspection, except the counter vector and tests explicitly described. Offsets are bytes.
They do not establish complete wire or runtime parity.

The upstream paths below are under `aeron-client/src/main/java/io/aeron/` at
[the pinned commit](https://github.com/aeron-io/aeron/tree/454da1e4e6c7edf3dd6d34878512d3243384992a).
Rust protocol primitives use little-endian `AtomicBuffer`; Java network flyweights
explicitly use little endian. Java command and shared metadata flyweights use native
Agrona buffer order; this comparison and vector were run on little-endian hardware.
Big-endian shared-memory interoperability is not established.

| Rust surface | Upstream source and compared layout |
| --- | --- |
| Header | `protocol/HeaderFlyweight.java`: length 0, version 4, flags 5, type 6, size 8, current version 0. |
| DATA | `protocol/DataHeaderFlyweight.java`: term offset 8, session 12, stream 16, term 20, reserved 24; size 32. |
| NAK | `protocol/NakFlyweight.java`: session 8, stream 12, term 16, term offset 20, length 24; size 28. |
| SM | `protocol/StatusMessageFlyweight.java`: session 8, stream 12, term 16, offset 20, window 24, receiver 28; size 36, optional feedback starts 36. |
| SETUP | `protocol/SetupFlyweight.java`: term offset 8, session 12, stream 16, initial term 20, active term 24, term length 28, MTU 32, TTL 36; size 40. |
| RTTM | `protocol/RttMeasurementFlyweight.java`: session 8, stream 12, timestamp 16, delta 24, receiver 32; size 40. |
| ERR | `protocol/ErrorFlyweight.java`: session 8, stream 12, receiver 16, group tag 24, code 32, string length 36, bytes 40. |
| Commands | `command/CorrelatedMessageFlyweight.java`: client 0, correlation 8. Publication stream 16, channel length 20/data 24; subscription registration 16, stream 24, channel length 28/data 32; destination registration 16, channel length 24/data 28. Remove registration 16; terminate token length 16/data 20. See the corresponding named flyweight sources. |
| Counter command | `command/CounterMessageFlyweight.java`: type 16, key length 20, key 24, label length at `24 + align(keyLength, 4)` and label immediately after it. Rust previously omitted key padding. Java `computeLength` reserves four extra trailing bytes beyond `length()`; the meaningful fields and minimum accepted encoded length are unchanged. |
| CnC | `CncFileDescriptor.java`: version 0, region lengths 4/8/12/16/20, timeout 24, start 32, PID 40, page size 48, metadata region 128. Version is 0.2.0 (512), independent of release version. Java checks the major version; Rust currently requires exact equality. |
| Log metadata | `logbuffer/LogBufferDescriptor.java`: tails 0, active count 24, EOS 128, connected 136, initial term 264, default header length 268, MTU 272, term length 276, page size 280, default header 320; metadata length 4096. |

Archive control protocol is separately versioned: target `AeronArchive.Configuration`
uses 1.12.0, matching `crates/archive/src/protocol/codecs.rs`. This does not validate
Archive connect schema compatibility (S6).

## Validation families and later runtime work

Target `protocol/NakFlyweight.isValid(termLength)` requires nonnegative, 32-byte
aligned offset strictly below the term length, nonnegative length, and widened
`offset + length <= termLength`. `StatusMessageFlyweight.isValid(termLength)`
requires the same offset bounds/alignment and a window from zero through half a
term. S4 additionally owns sender-relative bounds and the SETUP-request exception;
these checks alone do not establish those bounds.

`SetupFlyweight.isValid()` requires a power-of-two term length in 64 KiB..1 GiB,
an aligned offset in the term, and a 32-byte aligned MTU from 32 through 65504.
`ResolutionEntryFlyweight.isValid()` requires an IPv4 or IPv6 type, enough bytes
for its address and two-byte little-endian name length, a positive name length at
most 512, and enough bytes for the name. The entry's encoded extent is aligned to
8 bytes. Rust has resolver cache primitives but no resolution-entry flyweight;
that validation is unsupported. Source: the four corresponding flyweights and
`aeron-client/src/test/java/io/aeron/protocol/*FlyweightTest.java` at the pinned
commit. Rust's existing flyweights expose fields, not these validation methods.
S3 must apply applicable checks to complete packets before state mutation, including
later packed frames, lengths, heartbeat/padding and bounds; a layout match is not
malformed-packet rejection. SM feedback lengths other than the supported 0 or 8
bytes and bounded error strings also need runtime handling. Other frame types,
response flyweights and full CnC version negotiation were not exhaustively audited.

## Counter vector

`test/wire_contracts/CounterVector.java` uses Java's actual counter flyweight and
produced `0807060504030201181716151413121124232221030000003132330003000000616263`.
It was run with Corretto 25.0.5 and the original checkout's
`aeron-all-1.54.0.jar` (SHA-256
`88cc2de0f4406f1cd137b69c87e3020b708e5663ab15a31633d4f06e77ac296c`).
The original JAR was already present. After initializing the exact gitlink, the
interop smoke built a local JAR with the same SHA-256 and the vector was reproduced
with that JAR. Gradle cache provenance was not independently established, so
exact-tag source inspection above remains the authority for the alignment.
Compile with that JAR on the classpath, then run with
`--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`. Rust tests compare the
encoded bytes and decode the Java bytes, including a truncated-label rejection.
Existing conductor coverage also drives an odd-length key through the real CnC ring.
