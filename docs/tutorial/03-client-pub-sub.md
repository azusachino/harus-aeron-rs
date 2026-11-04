# Chapter 3: Client Publication, Subscription, and Conductor

The Aeron client turns channel/stream configuration into driver commands and exposes the data path through `ConcurrentPublication`, `Subscription`, and `Image`. The reference implementation lives under `crates/client`; the learner surface is mirrored under `tutorial/client`.

The client and the driver are separate processes that never call each other directly — every
request crosses a many-to-one ring buffer (Chapter 1) as a flat, fixed-offset record, and every
response comes back on a broadcast buffer as another flat record. `crates/client/src/command.rs`
defines the shape of the request side: `CorrelatedMessageFlyweight` is a 16-byte header —
`client_id` at offset 0, `correlation_id` at offset 8 — that every command (publication add,
subscription add, remove, counter add/remove) wraps as its first field. That `correlation_id` is
the one piece of state that ties an asynchronous request to its eventual reply; the client
generates it, writes it into the ring buffer, and then watches the broadcast buffer for a response
flyweight carrying the same value. There's no request/response pairing at the transport level —
the correlation id in this little table *is* the pairing.

## 1. Channel configuration

`ChannelUri` parses `aeron:ipc` and `aeron:udp?...` strings. `ChannelUriStringBuilder` provides typed setters for endpoints, aliases, reliability, term configuration, and session identifiers. A URI must round-trip through parse and formatting without changing its meaning.

Counter commands place the label length after the key rounded up to a four-byte boundary.
The key length excludes that padding; a three-byte key therefore leaves one padding byte.

## 2. Driver commands

`DriverProxy` serializes command flyweights into the Agrona many-to-one ring buffer. Publication and subscription registration commands carry a client id, correlation id, stream id, and channel. Removal commands carry the registration id. The correlation id is the bridge between an asynchronous driver response and the client-side resource.

## 3. Publication and subscription data paths

`ConcurrentPublication` writes frames into a three-term `LogBuffers` layout and reports Aeron status values such as `NOT_CONNECTED`, `BACK_PRESSURED`, and `CLOSED`. `Subscription` aggregates `Image` instances and polls them round-robin so one active image cannot permanently starve another. `FragmentAssembler` is an optional layer for reconstructing messages split across multiple fragments.

## 4. Client lifecycle

`CommonContext` owns shared directory and timeout configuration. `AeronContext` adds client naming and conductor-agent configuration. `ClientConductor` forwards lifecycle operations through `DriverProxy` and resolves asynchronous driver responses into ready resources; the response state machine is covered in [Chapter 4](04-client-conductor.md). Conclude configuration once, close resources idempotently, and preserve registration ids until the driver has acknowledged removal.

## 5. Errors and authorization

The client distinguishes invalid state/configuration from command-write failures. Storage exhaustion can be detected through an error source chain. `AuthorisationService` provides explicit allow-all and deny-all policies for command authorization.

## Hands-on exercises

Implement the learner stubs in `tutorial/client/src/` in this order:

1. Parse/build channel URIs and round-trip command flyweights.
2. Implement `BufferBuilder`, `TimestampUtil`, and `FragmentAssembler`.
3. Implement `LogBuffers`, `Image`, `ConcurrentPublication`, and `Subscription`.
4. Connect `DriverProxy`, `AeronContext`, and `ClientConductor`.
5. Add property tests for URI/command roundtrips and benchmark the codec/data-path hot loops.

Use the reference files in `crates/client/src/` and verify the learner workspace with:

```bash
make check
```
