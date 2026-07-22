# Architecture

Skeleton document — fill in as each crate gains real implementation.

## Crate map

Mirrors upstream `aeron-io/aeron`'s actual Gradle module graph (`vendor/aeron/build.gradle`):
`agrona` is an external dependency, not an Aeron module; `protocol`/`logbuffer`/`media`
are packages inside `aeron-client`/`aeron-driver`, not separate modules.

| Crate | Purpose | Depends on |
|---|---|---|
| `crates/agrona` | Ring buffer, broadcast, atomic buffer, idle strategies, counters | — |
| `crates/client` (`aeron-client`) | Wire protocol (`protocol.rs`) + log buffer (`logbuffer.rs`) + Publication/Subscription API | `agrona` |
| `crates/driver` (`aeron-driver`) | MediaDriver: Conductor, Sender, Receiver agents + UDP transport (`media.rs`); hosts the `aeron-driver` binary | `aeron-client`, `agrona` |
| `crates/archive` (`aeron-archive`) | Recording, replay, catalog persistence (future) | `aeron-client` |
| `crates/cluster` (`aeron-cluster`) | Raft consensus, replicated log (future) | `aeron-client`, `aeron-archive` |

## Runtime model

Aeron's driver agents (Conductor/Sender/Receiver) are duty-cycle loops with
configurable idle strategies (busy-spin, back-off, yielding), not thread-per-connection.
`crates/driver`'s `media` submodule uses `tokio` for async UDP I/O, but the intent is
to keep the duty-cycle model rather than fan work out across arbitrary tokio tasks —
match the zig implementation's agent boundaries so the two stay comparable.
