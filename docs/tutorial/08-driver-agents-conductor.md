# Chapter 8: Driver Conductor & Agent Duty Cycles

This chapter covers the core agent duty cycles and lifecycle orchestration for Aeron Media Driver.

## Agent Duty Cycles

- `crates/driver/src/sender.rs`: `Sender` agent duty cycle for iterating active `SendChannelEndpoint`s and flushing packet transmission buffers over non-blocking UDP sockets.
- `crates/driver/src/receiver.rs`: `Receiver` agent duty cycle for iterating `DataTransportPoller`s and dispatching incoming datagram frames to `ReceiveChannelEndpoint`s and publication image buffers.
- `crates/driver/src/conductor.rs`: `DriverConductor` command processor managing publication and subscription allocations (`on_add_publication`, `on_add_subscription`, `on_remove_publication`, `on_remove_subscription`) and registration IDs.

## MediaDriver Lifecycle

- `crates/driver/src/driver.rs`: `MediaDriver::launch` orchestrates driver configuration and agent instances (`Conductor`, `Sender`, `Receiver`).
- `crates/driver/src/bin/aeron-driver.rs`: Executable entry point initializing tracing logging and launching `MediaDriver`.

## Verification

Run all tests and workspace checks with:

```bash
make check
```
