# Chapter 8: Driver Conductor & Agent Duty Cycles

This chapter covers the core agent duty cycles and lifecycle orchestration for Aeron Media Driver.

The Media Driver is three agents that each own one table and get polled in a fixed order —
`MediaDriver::do_work` in `crates/driver/src/driver.rs` calls `conductor.do_work`, then
`sender.do_work`, then `receiver.do_work_at`, every cycle, with no thread handoff or locking
between them. Once you see each agent's owned state, the "duty cycle" stops being a flowchart and
becomes "iterate my table, do the obvious thing per row":

- `Sender` (`crates/driver/src/sender.rs`) owns `send_channel_endpoints: Vec<SendChannelEndpoint>`
  and `network_publications: Vec<NetworkPublication>`. Each cycle it walks every publication and
  flushes whatever data is ready to send — the endpoint queues from Chapter 7 are drained here.
- `Receiver` (`crates/driver/src/receiver.rs`) owns `receive_channel_endpoints:
  Vec<ReceiveChannelEndpoint>`. Each cycle it polls every endpoint's transport for a datagram,
  classifies the frame type, and dispatches it into the matching `ReceiveChannelEndpoint`'s queue
  or image buffer.
- `DriverConductor` (`crates/driver/src/conductor.rs`) owns the biggest table by far — over a
  dozen `HashMap`s keyed by registration id, session id, or `(client_id, stream_id)`, including
  `publications`, `subscriptions`, `images: HashMap<NetworkImageKey, PublicationImage>`, and
  `session_states`. It is the only agent that talks to clients (over the to-driver ring buffer) and
  the only one that creates or removes rows in the tables the other two agents read.

Because the three agents run cooperatively on one thread in a fixed order with no shared mutable
state protected by anything other than "only one agent touches this `Vec`/`HashMap` per cycle,"
there's never a race to reason about between them — just read the table each agent owns, and the
duty cycle is obvious.

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
