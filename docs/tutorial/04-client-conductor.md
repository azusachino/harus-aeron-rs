# Chapter 4: ClientConductor and Driver Events

`ClientConductor` is the asynchronous boundary between an Aeron client and the media driver. A
command is identified by a correlation id, and the driver later publishes a typed response on the
Agrona broadcast buffer. The conductor must reconcile those responses into resources without
assuming that the driver is trustworthy or that responses arrive in command order.

## Response flyweights and demultiplexing

`driver_events.rs` mirrors the upstream response layouts: publication-ready, subscription-ready,
available/unavailable image, error, operation-success, client-timeout, and next-session-id events.
`DriverEventsAdapter` owns `BroadcastReceiver`, validates each record, and returns typed
`DriverEvent` values for the conductor duty cycle.

## Registration lifecycle

Every command begins as `RegistrationState::Pending`. A matching ready response promotes it to
`Ready`; removal, an error, a timeout, or client close moves it to `Closed`. Correlation ids are
kept separately from publication registration ids because the driver may assign a different
resource id for normal publications.

## Resource resolution

Publication and image responses carry a log-buffer file name. The conductor resolves it through
`LogBuffers::from_file` (or an explicitly registered embedded buffer), reads the initial term id,
and binds the relevant shared counter value. Subscription-ready creates the `Subscription`, while
available-image adds an `Image` and invokes its callback.

## Duty cycle and controls

`do_work` drains driver events, periodically sends `CLIENT_KEEPALIVE`, and reports an
inter-service timeout for registrations that never become ready. The conductor also exposes
control-protocol version gating for the next-session-id command and optional log-buffer
pre-touching.

## Exercises

1. Add a broadcast-buffer integration test that publishes a publication-ready event.
2. Add a wrong-resource-type removal test for a registration id shared with the other resource map.
3. Extend the property tests when adding a new response flyweight.

Run the complete client and workspace checks with:

```bash
make check
```
