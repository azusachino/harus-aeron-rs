# Tutorial

Course layer mirroring [`harus-aeron-zig`](https://github.com/azusachino/harus-aeron-zig)'s
tutorial structure. Not yet written — chapters land alongside their `crates/*`
reference implementation, same convention as the zig repo:

1. Implement the chapter in `crates/<module>`.
2. Write the matching learner stub in `tutorial/<module>` (bodies are `todo!()`).
3. Write the chapter doc here.

Planned parts (see [`docs/plan.md`](../plan.md) for phase ordering):

1. Foundations — frame codecs, lock-free ring buffers, shared memory counters
2. Data Path — term appenders/readers, UDP transport
3. The Driver — Conductor/Sender/Receiver agents, CnC file
4. Client API — Publication/Subscription, Java interop
5. Archive — control protocol, persistent recording catalogs
6. Cluster — Raft election, replicated log coordination

Client chapters: [`02-client-wire-protocol.md`](02-client-wire-protocol.md),
[`03-client-pub-sub.md`](03-client-pub-sub.md), and
[`04-client-conductor.md`](04-client-conductor.md).
