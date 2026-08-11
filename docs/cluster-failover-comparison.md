# Cluster failover comparison

The v0.0.8 comparison has two layers:

1. `crates/cluster/tests/failover_repro_tests.rs` runs a deterministic in-memory
   3,000-message election model.
2. `scripts/cluster_failover_repro.py` starts three independent Tokio node
   processes and one UDP client, kills member 0 during a paced load, and checks
   acknowledgements plus survivor leader-change logs.

The issue-shaped run on 2026-08-11 used:

```bash
ORDER_COUNT=100 ORDER_INTERVAL_MS=1000 KILL_DELAY_MS=10000 \
ELECTION_TIMEOUT_MS=5000 OFFER_TIMEOUT_MS=60000 RESPONSE_TIMEOUT_MS=250 \
make cluster-failover-repro
```

Observed result: `accepted=100 total=100`; both surviving processes observed the
term-2 leader and did not exceed the repeated-election guard. A 30-order smoke
run with a 300 ms election timeout also passed.

This is real Tokio UDP/process-boundary evidence, but the harness uses a small
custom comparison protocol. It does not exercise Aeron framing, the Aeron Cluster
client, replicated log entries, or Archive recovery, so it is not full upstream
`StalledLeaderLogReplicationClusterTest` parity.
