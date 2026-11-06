# Driver/client system-test parity boundaries

The authoritative row-level ledger is [`test/upstream_map.jsonl`](https://github.com/azusachino/harus-aeron-rs/blob/main/test/upstream_map.jsonl).
This document records the v0.0.9 hardening audit of the 83 upstream
`aeron-system-tests` driver/client rows: 43 bounded Rust equivalents and 40
intentional skips.

## What `done` means here

`done` means that the Rust tree has executable evidence for the row's bounded
behavior. It does not mean that every JUnit method or parameterization in the
upstream file has been ported. Each ledger entry names the Rust test or source
boundary and states the missing ceiling where the behavior is narrower.

The strongest evidence is in
[`crates/driver/tests/system_tests.rs`](https://github.com/azusachino/harus-aeron-rs/blob/main/crates/driver/tests/system_tests.rs):

- real CnC registration and IPC publication/subscription;
- real UDP publication/subscription across two drivers;
- raw SETUP/DATA/STATUS/NAK frame exchange;
- single- and multi-gap loss recovery through a relay;
- manual multi-destination cast to a second receiver;
- fragmentation/reassembly, spy polling, reconnect, churn, liveness, session
  conflict, and bounded flow-control scenarios.

The remaining bounded rows use the corresponding client/driver contract tests
for URI, term geometry, image positions/EOS, session admission, CnC metadata,
wildcard ports, and resolver primitives. Those are useful regression evidence,
but are not promoted to full Java system-test parity.

## Intentional skips

The 40 driver/client skips name a missing boundary rather than hiding it. The
main groups are:

- API and runtime depth: `BufferClaim` (commit/abort, reserved values over IPC and UDP), controlled polling (`Abort`, `Break`, `Continue`), and `ControlledFragmentAssembler` retry semantics are fully exercised end to end (verified in S11); remaining unexercised areas include async resources, two-buffer offers, and multi-stream exclusive publication depth;
- transports or driver features not implemented: true multicast, multipath,
  response channels, publication revoke/reject-image, live resolver gossip,
  timestamp writes, and strategy-selected multicast flow control;
- Java-only infrastructure and diagnostics: helper/base classes, JMX echo,
  environment dumps, TestMediaDriver fixtures, and callback/re-entry plumbing;
- CnC contracts that are deliberately not claimed as wire-compatible yet:
  system-counter metadata/values and counter ownership/reference lifecycles;
- external resource/security behavior: deterministic disk exhaustion and the
  Java authentication/authorization services.

These skips do not count as Rust parity. They are the next substantive parity
backlog, even though the file ledger has no remaining `todo` rows.

## Verification

```bash
make test-parity
cargo test -p aeron-driver --test system_tests
make check
```

The 1.0.0 near-100% goal remains unmet until the skipped boundaries are either
implemented with real evidence or retired as explicitly out of scope for the
release.
