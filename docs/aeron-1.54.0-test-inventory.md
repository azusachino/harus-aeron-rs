# Aeron 1.54.0 test inventory reconciliation

The parity reporter reads the exact target tree `454da1e4e6c7edf3dd6d34878512d3243384992a` and verifies that the current ledger keys match its Java test paths. A source path is counted once per `.java` file under the five modules' `src/test/` trees. This is file inventory, not method coverage.

At this revision there are 335 current paths: 68 added, 95 retained paths with changed Git blobs, and 172 unchanged retained paths. One historical path is absent: `aeron-system-tests/src/test/java/io/aeron/driver/DriverNameResolverTest.java`. The test was split across `aeron-driver/src/test/java/io/aeron/driver/DriverNameResolverTest.java` and `aeron-system-tests/src/test/java/io/aeron/driver/DriverNameResolverSystemTest.java`; the old row stays in JSONL as historical evidence and is excluded from the current denominator. Git blob identity detects content changes, not behavioral changes.

## Audit disposition

All 68 added and 95 changed rows carry `audit_disposition: pending-source-review`; changed rows retain `previous_blob`, and each active row records the exact source revision and blob. This is an explicit unresolved disposition, not a claim that every source file represents behavior that Rust must implement. A bounded source audit must classify each row as behavior change, formatting-only, bounded equivalent, justified fixture, or still-unverified evidence before its existing status is treated as current behavioral evidence. Until then the ledger status is historical coverage evidence and `done` means an implemented or bounded Rust equivalent only at the recorded earlier audit scope.

This S1 pass establishes the reproducible inventory and queue; it does not complete all 163 source reviews. The machine-readable queue is `test/upstream_map.jsonl`. Use `uv run scripts/upstream_tests.py report` to see current resolved equivalents and pending review separately. `skip` is never included in implemented-equivalent counts.

The resolver relocation is not evidence that the Java behavior disappeared. The old system-test row remains marked absent, and the two current paths are separate inventory entries pending review. BufferClaim and controlled polling now exist in Rust; system-test rows that say those APIs are absent are stale and must be read as missing end-to-end depth instead.
