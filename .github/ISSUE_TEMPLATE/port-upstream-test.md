---
name: Port upstream test group
about: Port a group of upstream Aeron Java test files to a Rust equivalent
title: "<layer>: <short description>"
labels: test-parity
---

**Upstream source**

<!-- e.g. vendor/aeron/aeron-driver/src/test/java/io/aeron/driver -->

- [ ] `FooTest.java`
- [ ] `BarTest.java`

**Target**

<!-- e.g. crates/driver/src/media.rs -->

**Exit criteria**

<!-- e.g. N/N green, cumulative parity for this layer -->

**Update after landing**

- [ ] Flip the corresponding entries in `test/upstream_map.jsonl` to `"status": "done"`
      with `"rust_equivalent"` pointing at the new test(s) (or `"skip"` + reason if a
      test is Java-specific and doesn't apply — see `docs/plan.md`).
- [ ] `make test-parity` reflects the new count.
