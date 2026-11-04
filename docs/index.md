# Aeron in Rust

Build a wire-compatible Aeron implementation in Rust, from Agrona primitives and
shared-memory IPC through the Media Driver, Archive, and bounded Cluster foundations.

## Start here

| Path | Purpose |
| --- | --- |
| [Tutorial overview](tutorial/README.md) | Course map and learner-stub workflow |
| [System architecture](guides/architecture.md) | Reference view of the implemented crates |
| [Java interop soak](interop-soak.md) | Current Aeron `1.50.x` interoperability gate |
| [Roadmap](plan.md) | Milestones, parity accounting, and explicit ceilings |

## Work loop

```bash
make astro-dev        # preview this site locally
make tutorial-check   # compile the learner workspace
make check            # verify the Rust reference implementation
```

## Course shape

```mermaid
flowchart LR
  F[Foundations] --> C[Client]
  C --> D[Media Driver]
  D --> I[Interop]
  I --> A[Archive]
  A --> K[Cluster]
```

Each tutorial chapter points at the corresponding learner stub and reference crate.
The course teaches Aeron concepts and Rust implementation boundaries together; a
passing `tutorial-check` only confirms the stubs compile, not that they are complete.

## Documentation map

- `guides/` — architecture and implementation guides
- `tutorial/` — chapter-by-chapter learner material
- `plan.md` — roadmap and release gates
- `specs/` — approved wire and control-plane specifications
- `*parity-boundaries.md` — evidence and intentionally unsupported surfaces
