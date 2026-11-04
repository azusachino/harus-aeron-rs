# Aeron 1.54.0 baseline assessment

## Recommendation and evidence boundary

Adopt the exact Aeron `1.54.0` release as the next upstream baseline before expanding system-test parity. Treat changing the reference, updating coverage accounting, and proving compatibility as separate acceptance steps. This assessment does not change the pin or claim 1.54.0 interoperability.

Inspected on 2026-11-04 against Rust `main` at `87950250bd7215659f8641c1374a4e67a0151576`. The existing local `make check` passed in this session before these documentation changes. Java interop was not rerun locally; the upstream submodules are uninitialized. [CI for this Rust commit](https://github.com/azusachino/harus-aeron-rs/actions/runs/38036275947) passed against the existing reference.

## Vendoring and revisions

The workstation already catalogs and clones `harus-aeron-rs` as an independent repository. Within it, `.gitmodules` declares upstream Aeron at `vendor/aeron`, tracking `release/1.50.x`. The recorded gitlink is `8e0cdb2f92800a047c0066028444b790a80fe593`; upstream's [version file at that revision](https://github.com/aeron-io/aeron/blob/8e0cdb2f92800a047c0066028444b790a80fe593/version.txt) says `1.50.5-SNAPSHOT`. This is a branch snapshot rather than an exact release tag.

GitHub identifies [1.54.0](https://github.com/aeron-io/aeron/releases/tag/1.54.0), published 2026-10-27, as the latest release at inspection. Its tag resolves to `454da1e4e6c7edf3dd6d34878512d3243384992a`, whose `version.txt` says `1.54.0`. The tagged changelog still labels its section “unreleased”; the published release and version file establish the release identity.

The Makefile's `AERON_UPSTREAM_REF` variable does not control its setup recipe: that recipe uses `git submodule update --remote`, which follows `.gitmodules`. Changing the variable alone would not select 1.54.0. Existing submodule URLs use HTTPS; workstation policy requires SSH for future Git remote operations. Align transport before initializing or fetching references.

## Relevant changes since the current reference

These are selected from the official [tagged changelog](https://github.com/aeron-io/aeron/blob/1.54.0/CHANGELOG.adoc), including intervening 1.51–1.53 releases. They are audit targets, not assertions that each upstream bug exists in Rust.

| Area | Upstream change | Rust consequence / next check |
| --- | --- | --- |
| Archive connection | 1.53 removes legacy `ConnectRequest`, template 2. Template 58 `AuthConnectRequest` remains. | Rust `protocol/codecs.rs` and `service/control_session_adapter.rs` use template 2. Implement and test the modern connection boundary, including empty credentials and explicit authentication limits. |
| Archive schema | Schema 101 moves from version 13 to 14. | Diff supported templates and regenerate Java golden vectors. A schema-number replacement alone does not establish compatibility. |
| Archive functionality | 1.51 adds persistent subscriptions; 1.52 stops concurrent replays before truncation; 1.53 restricts control requests to the session's source image. | Classify persistent subscriptions explicitly. Audit truncation/replay ownership and source-image isolation against the managed Rust service. |
| Archive counters | 1.54 removes Java lookup overloads without `archiveId`, and adds it to C/C++ lookup APIs. | Rust already accepts `archive_id`; review its wildcard semantics and multi-Archive isolation rather than assuming a missing parameter. |
| Driver validation | 1.51 adds strict Status Message position bounds; later releases adjust them to half a term behind and 1.5 terms ahead. 1.53 validates all frames in received packets. | Audit sender bounds and complete packet validation, then add focused malformed-packet and boundary tests. |
| Driver lifecycle | Fixes include live receivers dropped by min flow control, dynamic-MDC destination removal, and untethered LINGER subscribers preventing draining. | Prioritize real duty-cycle regression tests around these implemented Rust boundaries. |
| Ring buffer | 1.54 C client fences the MPSC claim marker before writing the record (PR #2156). | Compare the Rust MPSC publication/reuse ordering and Agrona reference algorithm. Confirm with Loom; do not mechanically copy a C fence. |
| Client API | 1.53 removes `controlledPeek` and the image-position setter. | This does not remove controlled polling. Continue the planned `controlled_poll` system tests against current APIs. |
| Driver architecture/configuration | 1.52 moves log creation/removal to `NativeResourceAgent`; 1.54 adds UDP traffic-class configuration and broader affinity controls. | Record unsupported scheduling/configuration boundaries separately from wire compatibility; assess conductor stalls before expanding threading architecture. |
| Cluster | Version-validator, recovery, snapshot ordering, service-timeout, and active-log join fixes span these releases. | Re-audit the bounded core; the custom UDP failover harness still does not prove Aeron Cluster/Archive compatibility. |

Direct schema inspection confirms [version 14 and removal of template 2](https://github.com/aeron-io/aeron/blob/1.54.0/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml). The [old schema](https://github.com/aeron-io/aeron/blob/8e0cdb2f92800a047c0066028444b790a80fe593/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml) is version 13. Both XML files declare semantic version `5.2`; keep SBE schema version and the separately advertised runtime protocol version distinct during the audit.

The 1.54.0 [CnC descriptor](https://github.com/aeron-io/aeron/blob/1.54.0/aeron-client/src/main/java/io/aeron/CncFileDescriptor.java) still declares `SemanticVersion.compose(0, 2, 0)`, matching Rust's `CNC_VERSION = 512`. This is one matching field, not proof of complete layout or behavior compatibility.

## Test inventory impact

Compared the existing JSONL ledger with all `.java` paths under each selected module's `src/test/` in the [1.54.0 Git tree](https://api.github.com/repos/aeron-io/aeron/git/trees/454da1e4e6c7edf3dd6d34878512d3243384992a?recursive=1). The API reported a complete, non-truncated tree.

| Module | Existing ledger | 1.54.0 files | New paths | Old paths absent |
| --- | ---: | ---: | ---: | ---: |
| Client | 33 | 56 | 23 | 0 |
| Driver | 41 | 55 | 14 | 0 |
| Archive | 30 | 39 | 9 | 0 |
| Cluster | 38 | 48 | 10 | 0 |
| System tests, all three subsystems | 126 | 137 | 12 | 1 |
| Total | 268 | 335 | 68 | 1 |

These counts include helpers/fixtures, matching the existing script's path-selection rule. They are not test-method counts or behavioral parity percentages. Existing files may contain new or changed methods.

`scripts/upstream_tests.py init` only adds paths and preserves existing entries. It cannot reconcile the missing old path or invalidate stale `done`/`skip` explanations. An additive refresh alone would yield 336 ledger entries, not the current 335-path inventory. Preserve historical evidence while explicitly reconciling removed/renamed paths and revalidating changed files. The existing system-test entries that say BufferClaim and controlled polling are absent are already stale after PR #42.

## Proposed implementation sequence

1. Pin the exact 1.54.0 commit, make setup reproducible, and align upstream Git transport with SSH policy. Retain the old revision in Git history and this assessment.
2. Reconcile the test inventory and update current baseline documentation. Keep historic 1.50.x validation clearly labeled as historic; do not relabel it as 1.54.0 evidence.
3. Run `make check`, `make astro-build`, `make test-parity`, Rust process smoke, Java 1.54.0 smoke and soak, and Loom. Record each failure as a compatibility gap. Add Java Archive connection/record/replay evidence because the existing interop harness exercises client/driver paths, not Archive interoperability.
4. Implement modern Archive connect/schema handling and the highest-priority driver/concurrency regressions in small slices. Update matching tutorial stubs and chapter documentation as required by `AGENTS.md`.
5. Resume claim/controlled-poll/exclusive-publication system-test depth against the new reference. Keep persistent subscriptions and full cluster runtime as explicit scopes rather than silently counting them as parity.

The source assessment is complete. Reference initialization, exact-pin migration, runtime fixes, and 1.54.0 acceptance gates remain the next implementation work.

## Astro migration verification checkpoint

The assessment is served at `/aeron-1.54.0-baseline-assessment` and included in the Architecture navigation. Its original assessment prose is retained above; the current migration updates only the retired docs build command and adds this verification note.

- Branch: `feature/astro-documentation`.
- `make check` — passed; Rust format, lint, build and tests, script tests, and tutorial workspace build passed. Existing tutorial-stub dead-code warnings remain.
- `make astro-check` — passed; Astro sync, TypeScript, and focused Markdown link rewrite test passed.
- `make astro-build` — passed; 25 static pages plus the search index were generated, including the parent-owned catch-up plan.
- `make fmt-check` — passed; Rust, Prettier including Astro/TypeScript/CSS, and Ruff checks passed.
- `make astro-e2e` — passed; 3 headless Chromium tests cover home-to-overview-to-chapter click-through, a chapter cross-link, assessment and catch-up plan navigation, search, theme labels/persistence/cycling/system changes, clipboard contents, Mermaid rendering, narrow viewport, failed asset responses, page errors, and no visible images.
- Screenshots are stored under workstation `.tmp/harus-aeron-rs-astro/`. They are generated browser artifacts and have not been manually reviewed in this handoff.
- Vite emits its large-chunk warning for Mermaid/Cynefin bundles; Mermaid is dynamically imported only on pages with Mermaid diagrams.
