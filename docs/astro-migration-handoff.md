# Astro documentation migration handoff

## Checkpoint

- Working branch: `feature/astro-documentation`.
- Source checkout began at `87950250bd7215659f8641c1374a4e67a0151576`.
- Replace MkDocs with Astro while retaining authored Markdown, tutorial order, navigation, search, themes, and inline links.
- The owner-authorized Aeron 1.54.0 assessment is included at `docs/aeron-1.54.0-baseline-assessment.md`; its prose is retained except for the retired build command, followed by a migration verification note.

## Implementation

- Astro 7.3.5 and TypeScript 7.0.2 build the Markdown collection to `dist/`. `.mise.toml` selects Bun's 1.4 minor line; the Bun lockfile pins JS dependencies.
- The Make targets are `astro-setup`, `astro-check`, `astro-dev`, `astro-build`, and `astro-e2e`. The package scripts use the matching `astro:*` names.
- `make fmt` / `make fmt-check` run pinned Prettier on JSON, YAML, TypeScript, CSS, MJS, and Astro sources. Astro formatting uses `prettier-plugin-astro` 1.1.2.
- Relative Markdown links are rewritten at build time; query strings and fragments are preserved and external links remain unchanged. The Tutorial alias redirects to its overview page.
- Markdown under `docs/plans/` is automatically loaded as pages and search content, then listed under the Roadmap sidebar group. The current owner-authored catch-up plan was built at `/plans/2026-11-04-aeron-1.54-catch-up/` and clicked in Playwright; its source remains parent-owned and unstaged.
- The docs UI provides navigation, outline, full-text search, Mermaid diagrams, code copy controls, and persisted system/light/dark theme selection. No character artwork is included.

## Verification

- `make check` — passed: Rust format, lint, workspace build/tests, Python tests, and tutorial workspace build. Existing tutorial-stub dead-code warnings remain.
- `make astro-check` — passed: Astro sync, TypeScript check, and the Markdown link transform test.
- `make astro-build` — passed: generated 25 static pages plus the search index, including the parent-owned catch-up plan.
- `make fmt-check` — passed: Rust, Prettier including Astro/TypeScript/CSS, and Ruff checks.
- `make astro-e2e` — passed: 3 headless Chromium tests cover home-to-overview-to-chapter clicks, a chapter cross-link, assessment and catch-up plan navigation, search, saved theme labels/cycling/system preference changes, clipboard contents, Mermaid rendering, a 390px viewport, failed asset responses, page errors, and no visible images.
- Playwright screenshots are written to workstation `.tmp/harus-aeron-rs-astro/`; no manual screenshot review is claimed here.
- Vite reports large Mermaid/Cynefin chunks (>500 kB), although Mermaid is dynamically loaded only on pages containing diagrams.

## Branch and ownership

- Final acceptance commit: `47e6680` (`docs: complete Astro migration acceptance`) on `feature/astro-documentation`. Earlier history includes `9eea92d`, `f077e77`, `b36667b`, and `cdeb705`.
- The parent-owned `docs/plans/2026-11-04-aeron-1.54-catch-up.md` was build and browser-test input; it remains untracked and was excluded from this commit.
- No remote actions were taken during the worker migration. The assessment is included by owner authorization. The parent subsequently committed the catch-up plan and opened PR #45.

## CI formatting follow-up

PR #45's first CI run failed Prettier on nine Astro/TypeScript files. Local runs had inherited the workstation parent's `.prettierrc` (`printWidth: 300`), while the standalone GitHub checkout used Prettier defaults. Running the nine files with an explicit empty configuration reproduced the same failures. The repository now owns `prettier.config.ts` and uses Prettier's default wrapping preference without a custom `printWidth` or hard line-length rule. Formatting runs explicitly on Bun so loading the TypeScript configuration does not depend on an ambient Node version. The Astro configuration is also TypeScript (`astro.config.ts`). A nested clone and a standalone checkout now resolve the same local configuration. Keep formatting configuration local to this repository; parent workstation files are not CI inputs.

## Toolchain and reference follow-up

The owner expanded PR #45 to replace the Nix setup with mise. Make targets now run tools through `mise exec`; `make setup` installs the tools and locked Bun/uv dependencies. Rust stays on `stable`; Bun (`1.4`), uv (`0.13`), and the interop JDK (`temurin-21.0`) use minor selectors. uv continues to manage Python from `.python-version`. The retired flake and CI Nix bypass are removed. CI uses mise-action v5 at its recorded commit, and the Astro/formatting CLIs run on Bun explicitly.

The owner also selected Aeron 1.54.0. The setup target initializes the recorded gitlink instead of advancing the upstream branch. Existing 1.50.x coverage records remain historical; the new reference does not complete the inventory or runtime work tracked in #44. The follow-up passed `make setup`, `make check astro-build astro-e2e interop-rust-smoke`, `make test-parity`, and `mise exec -- env RUSTFLAGS="--cfg loom" cargo test -p agrona --lib` (eight tests). All three Chromium tests passed. `make interop-smoke interop-soak` passed using `aeron-all-1.54.0.jar`, including 1,000 ordered UDP messages. Rerunning `make setup-upstream-aeron` preserved commit `454da1e4e6c7edf3dd6d34878512d3243384992a`. Resolved local tools were Rust 1.99.1 stable, Bun 1.4.2, uv 0.13.6, and Temurin 21.0.13 on macOS arm64. A final Astro build covers the documentation updates made while interop ran. Historical ledger counts remain 175 bounded equivalents and 93 skips; they are not 1.54 behavioral-parity evidence.
