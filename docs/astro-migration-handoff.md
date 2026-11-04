# Astro documentation migration handoff

## Checkpoint

- Working branch: `feature/astro-documentation`.
- Source checkout began at `87950250bd7215659f8641c1374a4e67a0151576`.
- Replace MkDocs with Astro while retaining authored Markdown, tutorial order, navigation, search, themes, and inline links.
- The owner-authorized Aeron 1.54.0 assessment is included at `docs/aeron-1.54.0-baseline-assessment.md`; its prose is retained except for the retired build command, followed by a migration verification note.

## Implementation

- Astro 7.3.5 and TypeScript 7.0.2 build the Markdown collection to `dist/`. Bun 1.4.2 is pinned in `.mise.toml`; Bun lockfile pins JS dependencies.
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

- Prior migration commits: `9eea92d`, `f077e77`, `b36667b`, and `cdeb705`; the final acceptance commit will be recorded after this checkpoint is committed.
- No remote actions were taken. The assessment is included by owner authorization. The parent owns the separate `docs/plans/2026-11-04-aeron-1.54-catch-up.md` path; avoid modifying or staging it.
