# MkDocs to Astro migration handoff

## Checkpoint

- Started from `main` at `87950250bd7215659f8641c1374a4e67a0151576`.
- Working branch: `feature/astro-documentation`.
- Outcome: replace MkDocs Material docs with Astro, preserve Markdown docs and tutorial routes, retain the workstation light/dark/system palette and docs navigation/search behavior, and omit character artwork.
- The prior uncommitted Architecture navigation item for `docs/aeron-1.54.0-baseline-assessment.md` is represented in the Astro navigation. That Markdown file remains untracked and owner-owned; it is intentionally excluded from migration commits.

## Implementation

- Astro builds the authored files in `docs/` to `dist/`; `make docs-serve`, `make docs-build`, and `make docs-e2e` are the docs entry points.
- The local UI includes section navigation, heading outline, code copy controls, Mermaid rendering, full-text search over a generated local index, and a persisted light/dark/system theme selector.
- `.md` links in existing content map to Astro routes. `/tutorial/` redirects to `/tutorial/README` as before.
- Existing source uses standard Markdown fences and one Mermaid fence. No MkDocs admonition or tab syntax was found in `docs/`; all Markdown files remain unchanged as source content.
- Search/UI uses locally bundled assets only. No visible images are rendered.

## Verification

- `make check` — passed; Rust format, lint, workspace tests, Python tests, and tutorial workspace build passed. Existing tutorial stub dead-code warnings remain.
- `make docs-build` — passed; Astro check reported 0 errors/warnings/hints and generated 23 pages plus the search index. Vite reports large Mermaid/Cynefin chunks (>500 kB), although Mermaid is dynamically loaded only on pages containing Mermaid diagrams.
- `make fmt-check` — passed; Rust, JSON/YAML, ruff format, and ruff lint checks passed.
- `make docs-e2e` — passed in headless Chromium. Checks home, tutorial chapter/index, assessment, full-text search, manual theme persistence, system dark mode, Mermaid rendering, a 390px viewport, no visible images, failed asset responses, and page errors.
- Screenshots: workstation `.tmp/harus-aeron-rs-astro/home.png` and `assessment.png`.

## Ownership, branch and next step

- Migration-owned source is committed on `feature/astro-documentation`; the commit is recorded in the Git log after creation.
- `docs/aeron-1.54.0-baseline-assessment.md` remains untracked and is not part of this migration commit. Keep it with its owner; it is required by the visible Architecture navigation entry.
- No remote actions were taken. Next step: review the branch and coordinate the separate assessment file with its owner before merging.
