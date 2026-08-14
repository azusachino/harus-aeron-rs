"""MkDocs hook: auto-generate nav subtrees that would otherwise drift.

Sections rebuilt on every build:

- **Tutorial** — every `docs/tutorial/NN-*.md` (`README.md` excluded, pinned first
  as "Overview") is bucketed into a nav group by its two-digit filename prefix via
  `CHAPTER_GROUPS` below, in filename order within each group. The nav title is the
  file's own `# Chapter N: ...` heading with that prefix stripped. Adding chapter 12
  means classifying "12" in `CHAPTER_GROUPS` — the build fails loudly if a chapter
  file exists with no entry there, instead of silently leaving it out of nav (which
  is exactly how chapters 10-11 went stale before this hook existed).
- **Specifications** — every `docs/specs/*.md`, filename order, title from its first
  `# ` heading. Drop a new spec file in; no `mkdocs.yml` edit needed.
"""

from __future__ import annotations

import glob
import os
import re

# tutorial chapter filename prefix -> nav group title
CHAPTER_GROUPS: dict[str, str] = {
    "01": "Foundations",
    "02": "Foundations",
    "03": "Foundations",
    "04": "Foundations",
    "05": "Media Driver",
    "06": "Media Driver",
    "07": "Media Driver",
    "08": "Media Driver",
    "09": "Integration",
    "10": "Archive",
    "11": "Cluster",
}
GROUP_ORDER = ["Foundations", "Media Driver", "Integration", "Archive", "Cluster"]
CHAPTER_HEADING_RE = re.compile(r"^Chapter\s+\d+:\s*")

TUTORIAL_TITLE = "Tutorial"
TUTORIAL_SUBDIR = "tutorial"
SPECS_TITLE = "Specifications"
SPECS_SUBDIR = "specs"


def _first_heading(path: str) -> str:
    """Return the text of a markdown file's first top-level `# ` heading."""
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("# "):
                return line[2:].strip()
    raise ValueError(f"{path}: no top-level '# ' heading to derive a nav title from")


def _build_tutorial(docs_dir: str) -> list:
    tutorial_dir = os.path.join(docs_dir, TUTORIAL_SUBDIR)
    groups: dict[str, list] = {name: [] for name in GROUP_ORDER}

    for path in sorted(glob.glob(os.path.join(tutorial_dir, "*.md"))):
        filename = os.path.basename(path)
        if filename == "README.md":
            continue
        prefix = filename[:2]
        group = CHAPTER_GROUPS.get(prefix)
        if group is None:
            raise ValueError(
                f"docs/tutorial/{filename}: no CHAPTER_GROUPS entry for prefix "
                f"'{prefix}' in hooks/autonav.py — classify this chapter's nav group"
            )
        rel = os.path.relpath(path, docs_dir).replace(os.sep, "/")
        title = CHAPTER_HEADING_RE.sub("", _first_heading(path))
        groups[group].append({title: rel})

    children = [{"Overview": f"{TUTORIAL_SUBDIR}/README.md"}]
    children.extend({name: groups[name]} for name in GROUP_ORDER if groups[name])
    return children


def _build_specs(docs_dir: str) -> list:
    specs_dir = os.path.join(docs_dir, SPECS_SUBDIR)
    entries = []
    for path in sorted(glob.glob(os.path.join(specs_dir, "*.md"))):
        rel = os.path.relpath(path, docs_dir).replace(os.sep, "/")
        entries.append({_first_heading(path): rel})
    return entries


def _rewrite(nav: list, docs_dir: str) -> None:
    """Recursively replace the children of every managed section in-place."""
    for item in nav:
        if not isinstance(item, dict):
            continue
        for key, val in list(item.items()):
            if key == TUTORIAL_TITLE:
                item[key] = _build_tutorial(docs_dir)
            elif key == SPECS_TITLE:
                item[key] = _build_specs(docs_dir)
            elif isinstance(val, list):
                _rewrite(val, docs_dir)


def on_config(config, **_kwargs):
    nav = config.get("nav")
    if nav:
        _rewrite(nav, config["docs_dir"])
    return config
