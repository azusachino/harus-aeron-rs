#!/usr/bin/env -S uv run --script
"""Report Rust test-file parity against the vendored upstream Aeron test suite.

Two subcommands:
  init    — (re)generate test/upstream_map.jsonl by scanning vendor/aeron for test
            files not yet present in the map. Existing entries are left untouched
            (their status is preserved), so this is safe to re-run after `git
            submodule update --remote vendor/aeron` picks up new upstream tests.
  report  — print parity counts per module/layer, matching docs/plan.md's table.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "vendor" / "aeron"
MAP = ROOT / "test" / "upstream_map.jsonl"

# module -> Rust layer (crate) this module's tests should be ported into.
MODULES = {
    "aeron-client": "client",
    "aeron-driver": "driver",
    "aeron-archive": "archive",
    "aeron-cluster": "cluster",
    "aeron-system-tests": "system-tests",
}

STATUSES = ("todo", "in-progress", "done", "skip")


def upstream_tests() -> dict[str, list[Path]]:
    return {
        module: sorted((UPSTREAM / module / "src" / "test").rglob("*.java")) for module in MODULES
    }


def load_map() -> dict[str, dict]:
    if not MAP.exists():
        return {}
    entries: dict[str, dict] = {}
    for line in MAP.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        entries[entry["upstream"]] = entry
    return entries


def write_map(entries: dict[str, dict]) -> None:
    MAP.parent.mkdir(parents=True, exist_ok=True)
    with MAP.open("w") as f:
        for key in sorted(entries):
            f.write(json.dumps(entries[key], sort_keys=True) + "\n")


def cmd_init() -> int:
    existing = load_map()
    tests = upstream_tests()
    added = 0
    for module, layer in MODULES.items():
        for path in tests[module]:
            rel = str(path.relative_to(ROOT))
            if rel not in existing:
                existing[rel] = {
                    "upstream": rel,
                    "layer": layer,
                    "status": "todo",
                    "rust_equivalent": None,
                }
                added += 1
    write_map(existing)
    print(f"wrote {len(existing)} entries to {MAP.relative_to(ROOT)} ({added} new)")
    return 0


def cmd_report() -> int:
    entries = load_map()
    if not entries:
        rel = MAP.relative_to(ROOT)
        print(f"{rel} is empty or missing — run `scripts/upstream_tests.py init` first")
        return 1

    by_layer: dict[str, Counter[str]] = {}
    for entry in entries.values():
        by_layer.setdefault(entry["layer"], Counter())[entry["status"]] += 1

    print("Aeron upstream test-file parity (Rust)")
    header = f"{'layer':14}" + "".join(f"{s:>13}" for s in STATUSES) + f"{'total':>8}"
    print(header)
    print("-" * len(header))
    grand_total = Counter()
    for layer in MODULES.values():
        counts = by_layer.get(layer, Counter())
        total = sum(counts.values())
        row = f"{layer:14}" + "".join(f"{counts[s]:>13}" for s in STATUSES) + f"{total:>8}"
        print(row)
        grand_total.update(counts)
    total_all = sum(grand_total.values())
    print("-" * len(header))
    footer = (
        f"{'total':14}" + "".join(f"{grand_total[s]:>13}" for s in STATUSES) + f"{total_all:>8}"
    )
    print(footer)

    done = grand_total["done"] + grand_total["skip"]
    pct = (done / total_all * 100) if total_all else 0.0
    print(f"\n{done}/{total_all} resolved ({pct:.1f}%) — 0.1.0 release gate is near-100%")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "report"), default="report", nargs="?")
    args = parser.parse_args()
    if args.action == "init":
        return cmd_init()
    return cmd_report()


if __name__ == "__main__":
    raise SystemExit(main())
