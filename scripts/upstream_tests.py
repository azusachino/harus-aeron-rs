#!/usr/bin/env -S uv run --script
"""Reconcile Rust test dispositions against a pinned upstream Aeron inventory."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "vendor" / "aeron"
MAP = ROOT / "test" / "upstream_map.jsonl"
MODULES = {
    "aeron-client": "client",
    "aeron-driver": "driver",
    "aeron-archive": "archive",
    "aeron-cluster": "cluster",
    "aeron-system-tests": "system-tests",
}
STATUSES = ("todo", "in-progress", "done", "skip")
DEFAULT_REVISION = "454da1e4e6c7edf3dd6d34878512d3243384992a"


def upstream_tests() -> dict[str, list[Path]]:
    return {
        module: sorted((UPSTREAM / module / "src" / "test").rglob("*.java")) for module in MODULES
    }


def load_map() -> dict[str, dict]:
    if not MAP.exists():
        return {}
    entries = {}
    for line in MAP.read_text().splitlines():
        if line.strip():
            entry = json.loads(line)
            entries[entry["upstream"]] = entry
    return entries


def write_map(entries: dict[str, dict]) -> None:
    MAP.parent.mkdir(parents=True, exist_ok=True)
    with MAP.open("w") as stream:
        for key in sorted(entries):
            stream.write(json.dumps(entries[key], sort_keys=True) + "\n")


def git_blobs(revision: str) -> dict[str, str]:
    result = subprocess.run(
        ["git", "-C", str(UPSTREAM), "ls-tree", "-r", revision],
        check=True,
        capture_output=True,
        text=True,
    )
    prefix = tuple(f"{module}/src/test/" for module in MODULES)
    blobs = {}
    for line in result.stdout.splitlines():
        metadata, path = line.split("\t", 1)
        if path.endswith(".java") and path.startswith(prefix):
            blobs["vendor/aeron/" + path] = metadata.split()[2]
    return blobs


def inventory(revision: str) -> dict[str, str]:
    return git_blobs(revision)


def cmd_init(revision: str = DEFAULT_REVISION) -> int:
    old = load_map()
    target_blobs = inventory(revision)
    old_revision = next(
        (e.get("source_revision") for e in old.values() if e.get("source_revision")),
        "8e0cdb2f92800a047c0066028444b790a80fe593",
    )
    old_blobs = git_blobs(old_revision) if old_revision else {}
    entries = {}
    added = changed = retained = 0
    for key, blob in target_blobs.items():
        if key in old:
            entry = old[key]
            if old_blobs and old_blobs.get(key) != blob:
                changed += 1
                entry["audit_disposition"] = (
                    entry.get("audit_disposition") or "pending-source-review"
                )
                entry["previous_blob"] = old_blobs.get(key)
            else:
                retained += 1
        else:
            added += 1
            module = key.split("/")[2]
            entry = {
                "upstream": key,
                "layer": MODULES[module],
                "status": "todo",
                "rust_equivalent": None,
                "audit_disposition": "pending-source-review",
            }
        entry["source_revision"] = revision
        entry["source_blob"] = blob
        entries[key] = entry
    removed = sorted(set(old) - set(target_blobs))
    for key in removed:
        entry = old[key]
        entry["inventory_state"] = "absent-at-current-reference"
        entry["source_revision"] = revision
        entries[key] = entry
    write_map(entries)
    print(
        f"reference {revision}: {len(target_blobs)} current paths; "
        f"{added} added, {changed} changed, {retained} unchanged, "
        f"{len(removed)} absent (historical rows retained)"
    )
    return 0


def cmd_report(revision: str = DEFAULT_REVISION) -> int:
    entries = load_map()
    if not entries:
        print(f"{MAP.relative_to(ROOT)} is empty or missing — run init", file=sys.stderr)
        return 1
    invalid = [
        (key, e.get("status")) for key, e in entries.items() if e.get("status") not in STATUSES
    ]
    if invalid:
        print(f"unknown parity status: {invalid}", file=sys.stderr)
        return 1
    current = inventory(revision)
    mapped_current = {
        k: e for k, e in entries.items() if k in current and e.get("source_revision") == revision
    }
    if set(mapped_current) != set(current):
        print(
            f"inventory mismatch: upstream={len(current)} "
            f"ledger-current={len(mapped_current)}; run init",
            file=sys.stderr,
        )
        return 1
    print(f"Aeron test-file dispositions at {revision}: {len(current)} current paths")
    counts = Counter(e["status"] for e in mapped_current.values())
    audit_pending = sum(
        e.get("audit_disposition") == "pending-source-review" for e in mapped_current.values()
    )
    done = counts["done"]
    total = len(current)
    print("status: " + ", ".join(f"{s}={counts[s]}" for s in STATUSES))
    print(f"implemented/bounded equivalents: {done}/{total} ({done / total * 100:.1f}%)")
    print(f"unreviewed changed/new sources: {audit_pending}")
    absent = [
        k for k, e in entries.items() if e.get("inventory_state") == "absent-at-current-reference"
    ]
    if absent:
        print("historical paths absent at current reference:")
        for path in absent:
            print(f"  {path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "report"), default="report", nargs="?")
    parser.add_argument("--revision", default=DEFAULT_REVISION)
    args = parser.parse_args()
    return cmd_init(args.revision) if args.action == "init" else cmd_report(args.revision)


if __name__ == "__main__":
    raise SystemExit(main())
