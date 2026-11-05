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
OLD_REVISION = "8e0cdb2f92800a047c0066028444b790a80fe593"


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
    prefixes = tuple(f"{module}/src/test/" for module in MODULES)
    blobs = {}
    for line in result.stdout.splitlines():
        metadata, path = line.split("\t", 1)
        if path.endswith(".java") and path.startswith(prefixes):
            blobs[f"vendor/aeron/{path}"] = metadata.split()[2]
    return blobs


def inventory(revision: str) -> dict[str, str]:
    return git_blobs(revision)


def cmd_init(revision: str = DEFAULT_REVISION) -> int:
    old = load_map()
    try:
        target_blobs = inventory(revision)
        old_revision = next(
            (e["source_revision"] for e in old.values() if e.get("source_revision")), OLD_REVISION
        )
        old_blobs = git_blobs(old_revision)
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"cannot read required upstream revision: {exc}", file=sys.stderr)
        return 1
    entries = {}
    added = changed = retained = 0
    for key, blob in target_blobs.items():
        if key in old:
            entry = old[key]
            entry.pop("inventory_state", None)
            prior_blob = old_blobs.get(key)
            if prior_blob != blob:
                changed += 1
                entry["audit_disposition"] = "pending-source-review"
                entry["previous_blob"] = prior_blob
                entry.pop("audit_evidence", None)
            else:
                retained += 1
                entry.pop("previous_blob", None)
                if not entry.get("audit_disposition"):
                    entry["audit_disposition"] = "unchanged-source"
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
        (key, entry.get("status"))
        for key, entry in entries.items()
        if entry.get("status") not in STATUSES
    ]
    if invalid:
        print(f"unknown parity status: {invalid}", file=sys.stderr)
        return 1
    try:
        current = inventory(revision)
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"cannot read upstream revision {revision}: {exc}", file=sys.stderr)
        return 1
    mapped = {
        key: entry
        for key, entry in entries.items()
        if key in current and entry.get("source_revision") == revision
    }
    if set(mapped) != set(current):
        print(
            f"inventory mismatch: upstream={len(current)} ledger-current={len(mapped)}; run init",
            file=sys.stderr,
        )
        return 1
    stale = [
        (key, e.get("source_blob"), current[key])
        for key, e in mapped.items()
        if e.get("source_blob") != current[key]
    ]
    if stale:
        print("stale source_blob metadata; run init:", file=sys.stderr)
        for key, stored, actual in stale:
            print(f"  {key}: ledger={stored!r} upstream={actual!r}", file=sys.stderr)
        return 1
    counts = Counter(e["status"] for e in mapped.values())
    reviewed_done = sum(
        e["status"] == "done"
        and e.get("audit_disposition")
        not in ("pending-source-review", "behavior-unverified", "unsupported-runtime-behavior")
        for e in mapped.values()
    )
    historical_done = counts["done"] - reviewed_done
    unverified = sum(
        e.get("audit_disposition")
        in ("pending-source-review", "behavior-unverified", "unsupported-runtime-behavior")
        for e in mapped.values()
    )
    total = len(current)
    print(f"Aeron test-file dispositions at {revision}: {total} current paths")
    print("status: " + ", ".join(f"{s}={counts[s]}" for s in STATUSES))
    pct = reviewed_done / total * 100 if total else 0.0
    print(f"reviewed current-source equivalents: {reviewed_done}/{total} ({pct:.1f}%)")
    print(f"historical done mappings without current-source evidence: {historical_done}")
    print(f"unverified changed/new sources: {unverified}")
    absent = [
        key
        for key, entry in entries.items()
        if entry.get("inventory_state") == "absent-at-current-reference"
    ]
    if absent:
        print("historical paths absent at current reference:")
        for key in absent:
            print(f"  {key}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "report"), default="report", nargs="?")
    parser.add_argument("--revision", default=DEFAULT_REVISION)
    args = parser.parse_args()
    return cmd_init(args.revision) if args.action == "init" else cmd_report(args.revision)


if __name__ == "__main__":
    raise SystemExit(main())
