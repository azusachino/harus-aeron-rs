"""Tests for scripts/upstream_tests.py."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "upstream_tests.py"
spec = importlib.util.spec_from_file_location("upstream_tests", SCRIPT_PATH)
assert spec and spec.loader
ut = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ut)


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    upstream = tmp_path / "vendor/aeron"
    for module in ut.MODULES:
        (upstream / module / "src/test").mkdir(parents=True)
    monkeypatch.setattr(ut, "ROOT", tmp_path)
    monkeypatch.setattr(ut, "UPSTREAM", upstream)
    monkeypatch.setattr(ut, "MAP", tmp_path / "test/upstream_map.jsonl")
    return tmp_path


def test_init_reconciles_add_change_remove_reintroduction_and_is_idempotent(
    workspace, monkeypatch, capsys
):
    old, new, reintroduced = "old", "new", "reintroduced"
    same = "vendor/aeron/aeron-client/src/test/Same.java"
    changed = "vendor/aeron/aeron-client/src/test/Changed.java"
    removed = "vendor/aeron/aeron-driver/src/test/Removed.java"
    moved_old = "vendor/aeron/aeron-system-tests/src/test/Resolver.java"
    moved_new = "vendor/aeron/aeron-driver/src/test/Resolver.java"
    old_tree = {same: "s", changed: "before", removed: "r", moved_old: "m"}
    new_tree = {
        same: "s",
        changed: "after",
        moved_new: "m",
        "vendor/aeron/aeron-archive/src/test/Added.java": "a",
    }
    monkeypatch.setattr(ut, "inventory", lambda rev: old_tree if rev == old else new_tree)
    monkeypatch.setattr(ut, "git_blobs", lambda rev: old_tree if rev == old else new_tree)
    prior = {
        key: {
            "upstream": key,
            "layer": "client",
            "status": "done",
            "rust_equivalent": "evidence",
            "source_revision": old,
            "audit_disposition": "reviewed",
        }
        for key in old_tree
    }
    ut.write_map(prior)

    assert ut.cmd_init(new) == 0
    entries = ut.load_map()
    assert len(entries) == 6
    assert entries[same]["audit_disposition"] == "reviewed"
    assert entries[changed]["audit_disposition"] == "pending-source-review"
    assert entries[changed]["previous_blob"] == "before"
    assert entries[removed]["inventory_state"] == "absent-at-current-reference"
    assert moved_old in entries and moved_new in entries
    assert entries[moved_new]["audit_disposition"] == "pending-source-review"

    # Same revision and identical blobs do not reset review or report new changes.
    assert ut.cmd_init(new) == 0
    assert ut.load_map()[changed]["audit_disposition"] == "pending-source-review"
    assert "0 added, 0 changed" in capsys.readouterr().out

    # A row that reappears is active again and its historical absent marker clears.
    tree = {**new_tree, removed: "r"}
    monkeypatch.setattr(ut, "inventory", lambda rev: tree)
    monkeypatch.setattr(ut, "git_blobs", lambda rev: tree)
    assert ut.cmd_init(reintroduced) == 0
    assert "inventory_state" not in ut.load_map()[removed]


def test_report_validates_blob_and_separates_historical_done(workspace, monkeypatch, capsys):
    key = "vendor/aeron/aeron-client/src/test/A.java"
    ut.write_map({
        key: {
            "upstream": key,
            "layer": "client",
            "status": "done",
            "source_revision": "rev",
            "source_blob": "actual",
            "audit_disposition": "behavior-unverified",
        }
    })
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "actual"})
    assert ut.cmd_report("rev") == 0
    output = capsys.readouterr().out
    assert "reviewed current-source equivalents: 0/1" in output
    assert "historical done mappings without current-source evidence: 1" in output

    entries = ut.load_map()
    entries[key]["source_blob"] = "forged"
    ut.write_map(entries)
    assert ut.cmd_report("rev") == 1
    assert "stale source_blob metadata" in capsys.readouterr().err


def test_report_counts_skip_as_not_implemented(workspace, monkeypatch, capsys):
    key = "vendor/aeron/aeron-client/src/test/A.java"
    ut.write_map({
        key: {
            "upstream": key,
            "layer": "client",
            "status": "skip",
            "source_revision": "rev",
            "source_blob": "b",
        }
    })
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "b"})
    assert ut.cmd_report("rev") == 0
    assert "reviewed current-source equivalents: 0/1" in capsys.readouterr().out


def test_report_rejects_unknown_status(workspace, monkeypatch, capsys):
    key = "vendor/aeron/aeron-client/src/test/A.java"
    ut.write_map({
        key: {
            "upstream": key,
            "layer": "client",
            "status": "implemented",
            "source_revision": "rev",
            "source_blob": "b",
        }
    })
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "b"})
    assert ut.cmd_report("rev") == 1
    assert "unknown parity status" in capsys.readouterr().err


def test_report_rejects_missing_upstream_revision_and_inventory_mismatch(
    workspace, monkeypatch, capsys
):
    key = "vendor/aeron/aeron-client/src/test/A.java"
    ut.write_map({
        key: {
            "upstream": key,
            "layer": "client",
            "status": "todo",
            "source_revision": "rev",
            "source_blob": "b",
        }
    })
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "b", key + "2": "b2"})
    assert ut.cmd_report("rev") == 1
    assert "inventory mismatch" in capsys.readouterr().err
    monkeypatch.setattr(
        ut,
        "inventory",
        lambda revision: (_ for _ in ()).throw(subprocess.CalledProcessError(128, "git")),
    )
    assert ut.cmd_report("missing") == 1
    assert "cannot read upstream revision missing" in capsys.readouterr().err


def test_init_fails_clearly_for_missing_revision(workspace, monkeypatch, capsys):
    monkeypatch.setattr(
        ut,
        "inventory",
        lambda revision: (_ for _ in ()).throw(subprocess.CalledProcessError(128, "git")),
    )
    assert ut.cmd_init("missing") == 1
    assert "cannot read required upstream revision" in capsys.readouterr().err


def test_default_report_and_empty_map_error(workspace, monkeypatch, capsys):
    key = "vendor/aeron/aeron-client/src/test/A.java"
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "b"})
    ut.write_map({
        key: {
            "upstream": key,
            "layer": "client",
            "status": "todo",
            "source_revision": ut.DEFAULT_REVISION,
            "source_blob": "b",
        }
    })
    monkeypatch.setattr(sys, "argv", ["upstream_tests.py"])
    assert ut.main() == 0
    assert ut.DEFAULT_REVISION in capsys.readouterr().out
    ut.write_map({})
    assert ut.cmd_report() == 1
    assert "run init" in capsys.readouterr().err
