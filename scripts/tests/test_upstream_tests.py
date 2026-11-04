"""Tests for scripts/upstream_tests.py."""

from __future__ import annotations

import importlib.util
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
    monkeypatch.setattr(ut, "inventory", lambda revision: {})
    return tmp_path, upstream


def file(workspace, path, data="class Test {}"):
    root, _ = workspace
    target = root / "vendor/aeron" / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(data)
    return target


def test_init_reconciles_added_changed_removed_and_moved(workspace, monkeypatch, capsys):
    root, _ = workspace
    old = "old"
    new = "new"
    a = "vendor/aeron/aeron-client/src/test/A.java"
    b = "vendor/aeron/aeron-client/src/test/B.java"
    gone = "vendor/aeron/aeron-driver/src/test/Gone.java"
    moved_from = "vendor/aeron/aeron-system-tests/src/test/Resolver.java"
    moved_to = "vendor/aeron/aeron-driver/src/test/Resolver.java"
    (root / "vendor/aeron/aeron-client/src/test").mkdir(parents=True, exist_ok=True)
    (root / "vendor/aeron/aeron-driver/src/test").mkdir(parents=True, exist_ok=True)
    old_blobs = {a: "same", b: "before", gone: "gone", moved_from: "resolver"}
    new_blobs = {
        a: "same",
        b: "after",
        moved_to: "resolver",
        "vendor/aeron/aeron-archive/src/test/New.java": "new",
    }
    monkeypatch.setattr(
        ut, "git_blobs", lambda revision: old_blobs if revision == old else new_blobs
    )
    monkeypatch.setattr(
        ut, "inventory", lambda revision: old_blobs if revision == old else new_blobs
    )
    prior = {
        k: {
            "upstream": k,
            "layer": "client",
            "status": "done",
            "rust_equivalent": "evidence",
            "source_revision": old,
        }
        for k in old_blobs
    }
    ut.write_map(prior)
    ut.cmd_init(new)
    entries = ut.load_map()
    assert len(entries) == 6
    assert entries[a]["status"] == "done"
    assert entries[b]["previous_blob"] == "before"
    assert entries[b]["audit_disposition"] == "pending-source-review"
    assert entries[moved_from]["inventory_state"] == "absent-at-current-reference"
    assert moved_to in entries
    assert "2 added, 1 changed" in capsys.readouterr().out


def test_report_does_not_count_skip_as_implemented(workspace, monkeypatch, capsys):
    root, _ = workspace
    key = "vendor/aeron/aeron-client/src/test/A.java"
    entries = {
        key: {"upstream": key, "layer": "client", "status": "skip", "source_revision": "rev"}
    }
    ut.write_map(entries)
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "blob"})
    assert ut.cmd_report("rev") == 0
    output = capsys.readouterr().out
    assert "0/1" in output
    assert "skip=1" in output


def test_report_rejects_inventory_mismatch(workspace, monkeypatch, capsys):
    key = "vendor/aeron/aeron-client/src/test/A.java"
    ut.write_map({
        key: {"upstream": key, "layer": "client", "status": "todo", "source_revision": "rev"}
    })
    monkeypatch.setattr(ut, "inventory", lambda revision: {key: "blob", key + "2": "blob2"})
    assert ut.cmd_report("rev") == 1
    assert "inventory mismatch" in capsys.readouterr().err
