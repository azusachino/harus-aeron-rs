"""Tests for scripts/upstream_tests.py.

Loaded by path (not package-imported) since scripts/ ships standalone,
PEP-723 self-executing tools rather than an installable package.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "upstream_tests.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("upstream_tests", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def ut(tmp_path, monkeypatch):
    """A fresh upstream_tests module rooted at a throwaway tmp_path."""
    module = _load_module()
    root = tmp_path
    upstream = root / "vendor" / "aeron"
    for mod in module.MODULES:
        (upstream / mod / "src" / "test").mkdir(parents=True)
    map_path = root / "test" / "upstream_map.jsonl"

    monkeypatch.setattr(module, "ROOT", root)
    monkeypatch.setattr(module, "UPSTREAM", upstream)
    monkeypatch.setattr(module, "MAP", map_path)
    return module


def _touch_test_file(upstream: Path, module: str, name: str) -> None:
    (upstream / module / "src" / "test" / name).write_text("// stub\n")


def test_init_adds_one_entry_per_upstream_test_file(ut, tmp_path):
    upstream = tmp_path / "vendor" / "aeron"
    _touch_test_file(upstream, "aeron-client", "FooTest.java")
    _touch_test_file(upstream, "aeron-driver", "BarTest.java")

    assert ut.cmd_init() == 0

    entries = ut.load_map()
    assert len(entries) == 2
    layers = {e["layer"] for e in entries.values()}
    assert layers == {"client", "driver"}
    assert all(e["status"] == "todo" for e in entries.values())


def test_init_is_additive_and_preserves_existing_status(ut, tmp_path):
    upstream = tmp_path / "vendor" / "aeron"
    _touch_test_file(upstream, "aeron-client", "FooTest.java")
    ut.cmd_init()

    entries = ut.load_map()
    key = next(iter(entries))
    entries[key]["status"] = "done"
    entries[key]["rust_equivalent"] = "crates/client/src/protocol.rs"
    ut.write_map(entries)

    _touch_test_file(upstream, "aeron-driver", "BarTest.java")
    ut.cmd_init()

    entries = ut.load_map()
    assert len(entries) == 2
    assert entries[key]["status"] == "done"
    assert entries[key]["rust_equivalent"] == "crates/client/src/protocol.rs"


def test_report_returns_error_when_map_missing(ut, capsys):
    assert ut.cmd_report() == 1
    assert "init" in capsys.readouterr().out


def test_report_counts_match_totals(ut, tmp_path, capsys):
    upstream = tmp_path / "vendor" / "aeron"
    _touch_test_file(upstream, "aeron-client", "FooTest.java")
    _touch_test_file(upstream, "aeron-client", "BazTest.java")
    _touch_test_file(upstream, "aeron-driver", "BarTest.java")
    ut.cmd_init()

    entries = ut.load_map()
    done_key = next(k for k, e in entries.items() if e["layer"] == "client")
    entries[done_key]["status"] = "done"
    ut.write_map(entries)

    assert ut.cmd_report() == 0
    out = capsys.readouterr().out
    assert "3" in out  # total entries
    assert "1/3 resolved" in out


def test_main_defaults_to_report(ut, tmp_path, monkeypatch, capsys):
    upstream = tmp_path / "vendor" / "aeron"
    _touch_test_file(upstream, "aeron-client", "FooTest.java")
    ut.cmd_init()

    monkeypatch.setattr(sys, "argv", ["upstream_tests.py"])
    assert ut.main() == 0
    assert "parity" in capsys.readouterr().out
