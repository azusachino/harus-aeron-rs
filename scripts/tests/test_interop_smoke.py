"""Interop must identify the selected source and built Java artifact before probing it."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
PROBE = SCRIPTS.parent / "deploy" / "interop" / "AeronArtifactProbe.java"
SOURCE_SHA = "454da1e4e6c7edf3dd6d34878512d3243384992a"


@pytest.fixture
def interop(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("interop_smoke", SCRIPTS / "interop_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    upstream = tmp_path / "vendor" / "aeron"
    upstream.mkdir(parents=True)
    (upstream / "version.txt").write_text("1.54.0\n")
    build = upstream / "aeron-all" / "build" / "libs"
    build.mkdir(parents=True)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "JAVA_BUILD", build)
    return module


def fake_commands(interop, monkeypatch, *, checkout=SOURCE_SHA, produce_jar=True):
    calls = []

    def run(command, *, cwd=None):
        calls.append((command, cwd))
        if command[0] == "git":
            return SOURCE_SHA if cwd == interop.ROOT else checkout
        if command[0] == "./gradlew" and produce_jar:
            (interop.JAVA_BUILD / "aeron-all-1.54.0.jar").touch()
        return "artifact identity verified\n"

    monkeypatch.setattr(interop, "_run", run)
    return calls


def test_selects_source_version_instead_of_an_unrelated_cached_jar(interop, monkeypatch):
    (interop.JAVA_BUILD / "aeron-all-9.0.0.jar").touch()
    calls = fake_commands(interop, monkeypatch)

    artifact = interop._aeron_all_jar()

    assert artifact.name == "aeron-all-1.54.0.jar"
    assert any(command[0] == "./gradlew" for command, _ in calls)
    probe = next(command for command, _ in calls if command[0] == "java")
    assert str(artifact) in probe
    assert probe[-2:] == ["1.54.0", SOURCE_SHA]


def test_refreshes_gradle_outputs_even_when_the_expected_jar_already_exists(interop, monkeypatch):
    (interop.JAVA_BUILD / "aeron-all-1.54.0.jar").touch()
    calls = fake_commands(interop, monkeypatch)

    interop._aeron_all_jar()

    assert any(command[0] == "./gradlew" for command, _ in calls)


def test_rejects_a_checkout_different_from_the_recorded_gitlink(interop, monkeypatch):
    calls = fake_commands(interop, monkeypatch, checkout="b" * 40)

    with pytest.raises(RuntimeError, match="gitlink"):
        interop._aeron_all_jar()

    assert all(command[0] != "./gradlew" for command, _ in calls)


def test_missing_expected_artifact_does_not_fall_back_to_another_version(interop, monkeypatch):
    (interop.JAVA_BUILD / "aeron-all-1.50.4.jar").touch()
    fake_commands(interop, monkeypatch, produce_jar=False)

    with pytest.raises(RuntimeError, match=r"aeron-all-1\.54\.0\.jar"):
        interop._aeron_all_jar()


@pytest.mark.parametrize(
    ("version", "artifact_sha", "valid"),
    [
        ("1.54.0", SOURCE_SHA[:10], True),
        ("1.54.0", "b" * 10, False),
        ("1.50.4", SOURCE_SHA[:10], False),
        ("1.54.0", "", False),
    ],
)
def test_java_probe_checks_embedded_identity_not_just_the_filename(
    tmp_path, version, artifact_sha, valid
):
    source = tmp_path / "AeronVersion.java"
    source.write_text(
        "package io.aeron;\n"
        "public final class AeronVersion {\n"
        f"  public static final String VERSION = {json.dumps(version)};\n"
        f"  public static final String GIT_SHA = {json.dumps(artifact_sha)};\n"
        "}\n"
    )
    classes = tmp_path / "classes"
    subprocess.run(
        ["javac", "-d", str(classes), str(source)], check=True, capture_output=True, text=True
    )
    artifact = tmp_path / "aeron-all-1.54.0.jar"
    with zipfile.ZipFile(artifact, "w") as jar:
        jar.write(classes / "io" / "aeron" / "AeronVersion.class", "io/aeron/AeronVersion.class")

    result = subprocess.run(
        ["java", "-cp", str(artifact), str(PROBE), "1.54.0", SOURCE_SHA],
        capture_output=True,
        text=True,
        check=False,
    )

    assert (result.returncode == 0) == valid, result.stderr
    if valid:
        assert "Aeron artifact 1.54.0" in result.stdout
        assert SOURCE_SHA in result.stdout
    else:
        assert "artifact identity mismatch" in result.stderr
