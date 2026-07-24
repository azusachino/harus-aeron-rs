#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///

"""Run a Java-side compatibility probe against a Rust-generated CnC file."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DRIVER_BINARY = ROOT / "target" / "debug" / "aeron-driver"
JAVA_SOURCE = ROOT / "deploy" / "interop" / "CncLayoutProbe.java"
JAVA_BUILD = ROOT / "vendor" / "aeron" / "aeron-all" / "build" / "libs"


def _run(command: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(command, cwd=cwd, check=False, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _aeron_all_jar() -> Path:
    jars = sorted(JAVA_BUILD.glob("aeron-all-*.jar"))
    if not jars:
        _run(["./gradlew", ":aeron-all:shadowJar", "--no-daemon"], cwd=ROOT / "vendor" / "aeron")
        jars = sorted(JAVA_BUILD.glob("aeron-all-*.jar"))
    if not jars:
        raise RuntimeError("vendored Aeron aeron-all jar was not produced")
    return jars[-1]


def main() -> int:
    if shutil.which("java") is None or shutil.which("javac") is None:
        print("interop-smoke requires java and javac", file=sys.stderr)
        return 1

    print("[1/4] Building Rust MediaDriver binary...")
    _run(["cargo", "build", "-p", "aeron-driver", "--bin", "aeron-driver"], cwd=ROOT)

    print("[2/4] Resolving vendored Aeron Java jar...")
    aeron_all_jar = _aeron_all_jar()

    print("[3/4] Compiling Java CnC compatibility probe...")
    with tempfile.TemporaryDirectory(prefix="aeron-interop-") as temp_dir:
        classes = Path(temp_dir)
        _run(["javac", "-cp", str(aeron_all_jar), "-d", str(classes), str(JAVA_SOURCE)])

        print("[4/4] Mapping Rust cnc.dat with Aeron Java readers...")
        driver = subprocess.Popen(
            [str(DRIVER_BINARY), "--hold"],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            if driver.stdout is None:
                raise RuntimeError("Rust driver stdout was not available")
            cnc_path = driver.stdout.readline().strip()
            if not cnc_path:
                stderr = driver.stderr.read() if driver.stderr else ""
                raise RuntimeError(f"Rust driver did not publish a CnC path\n{stderr}")
            _run(
                [
                    "java",
                    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "-cp",
                    f"{classes}:{aeron_all_jar}",
                    "CncLayoutProbe",
                    cnc_path,
                ],
                cwd=ROOT,
            )
        finally:
            if driver.stdin is not None:
                driver.stdin.write("release\n")
                driver.stdin.close()
            try:
                driver.wait(timeout=5)
            except subprocess.TimeoutExpired:
                driver.kill()
                driver.wait()

    print("Java CnC layout interop probe passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
