#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///

"""Run a Java-side compatibility probe against a Rust-generated CnC file."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DRIVER_BINARY = ROOT / "target" / "debug" / "aeron-driver"
JAVA_SOURCE = ROOT / "deploy" / "interop" / "CncLayoutProbe.java"
JAVA_PUBSUB_SOURCE = ROOT / "deploy" / "interop" / "JavaPubSubProbe.java"
JAVA_UDP_SOURCE = ROOT / "deploy" / "interop" / "JavaUdpPubSubProbe.java"
JAVA_DRIVER_HOST_SOURCE = ROOT / "deploy" / "interop" / "JavaDriverHost.java"
# The source of truth for layout and protocol behavior is vendor/aeron source. The
# assembled JAR is used here only as the Java runtime classpath for the interop probes.
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


def _interop_message_count() -> int:
    value = os.environ.get("INTEROP_SOAK_MESSAGES", "1")
    try:
        message_count = int(value)
    except ValueError as error:
        raise RuntimeError("INTEROP_SOAK_MESSAGES must be a positive integer") from error
    if message_count <= 0:
        raise RuntimeError("INTEROP_SOAK_MESSAGES must be a positive integer")
    return message_count


def main() -> int:
    if shutil.which("java") is None or shutil.which("javac") is None:
        print("interop-smoke requires java and javac", file=sys.stderr)
        return 1

    print("[1/6] Building Rust MediaDriver binary...")
    _run(["cargo", "build", "-p", "aeron-driver", "--bin", "aeron-driver"], cwd=ROOT)

    print("[2/6] Resolving vendored Aeron Java runtime jar (source is vendor/aeron)...")
    aeron_all_jar = _aeron_all_jar()
    message_count = _interop_message_count()

    print("[3/6] Compiling Java compatibility probes...")
    with tempfile.TemporaryDirectory(prefix="aeron-interop-") as temp_dir:
        classes = Path(temp_dir)
        _run([
            "javac",
            "-cp",
            str(aeron_all_jar),
            "-d",
            str(classes),
            str(JAVA_SOURCE),
            str(JAVA_PUBSUB_SOURCE),
            str(JAVA_UDP_SOURCE),
            str(JAVA_DRIVER_HOST_SOURCE),
        ])

        print("[4/6] Mapping Rust cnc.dat with Aeron Java readers...")
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
            print("[5/6] Running Java Aeron IPC round-trip through Rust driver...")
            _run(
                [
                    "java",
                    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "-cp",
                    f"{classes}:{aeron_all_jar}",
                    "JavaPubSubProbe",
                    str(Path(cnc_path).parent),
                ],
                cwd=ROOT,
            )
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe_socket:
                probe_socket.bind(("127.0.0.1", 0))
                udp_port = str(probe_socket.getsockname()[1])
            print(
                "[6/6] Running Java Aeron UDP round-trip through Rust driver "
                f"(messages={message_count})..."
            )
            _run(
                [
                    "java",
                    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "-cp",
                    f"{classes}:{aeron_all_jar}",
                    "JavaUdpPubSubProbe",
                    str(Path(cnc_path).parent),
                    udp_port,
                    str(message_count),
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

        print("[7/8] Starting vendored Java MediaDriver for Rust-client mixing...")
        with tempfile.TemporaryDirectory(prefix="aeron-java-driver-") as java_dir:
            java_driver = subprocess.Popen(
                [
                    "java",
                    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "-cp",
                    f"{classes}:{aeron_all_jar}",
                    "JavaDriverHost",
                    java_dir,
                ],
                cwd=ROOT,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                if java_driver.stdout is None:
                    raise RuntimeError("Java MediaDriver stdout was not available")
                advertised_dir = java_driver.stdout.readline().strip()
                if advertised_dir != java_dir:
                    stderr = java_driver.stderr.read() if java_driver.stderr else ""
                    raise RuntimeError(
                        "Java MediaDriver did not publish its directory: "
                        f"{advertised_dir!r}\n{stderr}"
                    )
                print("[8/8] Running Rust client IPC round-trip through Java MediaDriver...")
                _run(
                    [
                        "cargo",
                        "run",
                        "-p",
                        "aeron-client",
                        "--bin",
                        "rust_client_probe",
                        "--",
                        java_dir,
                    ],
                    cwd=ROOT,
                )
            finally:
                if java_driver.stdin is not None:
                    java_driver.stdin.write("release\n")
                    java_driver.stdin.close()
                try:
                    java_driver.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    java_driver.kill()
                    java_driver.wait()

    print("Java CnC, bidirectional IPC, and UDP interop probes passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
