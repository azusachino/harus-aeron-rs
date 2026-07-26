#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///

"""Verify Rust MediaDriver IPC and UDP behavior across independent processes."""

from __future__ import annotations

import selectors
import socket
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DRIVER_BINARY = ROOT / "target" / "debug" / "aeron-driver"
PROBE_BINARY = ROOT / "target" / "debug" / "rust_process_probe"
PROCESS_TIMEOUT = 20


def _run(command: list[str]) -> None:
    result = subprocess.run(command, cwd=ROOT, check=False, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _start_driver() -> tuple[subprocess.Popen[str], Path]:
    process = subprocess.Popen(
        [str(DRIVER_BINARY)],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.stdout is None:
        process.kill()
        raise RuntimeError("Rust driver stdout was not available")

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    ready = selector.select(timeout=PROCESS_TIMEOUT)
    selector.close()
    if not ready:
        process.kill()
        process.wait()
        stderr = process.stderr.read() if process.stderr else ""
        raise RuntimeError(f"Rust driver did not publish a CnC path\n{stderr}")

    cnc_path = process.stdout.readline().strip()
    if not cnc_path:
        process.kill()
        process.wait()
        stderr = process.stderr.read() if process.stderr else ""
        raise RuntimeError(f"Rust driver published an empty CnC path\n{stderr}")
    return process, Path(cnc_path)


def _stop_driver(process: subprocess.Popen[str]) -> None:
    if process.poll() is None and process.stdin is not None:
        process.stdin.write("release\n")
        process.stdin.close()
    try:
        process.wait(timeout=PROCESS_TIMEOUT)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _run_probe(args: list[str]) -> None:
    _run([str(PROBE_BINARY), *args])


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def main() -> int:
    print("[1/4] Building Rust driver and process probe...")
    _run(["cargo", "build", "-p", "aeron-driver", "--bin", "aeron-driver"])
    _run(["cargo", "build", "-p", "aeron-client", "--bin", "rust_process_probe"])

    print("[2/4] Running Rust multi-process IPC round-trip...")
    ipc_driver, ipc_cnc = _start_driver()
    try:
        _run_probe(["ipc", str(ipc_cnc.parent)])
    finally:
        _stop_driver(ipc_driver)

    print("[3/4] Running Rust multi-process UDP round-trip through two drivers...")
    subscriber_driver, subscriber_cnc = _start_driver()
    publisher_driver, publisher_cnc = _start_driver()
    endpoint = f"127.0.0.1:{_free_udp_port()}"
    subscriber = subprocess.Popen(
        [str(PROBE_BINARY), "udp-subscriber", str(subscriber_cnc.parent), endpoint],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    publisher = subprocess.Popen(
        [str(PROBE_BINARY), "udp-publisher", str(publisher_cnc.parent), endpoint],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        subscriber_stdout, subscriber_stderr = subscriber.communicate(timeout=PROCESS_TIMEOUT)
        publisher_stdout, publisher_stderr = publisher.communicate(timeout=PROCESS_TIMEOUT)
        if subscriber.returncode or publisher.returncode:
            raise RuntimeError(
                "Rust UDP process probe failed\n"
                f"subscriber ({subscriber.returncode}) stdout:\n{subscriber_stdout}"
                f"stderr:\n{subscriber_stderr}\n"
                f"publisher ({publisher.returncode}) stdout:\n{publisher_stdout}"
                f"stderr:\n{publisher_stderr}"
            )
    finally:
        for process in (subscriber, publisher):
            if process.poll() is None:
                process.kill()
                process.wait()
        _stop_driver(subscriber_driver)
        _stop_driver(publisher_driver)

    print("[4/4] Rust multi-process IPC and UDP probes passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error
