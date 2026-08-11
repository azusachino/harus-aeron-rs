#!/usr/bin/env -S uv run --script
"""Run the real-process Tokio UDP comparison for the sibling issue #20 repro."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NODE_BINARY = ROOT / "target" / "debug" / "aeron-cluster-node"
CLIENT_BINARY = ROOT / "target" / "debug" / "aeron-cluster-client"


def _build() -> None:
    subprocess.run(
        [
            "cargo",
            "build",
            "-p",
            "aeron-cluster",
            "--bin",
            "aeron-cluster-node",
            "--bin",
            "aeron-cluster-client",
        ],
        cwd=ROOT,
        check=True,
    )


def _reserve_ports() -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(3):
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.bind(("127.0.0.1", 0))
            sockets.append(probe)
        return [probe.getsockname()[1] for probe in sockets]
    finally:
        for probe in sockets:
            probe.close()


def _terminate(process: subprocess.Popen[str], force: bool = False) -> None:
    if process.poll() is not None:
        return
    if force:
        process.kill()
    else:
        process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _leader_changes(log: str, member_id: int) -> list[str]:
    marker = f"RUST_CLUSTER_LEADER_CHANGE member={member_id}"
    return [line for line in log.splitlines() if marker in line]


def main() -> int:
    _build()
    ports = _reserve_ports()
    endpoint_map = ",".join(f"{member}=127.0.0.1:{port}" for member, port in enumerate(ports))
    ingress = ",".join(f"127.0.0.1:{port}" for port in ports)
    order_count = int(os.environ.get("ORDER_COUNT", "100"))
    order_interval_ms = int(os.environ.get("ORDER_INTERVAL_MS", "1000"))
    kill_delay_ms = int(os.environ.get("KILL_DELAY_MS", "10000"))
    offer_timeout_ms = int(os.environ.get("OFFER_TIMEOUT_MS", "60000"))
    response_timeout_ms = int(os.environ.get("RESPONSE_TIMEOUT_MS", "250"))
    timeout_seconds = max(30, order_count * order_interval_ms / 1000 + 30)

    with tempfile.TemporaryDirectory(prefix="aeron-rust-cluster-") as directory:
        log_paths = [Path(directory) / f"node-{member}.log" for member in range(3)]
        nodes: list[subprocess.Popen[str]] = []
        log_files = []
        client: subprocess.Popen[str] | None = None
        try:
            for member, log_path in enumerate(log_paths):
                environment = {
                    **os.environ,
                    "CLUSTER_MEMBER_ID": str(member),
                    "CLUSTER_LEADER_MEMBER_ID": "0",
                    "ELECTION_TIMEOUT_MS": os.environ.get("ELECTION_TIMEOUT_MS", "5000"),
                    "CLUSTER_INTERNAL_ENDPOINTS": endpoint_map,
                }
                log_file = log_path.open("w")
                log_files.append(log_file)
                nodes.append(
                    subprocess.Popen(
                        [str(NODE_BINARY)],
                        cwd=ROOT,
                        env=environment,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                )

            client_environment = {
                **os.environ,
                "INGRESS_ENDPOINTS": ingress,
                "ORDER_COUNT": str(order_count),
                "ORDER_INTERVAL_MS": str(order_interval_ms),
                "OFFER_TIMEOUT_MS": str(offer_timeout_ms),
                "RESPONSE_TIMEOUT_MS": str(response_timeout_ms),
            }
            client = subprocess.Popen(
                [str(CLIENT_BINARY)],
                cwd=ROOT,
                env=client_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            time.sleep(kill_delay_ms / 1000)
            _terminate(nodes[0], force=True)
            try:
                stdout, stderr = client.communicate(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                client.kill()
                stdout, stderr = client.communicate()
                raise RuntimeError(
                    f"client timed out\nstdout:\n{stdout}\nstderr:\n{stderr}"
                ) from None
            if client.returncode != 0:
                raise RuntimeError(
                    f"client failed ({client.returncode})\nstdout:\n{stdout}\nstderr:\n{stderr}"
                )
            if "RUST_CLUSTER_CLIENT_RESULT" not in stdout:
                raise RuntimeError(
                    f"client did not report success\nstdout:\n{stdout}\nstderr:\n{stderr}"
                )
        finally:
            if client is not None and client.poll() is None:
                _terminate(client)
            for node in nodes[1:]:
                _terminate(node)
            for log_file in log_files:
                log_file.close()

        logs = [path.read_text() for path in log_paths]
        for member in (1, 2):
            changes = _leader_changes(logs[member], member)
            if not any("leader=1 term=2" in line for line in changes):
                raise RuntimeError(
                    f"member {member} did not observe term-2 failover:\n{logs[member]}"
                )
            if len(changes) > 4:
                raise RuntimeError(f"member {member} repeated leader changes:\n{logs[member]}")
            print(f"member {member}: leader_changes={len(changes)} final={changes[-1]}")
        print("Rust Tokio UDP failover comparison passed")
        print(stdout.strip())
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error
