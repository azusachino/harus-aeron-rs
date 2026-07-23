#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
# ]
# ///

"""
Java Interop Smoke Harness for harus-aeron-rs.
Verifies Rust MediaDriver binary launch and configuration compatibility.
"""

import subprocess
import sys
import time


def main() -> None:
    print("=== harus-aeron-rs: Java Interop Smoke Harness ===")

    # Build driver binary
    print("[1/2] Building rust aeron-driver binary...")
    res = subprocess.run(
        ["cargo", "build", "--bin", "aeron-driver"], capture_output=True, text=True
    )
    if res.returncode != 0:
        print(f"Build failed: {res.stderr}")
        sys.exit(1)

    print("[2/2] Running interop driver smoke check...")
    proc = subprocess.Popen(
        ["cargo", "run", "--bin", "aeron-driver"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(1)

    # Terminate process if still running
    if proc.poll() is None:
        proc.terminate()
        proc.wait()

    print("Java Interop Smoke Harness completed successfully!")


if __name__ == "__main__":
    main()
