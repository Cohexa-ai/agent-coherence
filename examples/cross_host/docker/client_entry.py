#!/usr/bin/env python3
# Cross-host demo client entrypoint: wait for the coordinator's port (written to
# the shared .coherence volume), export CCS_REMOTE_PORT, then run the demo.
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from ccs.adapters.claude_code.lifecycle import read_port_from_file

PID_FILE = Path("/coord/.coherence/server.pid")


def main() -> int:
    port = None
    for _ in range(120):  # up to ~60s for the coordinator to bind + write the port
        port = read_port_from_file(PID_FILE)
        if port:
            break
        time.sleep(0.5)
    if not port:
        print("client_entry: coordinator port never appeared", file=sys.stderr)
        return 1
    os.environ["CCS_REMOTE_PORT"] = str(port)
    return subprocess.call([sys.executable, "/app/examples/cross_host/main.py"])


if __name__ == "__main__":
    sys.exit(main())
