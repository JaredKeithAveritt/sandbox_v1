#!/usr/bin/env python3
"""PBS command shim used inside the protected shell."""
import json
import os
from pathlib import Path
import socket
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
SOCKET_PATH = Path(os.environ.get("PBS_BROKER_SOCKET", REPO_ROOT / "pbs-broker.sock"))


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: pbs_client.py <pbs-command> [args...]")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(SOCKET_PATH))
    with client:
        client.sendall(json.dumps({"action": "run", "args": sys.argv[1:]}).encode())
        client.shutdown(socket.SHUT_WR)
        data = bytearray()
        while chunk := client.recv(65536):
            data.extend(chunk)
    reply = json.loads(data.decode())
    if reply.get("stdout"):
        sys.stdout.write(reply["stdout"])
    if reply.get("stderr"):
        sys.stderr.write(reply["stderr"])
    if not reply.get("ok"):
        print(reply.get("error", "PBS broker request failed"), file=sys.stderr)
        raise SystemExit(reply.get("returncode", 1) or 1)


if __name__ == "__main__":
    main()
