#!/usr/bin/env python3
"""Host-side broker for safe PBS command passthrough from Bubblewrap."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parent.parent
SOCKET_PATH = Path(os.environ.get("PBS_BROKER_SOCKET", REPO_ROOT / "pbs-broker.sock"))
SUPPORTED_COMMANDS = frozenset({"qsub", "qstat", "qdel", "pbsnodes"})


class PbsError(RuntimeError):
    """A PBS command could not be completed."""


class PbsBroker:
    """Run an allowlisted PBS client command on the host."""

    def run(self, args):
        if not args or args[0] not in SUPPORTED_COMMANDS:
            raise PbsError("unsupported PBS command")
        command = args[0]
        binary = os.environ.get(f"PBS_{command.upper()}", command)
        result = subprocess.run(
            [binary, *args[1:]], text=True, capture_output=True, check=False
        )
        return {
            "ok": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "error": "" if result.returncode == 0 else f"{command} exited with status {result.returncode}",
        }


def _receive(connection):
    data = bytearray()
    while chunk := connection.recv(65536):
        data.extend(chunk)
    return json.loads(data.decode()) if data else None


def _send(connection, payload):
    connection.sendall(json.dumps(payload).encode())


def _server():
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(SOCKET_PATH))
    os.chmod(SOCKET_PATH, 0o600)
    server.listen()
    broker = PbsBroker()
    try:
        while True:
            connection, _ = server.accept()
            with connection:
                request = _receive(connection)
                if not request:
                    continue
                if request.get("action") == "stop":
                    _send(connection, {"ok": True})
                    return
                if request.get("action") == "close":
                    _send(connection, {"ok": True})
                    continue
                try:
                    _send(connection, broker.run(request["args"]))
                except (KeyError, PbsError, OSError) as error:
                    _send(connection, {"ok": False, "error": str(error)})
    finally:
        server.close()
        if SOCKET_PATH.exists():
            SOCKET_PATH.unlink()


def _client(payload):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(SOCKET_PATH))
    with client:
        client.sendall(json.dumps(payload).encode())
        client.shutdown(socket.SHUT_WR)
        data = bytearray()
        while chunk := client.recv(65536):
            data.extend(chunk)
    return json.loads(data.decode())


def _print(reply):
    if reply.get("stdout"):
        sys.stdout.write(reply["stdout"])
    if reply.get("stderr"):
        sys.stderr.write(reply["stderr"])
    if not reply.get("ok"):
        print(reply.get("error", "PBS broker request failed"), file=sys.stderr)
        raise SystemExit(reply.get("returncode", 1) or 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("server", "close", "stop", "run"))
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.mode == "server":
        _server()
        return
    if args.mode in {"close", "stop"}:
        _print(_client({"action": args.mode}))
        return
    forward = args.args[1:] if args.args[:1] == ["--"] else args.args
    _print(_client({"action": "run", "args": forward}))


if __name__ == "__main__":
    main()
