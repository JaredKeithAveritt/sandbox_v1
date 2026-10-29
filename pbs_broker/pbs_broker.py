#!/usr/bin/env python3
"""Host-side broker for safe PBS command passthrough from Bubblewrap."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parent.parent
SOCKET_PATH = Path(
    os.environ.get("BWRAP_PBS_BROKER_SOCKET", REPO_ROOT / "pbs-broker.sock")
)
SUPPORTED_COMMANDS = frozenset({"qsub", "qstat", "qdel", "pbsnodes"})


class PbsError(RuntimeError):
    """A PBS command could not be completed."""


class PbsBroker:
    """Run an allowlisted PBS client command on the host."""

    def run(self, args):
        if not args or args[0] not in SUPPORTED_COMMANDS:
            raise PbsError("unsupported PBS command")
        command = args[0]
        binary = os.environ.get(f"BWRAP_PBS_{command.upper()}", command)
        # Some site-local PBS authentication helpers reject qsub when its
        # standard streams are anonymous pipes.  Let qsub inherit the login
        # terminal exactly as it does when run directly by the user.  qsub's
        # job ID and diagnostics consequently appear in that terminal.
        capture_output = command != "qsub"
        # qsub is also given its own session.  This matches `setsid qsub`,
        # which succeeds on the target PBS installation, rather than making
        # qsub a child in the broker daemon's session.
        own_session = command == "qsub"
        result = subprocess.run(
            [binary, *args[1:]],
            text=True,
            capture_output=capture_output,
            start_new_session=own_session,
            check=False,
        )
        return {
            "ok": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout or "",
            "stderr": result.stderr or "",
            "error": "" if result.returncode == 0 else f"{command} exited with status {result.returncode}",
        }

    def diagnose(self):
        """Return non-secret context that helps compare native and brokered PBS."""
        config = _pbs_config()
        server = os.environ.get("PBS_SERVER") or config.get("PBS_SERVER")
        try:
            port = int(os.environ.get("PBS_SERVER_PORT", "15001"))
        except ValueError:
            port = 15001
        endpoint = {"server": server, "port": port, "tcp": "not configured"}
        if server:
            try:
                with socket.create_connection((server, port), timeout=3):
                    endpoint["tcp"] = "reachable"
            except OSError as error:
                endpoint["tcp"] = f"unreachable: {error}"
        try:
            qstat = self.run(["qstat", "-B"])
        except OSError as error:
            qstat = {"ok": False, "error": str(error)}
        return {
            "pid": os.getpid(),
            "parent_pid": os.getppid(),
            "process_group": os.getpgrp(),
            "session": os.getsid(0),
            "cwd": str(Path.cwd()),
            "qsub_path": shutil.which("qsub"),
            "qstat_path": shutil.which("qstat"),
            "pbs_environment": _safe_pbs_environment(),
            "pbs_config": config,
            "endpoint": endpoint,
            "qstat_B": qstat,
        }


def _safe_pbs_environment():
    values = {}
    for name, value in os.environ.items():
        if name.startswith("PBS_") or name.startswith("BWRAP_PBS_"):
            sensitive = ("token", "password", "secret", "key")
            values[name] = "<redacted>" if any(word in name.lower() for word in sensitive) else value
    return values


def _pbs_config():
    """Read standard PBS configuration fields without exposing unrelated data."""
    candidate = os.environ.get("PBS_CONF_FILE", "/etc/pbs.conf")
    config = {"path": candidate, "status": "not found"}
    try:
        for line in Path(candidate).read_text().splitlines():
            line = line.strip()
            if "=" not in line or line.startswith("#"):
                continue
            name, value = line.split("=", 1)
            if name in {"PBS_SERVER", "PBS_EXEC", "PBS_HOME"}:
                config[name] = value
        config["status"] = "read"
    except OSError as error:
        config["status"] = f"unavailable: {error}"
    return config


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
                if request.get("action") == "diagnose":
                    try:
                        _send(connection, {"ok": True, "diagnostics": broker.diagnose()})
                    except (OSError, ValueError) as error:
                        _send(connection, {"ok": False, "error": str(error)})
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
    parser.add_argument("mode", choices=("server", "close", "stop", "run", "diagnose"))
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.mode == "server":
        _server()
        return
    if args.mode in {"close", "stop"}:
        _print(_client({"action": args.mode}))
        return
    if args.mode == "diagnose":
        reply = _client({"action": "diagnose"})
        if not reply.get("ok"):
            print(reply.get("error", "PBS broker diagnostics failed"), file=sys.stderr)
            raise SystemExit(1)
        print(json.dumps(reply["diagnostics"], indent=2, sort_keys=True))
        return
    forward = args.args[1:] if args.args[:1] == ["--"] else args.args
    _print(_client({"action": "run", "args": forward}))


if __name__ == "__main__":
    main()
