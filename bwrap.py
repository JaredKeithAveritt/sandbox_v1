#!/usr/bin/env python3
"""Launch a protected shell whose PBS commands are brokered on the host."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parent
PBS_BROKER_PATH = REPO_ROOT / "pbs_broker"
PBS_BROKER_DIR = str(PBS_BROKER_PATH.resolve())
PBS_BROKER_MAIN = str((PBS_BROKER_PATH / "pbs_broker.py").resolve())
BROKER_SOCKET_NAME = "pbs-broker.sock"


def _bwrap_binary():
    binary = shutil.which("bwrap")
    if binary is None:
        raise RuntimeError("bwrap executable not found in PATH")
    return binary


def _launch_dir():
    return str(Path.cwd().resolve())


def _socket_path():
    return str((Path(_launch_dir()) / BROKER_SOCKET_NAME).resolve())


def _wrapped_path():
    path = os.environ.get("PATH", "")
    return f"{PBS_BROKER_DIR}:{path}" if path else PBS_BROKER_DIR


def wrap_main(command, protected=True, broker_enabled=True):
    """Build the Bubblewrap command; the current directory remains writable."""
    root_mode = ["--ro-bind", "/", "/"] if protected else ["--bind", "/", "/"]
    path = _wrapped_path() if protected and broker_enabled else os.environ.get("PATH", "")
    broker_environment = (
        ["--setenv", "BWRAP_PBS_BROKER_SOCKET", _socket_path()]
        if broker_enabled
        else []
    )
    broker_mount = (
        ["--bind", PBS_BROKER_DIR, PBS_BROKER_DIR, "--remount-ro", PBS_BROKER_DIR]
        if broker_enabled
        else []
    )
    return [
        _bwrap_binary(),
        *root_mode,
        "--dev", "/dev",
        "--proc", "/proc",
        "--setenv", "IN_BWRAP", "1",
        "--setenv", "HOME", _launch_dir(),
        "--setenv", "TMPDIR", _launch_dir(),
        "--setenv", "PATH", path,
        *broker_environment,
        "--setenv", "PS1", "[bwrap-pbs] \\u@\\h:\\w\\$ ",
        "--chdir", _launch_dir(),
        "--bind", _launch_dir(), _launch_dir(),
        *broker_mount,
        "--die-with-parent",
        "--",
        *command,
    ]


def _wait_for_broker(deadline):
    environment = {**os.environ, "BWRAP_PBS_BROKER_SOCKET": _socket_path()}
    while time.monotonic() < deadline:
        probe = subprocess.run(
            [sys.executable, PBS_BROKER_MAIN, "close"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if probe.returncode in {0, 1}:
            return
        time.sleep(0.1)
    raise RuntimeError("PBS broker server did not start in time")


def _start_broker():
    broker = subprocess.Popen(
        [sys.executable, PBS_BROKER_MAIN, "server"],
        env={**os.environ, "BWRAP_PBS_BROKER_SOCKET": _socket_path()},
        start_new_session=True,
    )
    _wait_for_broker(time.monotonic() + 5)
    return broker


def _stop_broker(broker):
    subprocess.run(
        [sys.executable, PBS_BROKER_MAIN, "stop"],
        env={**os.environ, "BWRAP_PBS_BROKER_SOCKET": _socket_path()},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    try:
        broker.wait(timeout=5)
    except subprocess.TimeoutExpired:
        broker.terminate()
        broker.wait(timeout=5)


def _print_broker_diagnostics():
    """Start the normal host-side broker briefly and print its safe probe data."""
    broker = _start_broker()
    try:
        result = subprocess.run(
            [sys.executable, PBS_BROKER_MAIN, "diagnose"],
            env={**os.environ, "BWRAP_PBS_BROKER_SOCKET": _socket_path()},
            check=False,
        )
        raise SystemExit(result.returncode)
    finally:
        _stop_broker(broker)


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--unprotected", action="store_true")
    modes.add_argument("--no-broker", action="store_true")
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="print the host-side broker's PBS configuration and qstat probe; submits no job",
    )
    parser.add_argument("command", nargs="*")
    args = parser.parse_args()
    if args.diagnose:
        if args.command:
            parser.error("--diagnose does not accept a command")
        _print_broker_diagnostics()
        return
    command = args.command or ["/bin/bash", "-i"]
    broker = None
    if not args.unprotected and not args.no_broker:
        broker = _start_broker()
    try:
        raise SystemExit(
            subprocess.run(
                wrap_main(command, not args.unprotected, not args.no_broker), check=False
            ).returncode
        )
    finally:
        if broker is not None:
            _stop_broker(broker)


if __name__ == "__main__":
    main()
