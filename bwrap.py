#!/usr/bin/env python3
"""Small Bubblewrap helper for an interactive protected shell."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True


REPO_ROOT = Path(__file__).resolve().parent
READ_ONLY_PATH = REPO_ROOT / 'read_only_folder'
SLURM_BROKER_PATH = REPO_ROOT / 'slurm_broker'
READ_ONLY_DIR = str(READ_ONLY_PATH.resolve())
SLURM_BROKER_DIR = str(SLURM_BROKER_PATH.resolve())
SLURM_BROKER_MAIN = str((SLURM_BROKER_PATH / 'slurm_broker.py').resolve())
SLURM_BROKER_SOCKET_NAME = 'slurm-broker.sock'


def _wrapped_path():
    current = os.environ.get('PATH', '')
    if not current:
        return SLURM_BROKER_DIR
    return f'{SLURM_BROKER_DIR}:{current}'


def _bwrap_binary():
    binary = shutil.which('bwrap')
    if binary is None:
        raise RuntimeError('bwrap executable not found in PATH')
    return binary


def _launch_dir():
    return str(Path.cwd().resolve())


def _bwrap_rc_path():
    return str((Path(_launch_dir()) / '.bwrap_bashrc').resolve())


def _broker_socket_path():
    return str((Path(_launch_dir()) / SLURM_BROKER_SOCKET_NAME).resolve())


def _optional_bind_rules():
    rules = ['--bind', _launch_dir(), _launch_dir()]
    if SLURM_BROKER_PATH.exists():
        rules.extend(['--bind', SLURM_BROKER_DIR, SLURM_BROKER_DIR, '--remount-ro', SLURM_BROKER_DIR])
    if READ_ONLY_PATH.exists():
        rules.extend(['--bind', READ_ONLY_DIR, READ_ONLY_DIR, '--remount-ro', READ_ONLY_DIR])
    return rules


def wrap_main(command, protected=True):
    path_value = os.environ.get('PATH', '')
    if not protected:
        return [
            _bwrap_binary(),
            '--bind', '/', '/',
            '--dev', '/dev',
            '--proc', '/proc',
            '--setenv', 'IN_BWRAP', '1',
            '--setenv', 'HOME', _launch_dir(),
            '--setenv', 'PATH', path_value,
            '--setenv', 'SLURM_BROKER_SOCKET', _broker_socket_path(),
            '--setenv', 'PS1', '[bwrap-unprotected] \\u@\\h:\\w\\$ ',
            '--chdir', _launch_dir(),
            '--die-with-parent',
            '--',
            *command,
        ]

    return [
        _bwrap_binary(),
        '--ro-bind', '/', '/',
        '--dev', '/dev',
        '--proc', '/proc',
        '--setenv', 'IN_BWRAP', '1',
        '--setenv', 'HOME', _launch_dir(),
        '--setenv', 'TMPDIR', _launch_dir(),
        '--setenv', 'PATH', _wrapped_path(),
        '--setenv', 'SLURM_BROKER_SOCKET', _broker_socket_path(),
        '--setenv', 'PS1', '[bwrap] \\u@\\h:\\w\\$ ',
        '--chdir', _launch_dir(),
        '--bind', '/dev/shm', '/dev/shm',
        '--bind', '/dev/shm', '/var/spool/slurmd/pmixtmp/munge',
        *_optional_bind_rules(),
        '--ro-bind', '/dev/null', '/usr/lib/ssh/ssh-keysign',
        '--die-with-parent',
        '--',
        *command,
    ]


def wrap_slurm(command):
    return [
        _bwrap_binary(),
        '--ro-bind', '/', '/',
        '--dev', '/dev',
        '--proc', '/proc',
        '--setenv', 'IN_BWRAP', '1',
        '--setenv', 'PS1', '[bwrap-slurm] \\u@\\h:\\w\\$ ',
        '--bind', str(REPO_ROOT), str(REPO_ROOT),
        *_optional_bind_rules(),
        '--die-with-parent',
        '--',
        *command,
    ]


def _wait_for_broker(deadline):
    while time.monotonic() < deadline:
        probe = subprocess.run(
            [sys.executable, SLURM_BROKER_MAIN, 'close'],
            env={**os.environ, 'SLURM_BROKER_SOCKET': _broker_socket_path()},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if probe.returncode in {0, 1}:
            return
        time.sleep(0.1)
    raise RuntimeError('slurm broker server did not start in time')


def _start_broker():
    broker = subprocess.Popen(
        [sys.executable, SLURM_BROKER_MAIN, 'server'],
        env={**os.environ, 'SLURM_BROKER_SOCKET': _broker_socket_path()},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    _wait_for_broker(time.monotonic() + 5)
    return broker


def _stop_broker(broker):
    subprocess.run(
        [sys.executable, SLURM_BROKER_MAIN, 'stop'],
        env={**os.environ, 'SLURM_BROKER_SOCKET': _broker_socket_path()},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    try:
        broker.wait(timeout=5)
    except subprocess.TimeoutExpired:
        broker.terminate()
        try:
            broker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            broker.kill()
            broker.wait()


def _write_bash_rc(real_home, prompt_prefix):
    real_rc = Path(real_home) / '.bashrc'
    lines = []
    if real_rc.exists():
        lines.append(f'source {real_rc}\n')
    lines.append(f'PS1="{prompt_prefix}${{PS1}}"\n')
    lines.append('export PS1\n')
    Path(_bwrap_rc_path()).write_text(''.join(lines))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--unprotected', action='store_true')
    parser.add_argument('command', nargs='*')
    args = parser.parse_args()

    real_home = os.environ.get('HOME', str(REPO_ROOT))
    prompt_prefix = '[bwrap-unprotected] ' if args.unprotected else '[bwrap] '
    _write_bash_rc(real_home, prompt_prefix)
    if args.command:
        command = args.command
    else:
        command = ['/bin/bash', '--rcfile', _bwrap_rc_path(), '-i']
    wrapped = wrap_main(command, protected=not args.unprotected)
    broker = None
    if not args.unprotected:
        broker = _start_broker()

    try:
        raise SystemExit(subprocess.run(wrapped, check=False).returncode)
    finally:
        if broker is not None:
            _stop_broker(broker)


if __name__ == '__main__':
    main()

