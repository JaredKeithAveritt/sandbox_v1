"""Small Slurm broker with optional Bubblewrap protection."""

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.dont_write_bytecode = True

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bwrap import wrap_slurm

SOCKET_PATH = Path(os.environ.get('SLURM_BROKER_SOCKET', str((REPO_ROOT / 'slurm-broker.sock').resolve())))


class SlurmError(RuntimeError):
    """Slurm did not complete a requested operation."""


class SlurmBroker:
    """Keep one Slurm allocation and run commands inside it."""

    def __init__(self, protected=True):
        self.protected = protected
        self.name = f"slurm-broker-{os.getpid()}-{int(time.time())}"
        self.job_id = None
        self.allocation = None
        self.wrapper = str((Path(__file__).resolve().parent / 'wrapped_slurm').resolve())

    def open(self, salloc_args):
        if self.job_id is not None:
            raise SlurmError('allocation already exists')

        command = self._slurm([
            'salloc',
            '--no-shell',
            '--job-name', self.name,
            *salloc_args,
        ])
        self.allocation = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            job_id = self._find_job()
            if job_id is not None:
                self.job_id = job_id
                self._wait_ready(deadline)
                return {
                    'job_id': self.job_id,
                    'nodes': self.nodes(),
                    'stdout': self._drain_pipe(self.allocation.stdout),
                    'stderr': self._drain_pipe(self.allocation.stderr),
                }
            if self.allocation.poll() is not None:
                detail = self._drain_pipe(self.allocation.stderr).strip()
                raise SlurmError(f'salloc failed: {detail or "no error message"}')
            time.sleep(0.25)

        raise SlurmError('salloc did not allocate within 120 seconds')

    def close(self):
        if self.job_id is not None and self._find_job() == self.job_id:
            self.run(['scancel', self.job_id])
        self.job_id = None
        if self.allocation is not None:
            try:
                self.allocation.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.allocation.terminate()
                self.allocation.wait()
            self.allocation = None

    def nodes(self):
        self._require_job()
        result = self.run(['scontrol', 'show', 'job', self.job_id], capture_output=True)
        nodelist = None
        for field in result.stdout.split():
            if field.startswith('NodeList='):
                nodelist = field.split('=', 1)[1]
                break
        if not nodelist:
            raise SlurmError('could not find NodeList in scontrol output')
        hostnames = self.run(['scontrol', 'show', 'hostnames', nodelist], capture_output=True)
        return [line for line in hostnames.stdout.splitlines() if line]

    def srun(self, command, *, extra_args=None, capture_output=False):
        self._require_job()
        args = ['srun', '--jobid', self.job_id]
        if extra_args:
            args.extend(extra_args)
        args.extend(['--', *command])
        return self.run(args, capture_output=capture_output)

    def run(self, args, *, capture_output=False):
        command = self._slurm(args)
        result = subprocess.run(command, text=True, check=False, capture_output=capture_output)
        if result.returncode != 0:
            raise SlurmError(
                json.dumps({
                    'returncode': result.returncode,
                    'command': command,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                })
            )
        return result

    def passthrough(self, args):
        return self.run(args, capture_output=True)

    def _slurm(self, args):
        if self.protected and args[0] == 'srun':
            return wrap_slurm([self.wrapper, *args])
        return [str((Path('/usr/bin') / args[0]).resolve()), *args[1:]]

    def _find_job(self):
        result = subprocess.run(
            self._slurm(['squeue', '--noheader', '--name', self.name, '--format=%A|%j']),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            return None
        for line in result.stdout.splitlines():
            fields = line.strip().split('|', maxsplit=1)
            if len(fields) == 2 and fields[1] == self.name:
                return fields[0]
        return None

    def _wait_ready(self, deadline):
        while time.monotonic() < deadline:
            state = self._job_state()
            if state in {'RUNNING', 'COMPLETING'}:
                return
            if self.allocation is not None and self.allocation.poll() is not None:
                detail = self._drain_pipe(self.allocation.stderr).strip()
                raise SlurmError(f'salloc ended before allocation was ready: {detail or "no error message"}')
            time.sleep(0.25)
        raise SlurmError('allocation did not become ready within 120 seconds')

    def _job_state(self):
        if self.job_id is None:
            return None
        result = subprocess.run(
            self._slurm(['squeue', '--noheader', '--job', self.job_id, '--format=%T']),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            return None
        lines = result.stdout.strip().splitlines()
        return lines[0].strip() if lines else None

    def _require_job(self):
        if self.job_id is None:
            raise SlurmError('no active allocation')

    @staticmethod
    def _drain_pipe(pipe):
        if pipe is None:
            return ''
        try:
            return pipe.read() or ''
        except Exception:
            return ''


def _recv_json(connection):
    data = b''
    while True:
        chunk = connection.recv(65536)
        if not chunk:
            break
        data += chunk
    if not data:
        return None
    return json.loads(data.decode())


def _send_json(connection, payload):
    connection.sendall(json.dumps(payload).encode())


def _server():
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(SOCKET_PATH))
    os.chmod(SOCKET_PATH, 0o600)
    server.listen()
    broker = SlurmBroker(protected=True)

    try:
        while True:
            connection, _ = server.accept()
            with connection:
                request = _recv_json(connection)
                if request is None:
                    continue
                action = request['action']
                try:
                    if action == 'open':
                        reply = broker.open(request.get('args', []))
                        _send_json(connection, {'ok': True, **reply})
                    elif action == 'srun':
                        result = broker.srun(
                            request['command'],
                            extra_args=request.get('extra_args'),
                            capture_output=True,
                        )
                        _send_json(connection, {
                            'ok': True,
                            'returncode': result.returncode,
                            'stdout': result.stdout,
                            'stderr': result.stderr,
                        })
                    elif action == 'passthrough':
                        result = broker.passthrough(request['args'])
                        _send_json(connection, {
                            'ok': True,
                            'returncode': result.returncode,
                            'stdout': result.stdout,
                            'stderr': result.stderr,
                        })
                    elif action == 'close':
                        broker.close()
                        _send_json(connection, {'ok': True})
                    elif action == 'stop':
                        broker.close()
                        _send_json(connection, {'ok': True})
                        return
                    else:
                        _send_json(connection, {'ok': False, 'error': f'unknown action: {action}'})
                except Exception as error:
                    _send_json(connection, {'ok': False, 'error': str(error)})
    finally:
        broker.close()
        server.close()
        if SOCKET_PATH.exists():
            SOCKET_PATH.unlink()


def _client(payload):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(SOCKET_PATH))
    with client:
        client.sendall(json.dumps(payload).encode())
        client.shutdown(socket.SHUT_WR)
        data = b''
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            data += chunk
    return json.loads(data.decode())


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='mode', required=True)

    sub.add_parser('server')

    open_parser = sub.add_parser('open')
    open_parser.add_argument('args', nargs=argparse.REMAINDER)

    srun_parser = sub.add_parser('srun')
    srun_parser.add_argument('args', nargs=argparse.REMAINDER)

    sub.add_parser('close')
    sub.add_parser('stop')

    args = parser.parse_args()

    if args.mode == 'server':
        _server()
        return

    if args.mode == 'open':
        forward = args.args[1:] if args.args and args.args[0] == '--' else args.args
        reply = _client({'action': 'open', 'args': forward})
    elif args.mode == 'srun':
        forward = args.args[1:] if args.args and args.args[0] == '--' else args.args
        if '--' in forward:
            split = forward.index('--')
            extra_args = forward[:split]
            command = forward[split + 1:]
        else:
            extra_args = []
            command = forward
        reply = _client({'action': 'srun', 'extra_args': extra_args, 'command': command})
    elif args.mode == 'close':
        reply = _client({'action': 'close'})
    else:
        reply = _client({'action': 'stop'})

    if not reply.get('ok'):
        print(reply.get('error', 'broker request failed'), file=sys.stderr)
        raise SystemExit(1)

    if 'stdout' in reply and reply['stdout']:
        sys.stdout.write(reply['stdout'])
    if 'stderr' in reply and reply['stderr']:
        sys.stderr.write(reply['stderr'])
    if 'job_id' in reply:
        print(f'Granted job allocation {reply["job_id"]}')
    if 'nodes' in reply:
        for node in reply['nodes']:
            print(node)


if __name__ == '__main__':
    main()
