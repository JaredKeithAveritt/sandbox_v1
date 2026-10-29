#!/usr/bin/env python3
"""Client shims that forward normal Slurm calls to the host-side broker."""
import json
import os
import socket
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SOCKET_PATH = Path(os.environ.get('SLURM_BROKER_SOCKET', str((REPO_ROOT / 'slurm-broker.sock').resolve())))


def send(payload):
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


def print_reply(reply):
    if not reply.get('ok'):
        print(reply.get('error', 'broker request failed'), file=sys.stderr)
        raise SystemExit(1)
    if reply.get('stdout'):
        sys.stdout.write(reply['stdout'])
    if reply.get('stderr'):
        sys.stderr.write(reply['stderr'])
    if reply.get('job_id'):
        print(f'Granted job allocation {reply["job_id"]}')
    if reply.get('nodes'):
        shell_host = socket.gethostname().split('.')[0]
        target = reply['nodes'][0]
        print(f'Shell is still on {shell_host}')
        print(f'Allocation is on {target}')
        for node in reply['nodes']:
            print(node)


def main():
    if len(sys.argv) < 2:
        print('usage: slurm_client.py <command> [args...]', file=sys.stderr)
        raise SystemExit(2)
    command_name = sys.argv[1]
    command_args = sys.argv[2:]

    if command_name == 'salloc':
        print_reply(send({'action': 'open', 'args': command_args}))
        return

    if command_name == 'srun':
        if '--' in command_args:
            split = command_args.index('--')
            extra_args = command_args[:split]
            command = command_args[split + 1:]
        else:
            extra_args = []
            command = command_args
        print_reply(send({'action': 'srun', 'extra_args': extra_args, 'command': command}))
        return

    if command_name in {'squeue', 'scontrol'}:
        print_reply(send({'action': 'passthrough', 'args': [command_name, *command_args]}))
        return

    if command_name == 'scancel':
        if command_args:
            print_reply(send({'action': 'passthrough', 'args': ['scancel', *command_args]}))
        else:
            print_reply(send({'action': 'close'}))
        return

    print(f'unsupported broker client shim: {command_name}', file=sys.stderr)
    raise SystemExit(2)


if __name__ == '__main__':
    main()
