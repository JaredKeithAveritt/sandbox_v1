#!/usr/bin/env bash
# Verify the protected PBS command path. Run from a writable login-node directory.
set -euo pipefail

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
submit_test=false

usage() {
    printf 'Usage: %s [--submit]\n' "$0"
    printf '  Default: verify Bubblewrap, broker socket, and read-only qstat access.\n'
    printf '  --submit: additionally submit, inspect, then cancel a one-minute PBS job.\n'
}

if [[ ${1:-} == "--submit" ]]; then
    submit_test=true
elif [[ $# -ne 0 ]]; then
    usage >&2
    exit 2
fi

fail() {
    printf 'FAILED: %s\n' "$*" >&2
    exit 1
}
for command in python3 bwrap ssh qsub qstat qdel; do
    command -v "$command" >/dev/null || fail "required command not found: $command"
done
for file in bwrap.py pbs_broker/pbs_broker.py pbs_broker/pbs_client.py; do
    [[ -f "$root_dir/$file" ]] || fail "missing $file"
done
for shim in qsub qstat qdel pbsnodes; do
    [[ -x "$root_dir/pbs_broker/$shim" ]] || fail "broker shim is not executable: $shim"
done

run_protected() {
    "$root_dir/bwrap.py" /bin/sh -c "$1" pbs-smoke "$2"
}

printf 'Checking protected broker socket and PBS server status...\n'
run_protected 'test -S "$BWRAP_PBS_BROKER_SOCKET" && command -v qsub >/dev/null && qstat -B >/dev/null' '' \
    || fail "protected qstat check failed"
printf 'PASS: protected qstat reached the PBS server.\n'

if ! "$submit_test"; then
    printf 'No jobs were submitted. Re-run with --submit to test qsub and qdel.\n'
    exit 0
fi

printf 'Submitting the short smoke-test job...\n'
job_id=$(run_protected 'qsub "$1"' "$root_dir/pbs_smoke_test.pbs" | tr -d '[:space:]')
[[ -n "$job_id" ]] || fail "qsub returned no job ID"
cleanup_needed=true
cleanup() {
    if [[ ${cleanup_needed:-false} == true ]]; then
        run_protected 'qdel "$1" >/dev/null 2>&1 || true' "$job_id"
    fi
}
trap cleanup EXIT

run_protected 'qstat "$1" >/dev/null' "$job_id" || fail "qstat could not find submitted job $job_id"
run_protected 'qdel "$1" >/dev/null' "$job_id" || fail "qdel could not cancel submitted job $job_id"
cleanup_needed=false
trap - EXIT
printf 'PASS: submitted, observed, and cancelled PBS job %s.\n' "$job_id"
