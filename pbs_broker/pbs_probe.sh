#!/usr/bin/env bash
# Read-only PBS client probe. Run this outside and inside bwrap, then compare.
set -u

printf '=== identity and process context ===\n'
id
printf 'pid=%s ppid=%s pgid=%s sid=%s cwd=%s\n' "$$" "$(ps -o ppid= -p $$ | tr -d ' ')" "$(ps -o pgid= -p $$ | tr -d ' ')" "$(ps -o sid= -p $$ | tr -d ' ')" "$PWD"

printf '\n=== command resolution ===\n'
type -a qsub 2>&1 || true
type -a qstat 2>&1 || true

printf '\n=== PBS configuration ===\n'
if [[ -r ${PBS_CONF_FILE:-/etc/pbs.conf} ]]; then
  grep -E '^(PBS_SERVER|PBS_EXEC|PBS_HOME)=' "${PBS_CONF_FILE:-/etc/pbs.conf}" || true
else
  printf 'No readable PBS configuration at %s\n' "${PBS_CONF_FILE:-/etc/pbs.conf}"
fi
env | sort | grep -E '^(PBS_|BWRAP_PBS_)' | sed -E '/(TOKEN|PASSWORD|SECRET|KEY)=/I s/=.*/=<redacted>/' || true

printf '\n=== broker socket ===\n'
if [[ -n ${BWRAP_PBS_BROKER_SOCKET:-} ]]; then
  [[ -S $BWRAP_PBS_BROKER_SOCKET ]] && printf 'present: %s\n' "$BWRAP_PBS_BROKER_SOCKET" || printf 'missing: %s\n' "$BWRAP_PBS_BROKER_SOCKET"
else
  printf 'not using the broker\n'
fi

printf '\n=== read-only PBS query ===\n'
qstat -B
