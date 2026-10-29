# PBS sandbox example

This is the PBS equivalent of the supplied Slurm example. It preserves the
protected-shell pattern, but adapts it to PBS's ordinary batch workflow:
`qsub` submits a script, `qstat` observes it, and `qdel` cancels it.

The broker relays those PBS commands through SSH to the login host. This is
needed on clusters where PBS's `pbs_iff` authentication rejects a broker daemon
but accepts a normal SSH login session.

## Before first use

On the PBS login node, confirm that `bwrap`, `qsub`, `qstat`, and `qdel` are
available. Compile the included MPI program with the MPI environment supplied
by your cluster:

```sh
mpicc -O2 -o mpi_test mpi_test.c
chmod +x bwrap.py pbs_broker/qsub pbs_broker/qstat pbs_broker/qdel pbs_broker/pbsnodes
```

Before using the broker, establish the SSH host key once interactively and set
the relay target. For the tested setup this is `wheat06`:

```sh
ssh wheat06
exit
export BWRAP_PBS_SSH_HOST=wheat06
```

The included request asks for two nodes, four CPU cores and four MPI processes
per node. Change the `#PBS -l select=...` line in `mpi_test.pbs` to match your
site's resource policy. Sites using Torque commonly use `nodes=2:ppn=4` in
place of the `select` line. The correct MPI launcher is site-specific; the
example uses `mpiexec`.

## Protected PBS session

Start in the directory containing the batch script:

```sh
python3 /path/to/pbs_sandbox/bwrap.py
qsub mpi_test.pbs
qstat -u "$USER"
qdel <job-id>
```

The scheduler clients execute on the host through the Unix-socket broker, so
they keep the cluster's PBS credentials and configuration. The broker then
relays them through an SSH login to satisfy PBS authentication. Only `qsub`,
`qstat`, `qdel`, and `pbsnodes` are exposed by the broker. The current working
directory is writable in the sandbox; the broker code is read-only. `qsub`
intentionally writes its job ID and diagnostics directly to the terminal.

To test only the Bubblewrap isolation layer—without starting, mounting, or
contacting the PBS broker—run:

```sh
python3 /path/to/pbs_sandbox/bwrap.py --no-broker /bin/bash
```

This keeps the root filesystem read-only and the current directory writable.
`--unprotected` is different: it skips the broker *and* makes the root
filesystem writable.

## Diagnostics

These checks do not submit a job. First capture the broker's host-side context:

```sh
python3 bwrap.py --diagnose | tee broker-diagnose.json
```

Then compare native and protected-shell contexts:

```sh
./pbs_probe.sh | tee native-probe.txt
python3 bwrap.py ./pbs_probe.sh | tee bwrap-probe.txt
diff -u native-probe.txt bwrap-probe.txt
```

The output includes command resolution, PBS configuration, safe PBS-related
environment variables (credential-looking values are redacted), the broker
socket state, and a read-only `qstat -B` request.

## Verification

Run the non-mutating check from a writable directory on the PBS login node:

```sh
./verify_pbs_setup.sh
```

It checks that Bubblewrap can start the broker, that the protected command
shims reach the broker socket, and that `qstat -B` can query PBS. It creates no
jobs. To also verify submission and cancellation, run:

```sh
./verify_pbs_setup.sh --submit
```

That submits the bundled one-minute smoke job, confirms that `qstat` sees it,
and immediately cancels it with `qdel`. Adjust `pbs_smoke_test.pbs` if your PBS
site uses a different resource-request syntax.

## PBS command locations

If the PBS commands are not already in the login-node `PATH`, set these before
starting `bwrap.py`:

```sh
export BWRAP_PBS_QSUB=/opt/pbs/bin/qsub
export BWRAP_PBS_QSTAT=/opt/pbs/bin/qstat
export BWRAP_PBS_QDEL=/opt/pbs/bin/qdel
export BWRAP_PBS_PBSNODES=/opt/pbs/bin/pbsnodes
```

PBS does not have a portable counterpart to Slurm's reusable `salloc` + `srun`
allocation. This setup therefore intentionally submits a PBS batch script,
rather than pretending that a later command can attach to an existing PBS job.
