#!/bin/bash
#SBATCH --time=04:00:00
#SBATCH --partition=secondary
#SBATCH --signal=B:TERM@90       # SIGTERM 90s before wall limit -> checkpoint
#SBATCH --constraint=intel       # pin microarch to avoid SIGILL on mixed nodes
# Per-job --ntasks / --mem-per-cpu / --job-name / --output / --error are passed
# on the sbatch command line. Pass the CM/WCC subcommand + args after this script.
#
# Wraps the Dist-CM binary under mpirun and forwards SIGTERM so the program can
# checkpoint. Override the binary location with the DCM_BIN env var.

DCM_BIN="${DCM_BIN:-/path/to/Distributed-CM/build/distributed_connectivity_modifier}"

module load openmpi

cleanup() { kill -TERM "$PID" 2>/dev/null; wait "$PID"; }
trap cleanup SIGTERM

mpirun --use-hwthread-cpus -- "${DCM_BIN}" "$@" & PID=$!
wait $PID
