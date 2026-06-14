---
name: new-dist-cm-experiment
description: Scaffold a new Distributed-CM experiment in the standard two-phase (partition-once, then compute-many) layout, with numbered submit scripts, env-var dataset overrides, an sbatch wrapper, and /scratch placement. Use when the user wants to set up a new Dist-CM study, benchmark, or parameter sweep.
---

# Scaffolding a new Dist-CM experiment

Dist-CM studies all share one shape: **partition each (network, clustering) once,
then run the compute phase many times** across whatever you're sweeping (worker count,
`--min-batch-cost`, criterion, method, network). The partition depends only on
`(network, clustering, --min-batch-cost)`, so reusing a single partition across every
compute run is what makes a sweep fast and fair. Build the new study to match.

## How to use this skill

Copy the reference scaffold from this skill's **`templates/`** folder into a new study
directory, then edit the lists/paths for the actual study. The templates are:

- **`templates/dcm.sbatch`** — Slurm wrapper: runs the binary under `mpirun`, forwards
  SIGTERM for checkpointing, reads `DCM_BIN`. Per-job resources come from the command line.
- **`templates/01_partition.sh`** — phase 1: partition each (network, clustering) with
  1 MPI task and `--partition-only`.
- **`templates/02_sweep.sh`** — phase 2: reuse those partitions via
  `--partitioned-clusters-dir` and sweep one variable (worker count by default; notes
  at the bottom show how to swap in a batch-size / criterion / comparison sweep).

## Conventions the scaffold encodes (keep them)

- **Live under `/scratch`.** Dist-CM emits hundreds of thousands of small files — point
  every `--work-dir`, partition dir, and output under `/scratch/<netid>/...` (no inode
  limit; auto-purged ~30 days, so copy out keepers when done). See `run-dist-cm`.
- **One directory per study, numbered scripts run in order:** `01_partition.sh` →
  `02_<sweep>.sh` (add `02b_*.sh`, `03_*.sh` for variants).
- **Paths derive from the script's own location** (`SCRIPT_DIR`), and the dataset path
  is an **env-var override** (`DATASET_DIR="${DATASET_DIR:-...}"`) so collaborators
  don't edit the script to point at their own data. Don't hardcode another user's home.
- **Standard defaults across studies:** `--min-batch-cost 1000`,
  `--yield-node-threshold 100000`, `--mincut-type cactus`, `--log-level 2`.
- **CM vs WCC:** CM needs `--algorithm` (+`--clustering-parameter` for leiden-cpm);
  WCC takes neither and usually sets `--connectedness-criterion`.
- **Naming** encodes the sweep so outputs don't collide, e.g.
  `<method>.<network>.<clustering>.w<workers>.r<rep>`; Slurm logs → `slurm-reports/`.
- Partitioning is memory-hungry — give phase 1 a high `--mem-per-cpu` (32–450GB for big
  networks); phase 2 scales `--ntasks` to the worker count.

## Steps

1. Make the study dir under `/scratch`; copy the three templates in.
2. Edit the network / clustering / sweep lists and the `DATASET_DIR` + `DCM_BIN` paths.
3. Ensure inputs exist in binary form — `run-clustering` for an initial clustering,
   `format-conversion` for `.bedgelist`/`.bcluster`.
4. Write a short `README.md` (what's measured, how many jobs each step submits).
5. `bash 01_partition.sh`; wait for completion; then `bash 02_<sweep>.sh`.
6. Summarize with `analyze-dist-cm-results`; log the launch with `record-keeping`.
