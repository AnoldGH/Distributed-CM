---
name: slurm-job
description: Submit and manage Slurm batch jobs on the UIUC campus cluster. Use when the user asks to run something as a Slurm job, or when a task is too large/long to run interactively (e.g. large graphs, long computations, conversions, clustering, or Dist-CM runs).
---

# Slurm batch jobs (UIUC campus cluster)

Submit tasks as Slurm batch jobs.

## Cluster defaults

- **Partition:** `secondary` — the **default** general-purpose queue. Use it unless the
  user explicitly specifies a different partition (then accommodate that).
- **Wall time:** `secondary` has a **hard 4-hour maximum** (`--time=04:00:00`). You
  **cannot** request more than 4h on `secondary`. If a job genuinely needs longer, say
  so to the user rather than silently raising `--time` — for Dist-CM, long jobs rely on
  checkpoint/resume instead (see `run-dist-cm`).
- **Memory:** `16G` default; `32G`+ for large datasets (>10M nodes/edges).
- **Output log:** write to the same directory as the output data, `<jobname>_%j.log`.

## Writing sbatch scripts

1. Write the `.sbatch` file in the same directory as the input/output data.
2. Start from this skill's **`templates/job.sbatch`** — fill in the job name,
   `<output_dir>`, `<command>`, and your `<netid>` (don't hardcode someone else's).
3. Submit with `sbatch <script>.sbatch`, then report the job ID back to the user.

## Guidelines

- **Memory sizing:** ~75M+ node graphs → at least `32G` (Dist-CM partitioning can need
  far more — 128–450G; see `run-dist-cm`). <10M nodes → `16G` is usually fine.
- **Parallelism:** submit independent files/tasks as separate jobs so they run in
  parallel.
- **Monitoring:** check status with `squeue -u $USER`; read logs once jobs complete.
- **Interactive vs batch:** only use Slurm for tasks too large/long for interactive
  execution. Small datasets (e.g. LiveJournal-scale, ~5M nodes) can run interactively.
- **Dist-CM jobs** have their own wrapper (MPI ranks, SIGTERM checkpointing, microarch
  pinning) — use `run-dist-cm` / `new-dist-cm-experiment` rather than this generic
  template for those.
