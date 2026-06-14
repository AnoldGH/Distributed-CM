---
name: run-dist-cm
description: Run a Distributed-CM (CM or WCC) experiment end to end on the campus cluster — prepare inputs, partition once, then compute across MPI workers, with checkpoint/resume. Use when the user asks to run Dist-CM / Dist-WCC, run the connectivity modifier, or process a clustering with Dist-CM.
---

# Running Dist-CM

Dist-CM has two subcommands: **CM** (re-clusters each community with `--algorithm`)
and **WCC** (mincut-only well-connectedness checking, no re-clustering). They share
the same common arguments. The binary is `build/distributed_connectivity_modifier`.

## Rule 0: run under `/scratch`

Dist-CM creates a **huge number of small files** — one `.edgelist` + one `.cluster`
per cluster at partition time, plus per-worker output dirs and per-cluster logs. A
large network partitions into tens/hundreds of thousands of inodes, which **blows the
file-count quota on home/project space**.

➡️ Put every `--work-dir`, partition dir, and output under **`/scratch/<netid>/...`**
(no inode limit). `/scratch` is **auto-purged ~every 30 days** — when a study is done,
copy out what you want to keep (final clusterings, `logs/load_balancer.log`, result
tables) to persistent storage.

## Step 1 — prepare inputs

Inputs are a network **edgelist** and an **existing clustering** to refine:
- Text (CSV with header): edgelist `source,target`; clustering `node_id,cluster_id`.
- Binary (recommended for large networks — smaller, faster I/O): `.bedgelist`,
  `.bcluster`. Convert with `tools/edgelist_to_binary.py` / `tools/cluster_to_binary.py`
  (or use the `format-conversion` skill). The binary may take either text or binary;
  keep edgelist and clustering in the **same** representation.

Need an initial clustering first? Use the `run-clustering` skill (Leiden-Mod /
Leiden-CPM / IKC).

## Step 2 — partition once (`--partition-only`, 1 MPI task)

Partitioning slices the clustering into per-cluster files. It depends only on
`(network, clustering, --min-batch-cost)` — **not** on worker count or criterion — so
do it **once** and reuse it. Run with a single MPI task:

```bash
mpirun -np 1 build/distributed_connectivity_modifier CM \
    --edgelist  <net>.bedgelist \
    --existing-clustering <net>.<clust>.bcluster \
    --output-file /scratch/<netid>/.../partition.out \
    --work-dir   /scratch/<netid>/.../work-partition/<name> \
    --algorithm leiden-cpm --clustering-parameter 0.01 \
    --min-batch-cost 1000 --log-level 2 \
    --partition-only
```

This writes `<work-dir>/clusters/` (`summary.csv` + `<id>.edgelist` + `<id>.cluster`).
Partitioning big networks is memory-hungry — give the 1-task job lots of RAM
(`--mem-per-cpu=32GB`, up to 128–450GB for the largest networks).

## Step 3 — compute (N MPI ranks, reuse the partition)

Point `--partitioned-clusters-dir` at the partition from step 2 to skip partitioning,
and scale `-np` / `--ntasks` to the worker count you want:

```bash
mpirun -np 64 build/distributed_connectivity_modifier CM \
    --edgelist  <net>.bedgelist \
    --existing-clustering <net>.<clust>.bcluster \
    --output-file /scratch/<netid>/.../output/<job>.out \
    --work-dir   /scratch/<netid>/.../work/<job> \
    --partitioned-clusters-dir /scratch/<netid>/.../work-partition/<name>/clusters \
    --algorithm leiden-cpm --clustering-parameter 0.01 \
    --min-batch-cost 1000 --yield-node-threshold 100000 \
    --mincut-type cactus --log-level 2
```

**WCC** is the same but the subcommand is `WCC` and you **drop** `--algorithm` /
`--clustering-parameter` (WCC doesn't re-cluster). WCC commonly takes an explicit
`--connectedness-criterion`.

## Argument cheat-sheet (defaults / conventions)

| Argument | Typical | Meaning |
|---|---|---|
| subcommand | `CM` / `WCC` | re-cluster vs. mincut-only |
| `--algorithm` (CM) | `leiden-cpm`, `leiden-mod`, `louvain` | required for CM |
| `--clustering-parameter` (CM) | `0.01` | resolution for leiden-cpm; **omit for leiden-mod** |
| `--min-batch-cost` | `1000` | bigger = fewer/larger worker batches = less comm overhead |
| `--connectedness-criterion` | `1log_10(n)` (default) | also `Cn^x`, `Clog_x(n)`, `piecewise` |
| `--mincut-type` | `cactus` | or `noi` |
| `--yield-node-threshold` | `100000` | split clusters ≥ this size and redistribute (0 = off) |
| `--num-processors` | `1` | threads per worker; must match sbatch `--cpus-per-task` |
| `--time-limit-per-cluster` | `-1` | abort a cluster after N seconds (-1 = none) |
| `--log-level` | `2` | 0 silent / 1 info / 2 debug |
| `--prune`, `--bypass-clique` | off | optional; `--drop-cluster-under N` filters tiny clusters |

## Step 4 — submit via Slurm

Wrap the binary so Slurm can forward SIGTERM for checkpointing. Use the reusable
wrapper in this skill's **`templates/dcm.sbatch`** — it runs the binary under
`mpirun`, traps SIGTERM, and reads `DCM_BIN` (set this to your built binary). Copy it
next to your study, then pass the CM/WCC subcommand + args after it, setting ranks and
memory on the command line:

```bash
sbatch --ntasks 64 --mem-per-cpu=32GB \
    --job-name=distcm.<net>.<clust>.w64 \
    --output=<slurm-reports>/%x.%j.out --error=<slurm-reports>/%x.%j.err \
    dcm.sbatch CM --edgelist ... --partitioned-clusters-dir .../clusters ...
```

- **Rank 0 is the load balancer** (1 CPU); ranks 1+ are workers. For threaded workers
  (`--num-processors > 1`) allocate `--cpus-per-task` to match; a heterogeneous job
  (`#SBATCH hetjob`, 1 CPU for rank 0, N CPUs per worker) is the efficient layout.
- See the `slurm-job` skill for partitions / time limits / memory sizing.

## Checkpoint / resume

On SIGTERM (wall limit), SIGABRT, or partial cluster failure, Dist-CM writes
`<work-dir>/checkpoint.csv`. **Re-run the same command with the same `--work-dir`** to
resume — it auto-detects the checkpoint. This is why each job gets its own `--work-dir`.

## After the run

Read runtimes and failures with the `analyze-dist-cm-results` skill (parses
`logs/load_balancer.log`). Record what you launched per the `record-keeping` skill,
and copy keepers off `/scratch` before the 30-day purge.
