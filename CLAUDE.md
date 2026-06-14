# Distributed-CM (Dist-CM)

Distributed implementation of **Connectivity Modifier (CM)** and **Well-Connected
Clusters (WCC)** using MPI. It takes a network + an existing clustering and
re-processes every cluster in parallel (mincut-based well-connectedness checking,
optionally re-clustering with Leiden/Louvain), then writes a new clustering.

This file orients you (and Claude) in the codebase. For step-by-step task help,
prefer the **skills** listed at the bottom — they encode the workflows and gotchas.

## The one thing to get right first: where to run

**Dist-CM creates an enormous number of small files** (one `.edgelist` + one
`.cluster` per cluster during partitioning, per-worker output dirs, per-cluster
logs). A single OCv1 run partitions into ~75k clusters → hundreds of thousands of
inodes. On the **UIUC campus cluster this will blow the inode/file-count quota on
`/home`/project space.**

➡️ **Always run experiments under `/scratch`** (`/scratch/<netid>/...`), which has
no inode limit. Caveat: `/scratch` is **auto-purged every ~30 days**, so copy
anything you want to keep (final clusterings, `load_balancer.log`s, result tables)
out to persistent storage when a study finishes. This is why paper-experiment work
dirs live under `/scratch` while kept logs (`past_reports/`) are copied out.

## Layout

```
src/main.cpp            # CLI parsing (argparse), MPI init, dispatch to LB/worker
src/load_balancer.cpp   # rank 0: partitions clusters, assigns batches, aggregates
src/worker.cpp          # rank 1+: process clusters in forked children (OOM/timeout safe)
includes/               # load_balancer.hpp, worker.hpp, logger.hpp, constants.hpp, utils.hpp
tools/                  # text <-> binary format converters (see "File formats")
external/constrained-clustering   # submodule: shared-memory CM + igraph/libleidenalg
CMakeLists.txt          # builds the MPI executable, links the submodule libs
easy_build_and_compile.sh / setup.sh
build/distributed_connectivity_modifier   # the binary (after building)
```

**Architecture:** rank 0 is the load balancer (partitions the work, hands batches to
workers by estimated cost, aggregates output). Ranks 1+ are workers that process
clusters in forked child processes so an OOM-kill or timeout on one cluster doesn't
take down the worker. With only 1 rank, rank 0 also acts as a worker.

## Building

```bash
git submodule update --init --recursive   # first time only
./setup.sh                                 # builds the constrained-clustering submodule
./easy_build_and_compile.sh                # builds the MPI binary -> build/distributed_connectivity_modifier
```

**Gotcha — SIGILL on the cluster.** The campus cluster is heterogeneous (different
CPU generations). A binary built with `-march=native` can emit newer instructions
(e.g. AVX-512) and then crash with **SIGILL / "Illegal instruction"** when the job
lands on an older node. Prefer a **general instruction set** over `native`:
`-march=x86-64` is a safe choice and is what the current build scripts use to avoid
this (watch the submodule's own `-march` too). You can also pin nodes with
`--constraint=intel` / `--constraint=AE7713` in sbatch. See the `build-dist-cm` skill.

## Running

Two subcommands: `CM` (re-clusters with `--algorithm`) and `WCC` (mincut-only
well-connectedness, no re-clustering). Both take the same common arguments.

```bash
mpirun -np <ranks> ./build/distributed_connectivity_modifier CM \
    --edgelist net.csv --existing-clustering init.csv --output-file out.csv \
    --algorithm leiden-cpm --clustering-parameter 0.01
```

### The partition → compute workflow (how every experiment is structured)

Partitioning the clustering into per-cluster files is expensive and depends only on
`(network, clustering, --min-batch-cost)` — **not** on worker count or criterion. So
experiments do it **once** and reuse it:

1. **Partition (1 MPI task, `--partition-only`):** writes `<work-dir>/clusters/`
   (`summary.csv` + `<id>.edgelist` + `<id>.cluster` per cluster).
2. **Compute (N MPI ranks, `--partitioned-clusters-dir <...>/clusters`):** skips
   partitioning, processes the pre-partitioned clusters. Run this many times across
   worker counts / criteria / repetitions, all pointing at the same partition.

A study is therefore typically two scripts: a numbered `NN_partition.sh` (submits the
1-task `--partition-only` jobs) and a `NN_run.sh` (submits the N-rank compute jobs that
point `--partitioned-clusters-dir` at the partitions). The `new-dist-cm-experiment`
skill scaffolds exactly this.

### Key arguments

Required (CM & WCC): `--edgelist`, `--existing-clustering`, `--output-file`.

| Argument | Default | Notes |
|---|---|---|
| `--work-dir` | `dcm-work-dir` | All intermediate + output state; enables checkpoint resume. Put under `/scratch`. |
| `--partition-only` | false | Phase 1: partition and stop. |
| `--partitioned-clusters-dir` | `<work-dir>/clusters` | Phase 2: reuse a prior partition, skip partitioning. |
| `--min-batch-cost` | 1.0 | Min estimated cost per worker batch. Higher = fewer, bigger batches = less communication. Experiments use 1000. |
| `--connectedness-criterion` | `1log_10(n)` | `Clog_x(n)`, `Cn^x`, or `piecewise`. n = cluster size. |
| `--algorithm` (CM only) | — | `leiden-cpm`, `leiden-mod`, or `louvain`. |
| `--clustering-parameter` (CM only) | 0.01 | Resolution for leiden-cpm. Omit for leiden-mod. |
| `--mincut-type` | `cactus` | `cactus` or `noi`. |
| `--num-processors` | 1 | Threads per worker for mincut. Must be matched by sbatch `--cpus-per-task`. |
| `--yield-node-threshold` | 0 (off) | Clusters with ≥ this many nodes are split and yielded back to the LB for redistribution. Experiments use 100000. |
| `--time-limit-per-cluster` | -1 (none) | Abort a cluster after N seconds. |
| `--prune` | false | Prune nodes via mincuts. |
| `--report-interval` | 10 | Worker→LB status report cadence (OOM/timeout/peak-mem). |
| `--log-level` | 1 | 0 silent / 1 info / 2 debug. Experiments use 2. |

Expert/finer-control: `--drop-cluster-under N`, `--bypass-clique`. Note
`--yield-node-threshold` is real but not yet in the upstream README.

## File formats

**Text** (CSV with header): edgelist `source,target`; clustering `node_id,cluster_id`.

**Binary** (used for large networks like OCv1 — much smaller + faster I/O):
- `.bedgelist`: `[uint64 num_edges][int32 source, int32 target] × num_edges`
- `.bcluster`: `[uint32 num_entries][int32 node_id, int32 cluster_id] × num_entries`

Convert with `tools/edgelist_to_binary.py` / `tools/cluster_to_binary.py` (and the
`binary_to_*` inverses). The `format-conversion` skill also covers this.

## Work-dir structure & reading results

```
<work-dir>/
├── checkpoint.csv      # written on SIGTERM/SIGABRT or partial failure
├── clusters/           # partitioned per-cluster files (summary.csv, <id>.edgelist, <id>.cluster)
├── logs/load_balancer.log     # <-- the headline log; runtimes & summary live here
├── logs/worker_<rank>.log
├── output/worker_<rank>/, worker_<rank>.out, bypass.out
├── history/  pending/
```

**Runtime** = time between these two `load_balancer.log` lines (the `(t=Ns)` stamps):
```
LoadBalancer runtime phase started
LoadBalancer runtime phase ended
```
The last line also reports `Worker report summary: X OOM kills, Y timeouts, peak
cluster memory Z MB`. The `analyze-dist-cm-results` skill extracts these.

Log line format: `[LEVEL][rank-...](t=<seconds>s) message`.

## Checkpointing

Run the job under Slurm with `--signal=B:TERM@90` so Slurm sends SIGTERM ~90s before
the wall limit; the program checkpoints to `<work-dir>/checkpoint.csv`. **Re-run with
the same `--work-dir` to resume.** Checkpoints are also written when clusters fail
(timeout/OOM).

## SLURM

The standard pattern is a small sbatch wrapper that runs the binary under
`mpirun --use-hwthread-cpus`, traps SIGTERM (forwarding it so the program can
checkpoint), and reads the binary path from a `DCM_BIN` env var (default
`build/distributed_connectivity_modifier`). The `run-dist-cm` skill ships this
wrapper. Workers can use threads via `--num-processors` — you must allocate the
matching `--cpus-per-task` yourself. Because rank 0 (LB) needs only 1 CPU while
workers need `num-processors` each, a **heterogeneous job** (`#SBATCH hetjob`) is the
efficient layout for threaded runs (see upstream README "Slurm Usage"). Pin
microarchitecture with `--constraint` to avoid SIGILL.

## Onboarding skills

- **build-dist-cm** — build/rebuild correctly (submodules, the `-march` SIGILL fix).
- **run-dist-cm** — run a CM/WCC experiment end to end (partition → compute, args, sbatch, resume).
- **analyze-dist-cm-results** — pull runtimes / OOM / peak-mem from logs, count clusters, build tables.
- **new-dist-cm-experiment** — scaffold a new experiment dir in the paper-experiments two-phase style.

Also bundled in-repo (codebase copies of the group's common skills, scripts included):
- **run-clustering** — produce an initial clustering (Leiden-Mod/CPM, IKC) to feed Dist-CM.
- **format-conversion** — text↔binary (`.bedgelist`/`.bcluster`) and delimiter/header conversion.
- **slurm-job** — generic Slurm batch submission (defaults: `secondary`, 4h cap, 16G).

Compose with **evaluate-clustering** (compare clusterings) and **record-keeping** too.
