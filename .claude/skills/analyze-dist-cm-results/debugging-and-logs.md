# Dist-CM logging structure & debugging

Open this when a run **fails, hangs, or gives wrong output**. For routine
runtime/summary extraction, the main SKILL.md is enough.

All paths are under the run's `--work-dir`. Every log line is
`[LEVEL][days-h:m:s](t=<seconds>s) message`. Levels: `INFO`, `DEBUG`, `ERROR`.
`--log-level 1` → INFO; `--log-level 2` → DEBUG (the CLI value is decremented
internally: INFO=0, DEBUG=1, ERROR=-1). **Logs flush only every ~10 writes**, so on a
hard crash the last few lines of a file may be missing — don't trust the absence of a
final line as proof of where it stopped.

## The logging tree

```
<work-dir>/
├── logs/
│   ├── load_balancer.log              # rank 0: orchestration (see main SKILL.md)
│   ├── worker_<rank>.log              # rank 1+: one per worker — per-cluster lifecycle
│   └── clusters/<cluster_id>.log      # the CM/WCC library's own log for ONE cluster
├── output/
│   ├── worker_<rank>/<cluster_id>.output   # per-cluster result (pre-aggregation)
│   ├── worker_<rank>.out                   # aggregated per-worker output
│   └── bypass.out                          # clusters accepted without processing (e.g. cliques)
├── history/worker_<rank>/<cluster_id>.hist # CM history per cluster
├── pending/                                # in-flight cluster markers
└── checkpoint.csv                          # written on SIGTERM/SIGABRT or partial failure
```

**Three logging layers, narrowing scope:**
1. `load_balancer.log` — *global* orchestration: args echoed at startup, clusters
   loaded, batches assigned to workers, completions/aborts, aggregation, final
   `Worker report summary`. Best-effort failure counts (workers piggyback reports).
2. `worker_<rank>.log` — *per worker*: each cluster it requested, received, completed
   or aborted, the forked child's outcome, and per-cluster peak memory. This is the
   layer that knows **which cluster failed and how**.
3. `logs/clusters/<cluster_id>.log` — *per cluster*: the internal CM / MincutOnly
   (WCC) log from the constrained-clustering library (a separate logging system).
   This is where the **algorithmic detail / crash reason for one cluster** lives.

Note: the LB summary can undercount failures if a worker died before reporting. For
ground truth, grep the worker logs directly (below).

## Tracing a failure

**1. Did clusters abort, and which?** Scan worker logs (not the LB):
```bash
grep -h "Aborted cluster" logs/worker_*.log        # which clusters failed
grep -h -E "killed by signal|Timeout|exited with code|Fork failed" logs/worker_*.log
```
Failure signatures in `worker_<rank>.log`:
- `Child killed by signal: 9` → the cluster's child was **OOM-killed** (signal 9).
  Feeds the LB's "OOM kills" count. Fix: more memory per task, or smaller batches.
- `Timeout. Child was killed after N seconds` → exceeded `--time-limit-per-cluster`.
- `Child exited with code: N` (N≠0) → the CM/WCC library errored on that cluster.
- `Fork failed` → serious; the worker couldn't spawn the child (resource exhaustion).
- `Cluster <id> peak memory: <M> MB` → per-cluster RSS; find the memory hog.

**2. Why did *that* cluster fail?** Open its per-cluster log:
```bash
tail -50 logs/clusters/<cluster_id>.log
```
This has the CM/Leiden/mincut internal trace for just that cluster. Pair it with the
cluster's input under the partition dir (`clusters/<cluster_id>.edgelist` /
`.cluster`) to reproduce in isolation.

**3. Job-level death (whole run gone).** Check the Slurm `.err`/`.out`:
- **SIGILL / "Illegal instruction"** → binary built for a newer CPU than the node it
  landed on. Rebuild with a general ISA and/or pin `--constraint`; see `build-dist-cm`.
- Job-level OOM / time limit → Slurm killed the step. If `--signal=B:TERM@90` was set,
  a `checkpoint.csv` exists — resume with the same `--work-dir`.

**4. Hang (job runs but never finishes).** Tail the LB and the workers:
```bash
tail -20 logs/load_balancer.log
for f in logs/worker_*.log; do echo "== $f"; tail -3 "$f"; done
```
A worker stuck mid-cluster shows `Received cluster X …` with no matching
`Completed/Aborted cluster X`; cross-reference `logs/clusters/X.log`. A known hang
mode is a malformed/empty per-cluster input mmap-ing to a sentinel — confirm the
cluster's `.edgelist` under the partition dir is well-formed.

## Resuming after a partial failure

Any abort writes `checkpoint.csv`. Re-running the **same command with the same
`--work-dir`** auto-detects it and reprocesses only the unfinished clusters. After a
clean resume, the `Worker report summary` should show 0/0 and the output is complete.
