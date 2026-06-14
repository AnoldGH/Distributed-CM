---
name: analyze-dist-cm-results
description: Extract runtimes, failure counts, and peak memory from Distributed-CM run logs, count output clusters, assemble scaling/comparison tables, and debug failed runs. Use when the user asks how long a Dist-CM run took, whether it succeeded, why it failed, or to summarize/tabulate Dist-CM results.
---

# Analyzing Dist-CM results

For a **successful run**, the headline numbers come from rank 0's log:
`<work-dir>/logs/load_balancer.log`. For **debugging a failure** (some clusters
aborted, a hang, wrong output), `load_balancer.log` is *not* enough — see
[Debugging & the full logging structure](#debugging) below.

> **These logs get big** (hundreds of thousands of DEBUG lines on large runs). Never
> read a log file whole — use `grep`, `head`, `tail`, `wc -l`. Line format is
> `[LEVEL][days-h:m:s](t=<seconds>s) message`; `t=` is seconds since the LB started.

## Runtime

The compute-phase wall time is bracketed by these two lines:

```
LoadBalancer runtime phase started
LoadBalancer runtime phase ended
```

```bash
grep -E "runtime phase (started|ended)" logs/load_balancer.log
```

The `(t=Ns)` on the `ended` line is the runtime in seconds. If the run *also*
partitioned (no `--partitioned-clusters-dir`), partitioning happens before
`runtime phase started`, so this number is compute-only — get partition time from the
gap before "Loaded N clusters …".

## Success / failure & peak memory

```bash
grep "Worker report summary" logs/load_balancer.log
# -> "Worker report summary: X OOM kills, Y timeouts, peak cluster memory Z MB"
```

Clean run = **0 OOM kills, 0 timeouts**. Non-zero ⇒ clusters were aborted, a
checkpoint was written, and the output clustering is **incomplete** until you resume
(re-run with the same `--work-dir`, see `run-dist-cm`). Caveat: this summary is
*best-effort* (piggybacked worker reports) and can undercount if a worker died — for
ground truth on what failed and why, go to the worker / per-cluster logs ([Debugging](#debugging)).
Also check the Slurm `.err` file for SIGILL (architecture mismatch — see `build-dist-cm`)
or OOM at the job level.

## Cluster counts

```bash
grep -E "Loaded [0-9]+ clusters" logs/load_balancer.log          # input clusters
tail -n +2 <output-file>.out | cut -d, -f2 | sort -u | wc -l     # output communities
```

Compare an output clustering against the input or ground truth with the
`evaluate-clustering` skill.

## Tables across runs

Studies sweep one variable (worker count, `--min-batch-cost`, criterion, method,
network) and name logs accordingly. Pull runtime + summary from each:

```bash
for f in *.load_balancer.log; do
  rt=$(grep "runtime phase ended" "$f" | grep -oE 't=[0-9]+s' | tail -1)
  summary=$(grep "Worker report summary" "$f" | sed 's/.*summary: //')
  printf "%-50s %8s   %s\n" "$f" "${rt:-NA}" "${summary:-no-summary}"
done
```

- **Scaling study:** tabulate runtime vs. worker count; report speedup
  `t(baseline)/t(w)` and parallel efficiency.
- **Comparison study:** line up Dist-CM/WCC vs. baselines at a *matched core budget*.

Present as a markdown table and flag any run with non-zero OOM/timeout as unreliable.

## Debugging

When a run fails, hangs, or produces wrong output, `load_balancer.log` alone won't
tell you which cluster broke or why. **Read the companion reference
[`debugging-and-logs.md`](debugging-and-logs.md)** in this skill folder — it documents
the full Dist-CM logging tree (worker logs, per-cluster CM/WCC logs, output/history
dirs), how to trace a single failing cluster, and the common failure signatures
(OOM-kill signal, timeout, SIGILL, hang, missing tail from unflushed logs). Open it
only when debugging; for routine runtime/summary extraction the sections above suffice.

## Record it

Save tables/findings per the `record-keeping` skill, and copy source logs off
`/scratch` before the ~30-day purge if they back a paper result.
