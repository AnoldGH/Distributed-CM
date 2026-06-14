---
name: run-clustering
description: Run a community-detection algorithm (Leiden-Mod, Leiden-CPM, or IKC) on a network edgelist to produce an initial clustering. Use when the user needs a clustering of a network — e.g. the input clustering that Dist-CM will then refine.
---

# Running clustering

Produces an initial clustering of a network, typically the `--existing-clustering`
input that Dist-CM (`run-dist-cm`) refines. Input is an **edgelist** — CSV/TSV/space
with a `source,target` header; each row is an edge between `source` and `target`.

The helper scripts are bundled in this skill's **`scripts/`** folder:

| Algorithm | Script | Usage |
|---|---|---|
| Leiden-Mod / Leiden-CPM | `scripts/run_leiden.py` | `run_leiden.py -e <edgelist> [-o out] [-m mod\|cpm] [-r resolution] [-n n_iter] [-s seed]` |
| IKC | `scripts/run_ikc.py` | `run_ikc.py -e <edgelist> -o <out> [-k kvalue] [-q] [-v]` |

- `-m` accepts `mod` (modularity) or `cpm` (CPM, needs `-r <resolution>`).
- **Leave optional parameters empty unless the user specifies them** so defaults apply
  (e.g. Leiden `n_iterations` defaults to 2). Only set what's requested.

## Output naming

For an edgelist named `<network>`:
- Leiden-Mod → `<network>.mod.cluster`
- Leiden-CPM(r) → `<network>.<r>.cluster`  (e.g. `<network>.0.01.cluster`)
- IKC → `<network>.ikc.cluster`

Output is a CSV with header `node_id,cluster_id`.

## Dependencies & scale

- `run_leiden.py` needs `leidenalg`, `igraph`, `pandas`; `run_ikc.py` needs
  `networkit`. Use the group's existing Python env.
- For large graphs, run as a Slurm batch job (see `slurm-job`); small graphs
  (≲ a few M nodes) can run interactively.

## Next steps for Dist-CM

Convert the resulting clustering (and the edgelist) to binary `.bcluster`/`.bedgelist`
with `format-conversion` for large networks, then refine it with `run-dist-cm`. Record
what you produced per `record-keeping`.
