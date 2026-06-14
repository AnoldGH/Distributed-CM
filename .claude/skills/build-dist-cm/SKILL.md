---
name: build-dist-cm
description: Build or rebuild the Distributed-CM MPI binary — submodules, the constrained-clustering dependency, and choosing a portable instruction set so the binary doesn't SIGILL on the heterogeneous campus cluster. Use when asked to build/rebuild/compile Dist-CM, or when a Dist-CM job dies with SIGILL / "Illegal instruction".
---

# Building Dist-CM

Run from the repository root. Produces the MPI binary
`build/distributed_connectivity_modifier`.

## Steps

```bash
git submodule update --init --recursive   # first time only — pulls external/constrained-clustering
./setup.sh                                 # builds the submodule (shared-memory CM + igraph/libleidenalg)
./easy_build_and_compile.sh                # builds the MPI binary
```

`easy_build_and_compile.sh` wipes `build/`, configures CMake in **Release**, copies
`compile_commands.json` to the repo root, and runs `make -j 4`. The binary lands at
`build/distributed_connectivity_modifier`. The shared-memory baseline used in
comparison studies is built by `setup.sh` at
`external/constrained-clustering/build/bin/constrained_clustering`.

## Heterogeneous cluster → avoid `-march=native` (SIGILL)

The campus cluster is **heterogeneous**: a job may land on a CPU older than the node
you built on. A binary compiled with `-march=native` can emit newer instructions
(e.g. AVX-512) and then die with **SIGILL / "Illegal instruction"** on an older node.

Advice: build with a **general instruction set** rather than `native`. Passing
`-march=x86-64` (in `CMAKE_CXX_FLAGS`) is a safe, portable choice and is what the
current build scripts use to resolve this. Watch out for the *submodule* too — if it
configures its own `-march=native`, override it to the general ISA as well so the
whole binary stays portable.

If a user hits SIGILL at runtime:
1. Check what `-march` the binary (and submodule) was built with — `native` is the
   usual culprit.
2. Rebuild from a clean `build/` with a general ISA (`-march=x86-64`).
3. As reinforcement, pin the microarchitecture in the Slurm script with
   `--constraint=intel` (or a specific node type such as `--constraint=AE7713`) so the
   run target matches the build target.

## Notes

- Building is occasional and not inode-heavy — run it **interactively**. Only
  experiment *runs* need `/scratch` (see `run-dist-cm`).
- Sanity-check after building by running a tiny CM job on a toy network, or just
  confirm `build/distributed_connectivity_modifier` exists and is non-empty.
- Requires an MPI toolchain (`module load openmpi`) and CMake ≥ 3.23.
- Log the build — and any SIGILL fix — per the `record-keeping` skill.
