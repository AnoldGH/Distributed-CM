---
name: format-conversion
description: Convert graph files (edgelist, nodelist, cluster) between delimiters/headers, and convert text edgelists/clusterings to Dist-CM's compact binary .bedgelist/.bcluster formats (and back). Use when the user asks for format conversion, or to prepare binary inputs for Dist-CM.
---

# Format conversion

We work with three graph file types:
1. **edgelist** — columns `source,target` (an edge between the two nodes).
2. **nodelist** — column `node_id`.
3. **cluster** — columns `node_id,cluster_id` (which cluster each node is in).

Helper scripts are bundled in this skill's **`scripts/`** folder.

## Text conversions (delimiter / header)

Most conversions are between headerless TSVs and CSVs-with-headers. Use:

```
python scripts/conversion_toolkit.py <input> [-o OUTPUT] [-i] [-d {\t,\s,comma}] [--header H] [--remove-header]
```

- `-o` output path (`-i` / `--inplace` overwrites the input instead).
- `-d` target delimiter: `\t`, `\s` (space), or `comma`.
- `--header "col1,col2"` sets output headers; `--remove-header` strips them.
- The input delimiter and header presence are auto-detected.

Unless told otherwise, just convert when the file is one of the three types above.

## Binary conversions (for Dist-CM)

Large networks should be fed to Dist-CM in binary form — much smaller and faster I/O.
These tools are also bundled in `scripts/` (copies of the repo's `tools/`):

```
python scripts/edgelist_to_binary.py <input.edgelist> [output.bedgelist]
python scripts/cluster_to_binary.py  <input.cluster>  [output.bcluster]
python scripts/binary_to_edgelist.py <input.bedgelist> [output.edgelist]   # inverse
python scripts/binary_to_cluster.py  <input.bcluster>  [output.cluster]    # inverse
```

- Input: text edgelist/cluster (CSV/TSV/space, with header). Output extension defaults
  to `<input>.bedgelist` / `<input>.bcluster` if omitted.
- Binary layouts:
  - `.bedgelist`: `[uint64 num_edges][int32 source, int32 target] × num_edges`
  - `.bcluster`: `[uint32 num_entries][int32 node_id, int32 cluster_id] × num_entries`
- Node/edge IDs must fit in `int32`. Keep the edgelist and clustering in the **same**
  representation (both binary or both text) for a given Dist-CM run.

### Naming convention

For a network `<network>`:
- `<network>.bedgelist` (binary edgelist)
- `<network>.mod.bcluster`, `<network>.0.01.bcluster`, … (binary clustering, matching
  the text cluster file's name).

## Scale

For large graphs (~75M+ nodes) these conversions are memory/time-heavy — submit them
as Slurm jobs (`slurm-job`). Smaller graphs (<10M nodes) run fine interactively.
