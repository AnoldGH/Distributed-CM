#!/usr/bin/env python3
"""Convert a binary .bcluster file back to CSV format.

Binary format:
    [4 bytes: uint32 num_entries]
    [8 bytes per entry: int32 node_id, int32 cluster_id] x num_entries

Usage:
    python binary_to_cluster.py input.bcluster output.cluster
    python binary_to_cluster.py input.bcluster              # writes input.cluster
"""

import argparse
import struct
from pathlib import Path


def convert(input_path: str, output_path: str) -> None:
    with open(input_path, "rb") as f:
        num_entries = struct.unpack("<I", f.read(4))[0]

        with open(output_path, "w") as out:
            out.write("node_id,cluster_id\n")
            for _ in range(num_entries):
                node_id, cluster_id = struct.unpack("<ii", f.read(8))
                out.write(f"{node_id},{cluster_id}\n")

    bin_size = Path(input_path).stat().st_size
    text_size = Path(output_path).stat().st_size

    print(f"Converted {num_entries} entries")
    print(f"  {input_path}: {bin_size:,} bytes")
    print(f"  {output_path}: {text_size:,} bytes")


def main():
    parser = argparse.ArgumentParser(description="Convert binary .bcluster to CSV cluster format")
    parser.add_argument("input", help="Input binary .bcluster file")
    parser.add_argument("output", nargs="?", default=None, help="Output CSV file (default: input with .cluster extension)")
    args = parser.parse_args()

    output = args.output
    if output is None:
        output = str(Path(args.input).with_suffix(".cluster"))

    convert(args.input, output)


if __name__ == "__main__":
    main()
