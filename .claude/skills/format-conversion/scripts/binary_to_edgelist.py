#!/usr/bin/env python3
"""Convert a binary .bedgelist file back to CSV format.

Binary format:
    [8 bytes: uint64 num_edges]
    [8 bytes per edge: int32 source, int32 target] x num_edges

Usage:
    python binary_to_edgelist.py input.bedgelist output.edgelist
    python binary_to_edgelist.py input.bedgelist              # writes input.edgelist
"""

import argparse
import struct
from pathlib import Path


def convert(input_path: str, output_path: str) -> None:
    with open(input_path, "rb") as f:
        num_edges = struct.unpack("<Q", f.read(8))[0]

        with open(output_path, "w") as out:
            out.write("source,target\n")
            for _ in range(num_edges):
                src, tgt = struct.unpack("<ii", f.read(8))
                out.write(f"{src},{tgt}\n")

    bin_size = Path(input_path).stat().st_size
    text_size = Path(output_path).stat().st_size

    print(f"Converted {num_edges} edges")
    print(f"  {input_path}: {bin_size:,} bytes")
    print(f"  {output_path}: {text_size:,} bytes")


def main():
    parser = argparse.ArgumentParser(description="Convert binary .bedgelist to CSV edgelist format")
    parser.add_argument("input", help="Input binary .bedgelist file")
    parser.add_argument("output", nargs="?", default=None, help="Output CSV file (default: input with .edgelist extension)")
    args = parser.parse_args()

    output = args.output
    if output is None:
        output = str(Path(args.input).with_suffix(".edgelist"))

    convert(args.input, output)


if __name__ == "__main__":
    main()
