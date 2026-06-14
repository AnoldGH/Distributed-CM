import argparse
import pandas as pd
from pathlib import Path
import csv
from enum import Enum


# Filetype defined in the spec
class FileType(Enum):
    NODELIST = 1
    EDGELIST = 2
    CLUSTER = 3


def get_delimiter(filepath: str) -> str:
    with open(filepath, "r") as f:
        for current_line in f:
            if "," in current_line:
                return ","
            elif " " in current_line:
                return " "
            elif "\t" in current_line:
                return "\t"
            else:
                raise ValueError(
                    "Unsupported delimiter: delimiter must be either comma, tab, or whitespace."
                )


def convert(
    input, output, src_delimiter, tar_delimiter, tar_header, src_has_header=True
):
    print(
        f"Converting {input} to {output} with delimiter {tar_delimiter} and header {tar_header}"
    )
    Path(output).parent.mkdir(exist_ok=True, parents=True)
    # read input preserving first-row data when the source has no header
    read_header = 0 if src_has_header else None
    pd.read_csv(input, sep=src_delimiter, header=read_header).to_csv(
        output, index=False, sep=tar_delimiter, header=tar_header
    )
    print(f"Conversion completed")


def convert_to(input, output, tar_delimiter, tar_header, src_has_header=True):
    print(
        f"Converting {input} to {output} with delimiter {tar_delimiter} and header {tar_header}"
    )
    src_delimiter = get_delimiter(input)
    Path(output).parent.mkdir(exist_ok=True, parents=True)
    read_header = 0 if src_has_header else None
    pd.read_csv(input, sep=src_delimiter, header=read_header).to_csv(
        output, index=False, sep=tar_delimiter, header=tar_header
    )
    print(f"Conversion completed")


# TODO: for now this assumes the file is "mostly correct."
def convert_to_canonical(input, output, filetype: FileType):
    if filetype == FileType.NODELIST:
        convert_to(input, output, ",", ["node_id"])
    elif filetype == FileType.EDGELIST:
        convert_to(input, output, ",", ["source", "target"])
    elif filetype == FileType.CLUSTER:
        convert_to(input, output, ",", ["node_id", "cluster_id"])


def check_header(filename, delimiter):
    with open(filename, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        first_row = next(reader, [])

        # detect header
        has_header = any(not cell.isdigit() for cell in first_row)
        return has_header, first_row


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate and process a character-delimited file, converting it to use a specified delimiter"
    )

    parser.add_argument("input", help="Path to the input character-delimited file")
    parser.add_argument("-o", "--output", default=None, help="Path to the output file")
    parser.add_argument(
        "-i",
        "--inplace",
        action="store_true",
        help="If specified, the input file will be changed directly",
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        default=None,
        choices=["\\t", "\\s", "comma"],
        help="Target delimiter to use. When specified, the input file will be converted to use that delimiter",
    )
    parser.add_argument(
        "--header",
        type=lambda x: x.split(","),
        help='Headers to use for the output file. If not specified, the headers will be inferred from the input file. The headers should be separated by commas. E.g. --header "col1,col2,col3"',
    )
    parser.add_argument(
        "--remove-header",
        action="store_true",
        help="Remove headers in the target file",
    )

    args = parser.parse_args()

    # Argument validation
    if args.header is not None:
        if args.remove_header:
            parser.error("Cannot specify both --headers and --remove-header")
        if len(args.header) != len(set(args.header)):
            parser.error("Headers must be unique")

    # Inplace
    if args.inplace:
        args.output = args.input

    # Get delimiter
    delimiter = get_delimiter(args.input)
    if args.delimiter == "\\t":
        args.delimiter = "\t"
    elif args.delimiter == "\\s":
        args.delimiter = " "
    elif args.delimiter == "comma":
        args.delimiter = ","

    # Get header
    has_header, headers = check_header(args.input, delimiter)

    if args.header is not None and len(args.header) != len(headers):
        parser.error("Number of headers must match number of columns in the input file")

    # Conversion
    if args.delimiter is not None or args.remove_header:
        if args.delimiter == delimiter or args.delimiter is None:
            print(f"No need to change delimiter")

        # TODO: when the input doesn't have headers but need to append
        if args.output is not None:
            convert(
                args.input,
                args.output,
                delimiter,
                args.delimiter,
                (
                    None
                    if args.remove_header
                    else headers if args.header is None else args.header
                ),
                src_has_header=has_header,
            )
