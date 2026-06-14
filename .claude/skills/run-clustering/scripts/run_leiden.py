import argparse
import leidenalg as la
import igraph as ig
import logging
import time
import pandas as pd


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


# TODO: Credits to DSC run_leiden.py and CM-pipeline scripts
def run_leiden(
    edgelist_path,
    output_path,
    model="cpm",
    resolution=None,
    n_iters=2,
    seed=1234,
):
    # Read in leiden
    start = time.perf_counter()

    delimiter = get_delimiter(edgelist_path)
    df = pd.read_csv(edgelist_path, sep=delimiter)
    g = ig.Graph.TupleList(
        df.itertuples(index=False), directed=False, vertex_name_attr="name"
    )

    elapsed = time.perf_counter() - start
    logging.info(f"[TIME] Loading network: {elapsed}")

    # Running leiden
    start = time.perf_counter()

    if model == "cpm":
        partition = la.find_partition(
            g,
            la.CPMVertexPartition,
            resolution_parameter=resolution,
            seed=seed,
            n_iterations=n_iters,
        )
    elif model == "mod":
        partition = la.find_partition(
            g, la.ModularityVertexPartition, seed=seed, n_iterations=n_iters
        )
    else:
        raise ValueError(f"Unknown leiden model: {model}")

    elapsed = time.perf_counter() - start
    logging.info(f"[TIME] Running Leiden algorithm: {elapsed}")

    # Output
    start = time.perf_counter()

    df2 = pd.DataFrame(
        {
            "node_id": [g.vs[i]["name"] for i in g.vs.indices],
            "cluster_id": partition.membership,
        }
    )
    df2.to_csv(
        output_path, index=False, sep=delimiter, header=True
    )  # TODO: file format

    elapsed = time.perf_counter() - start
    logging.info(f"[TIME] Saving results: {elapsed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Leiden-specific parameters
    parser.add_argument(
        "-e",
        "--edgelist",
        metavar="edgelist",
        type=str,
        required=True,
        help="Input network edgelist path",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="output",
        type=str,
        default="leiden_output.csv",
        help="Output path",
    )
    parser.add_argument(
        "-m",
        "--model",
        metavar="model",
        type=str,
        default="cpm",
        help="Model to use for clustering",
    )

    parser.add_argument(
        "-r",
        "--resolution",
        metavar="resolution",
        type=float,
        default=None,
        help="Resolution parameter for the CPM model (default: None)",
    )

    parser.add_argument(
        "-n",
        "--n-iterations",
        metavar="n_iteration",
        type=int,
        help="Number of iterations for the Leiden algorithm (default: 2)",
        default=2,
    )

    # Pipeline-specific parameters
    parser.add_argument(
        "-s",
        "--seed",
        metavar="seed",
        type=int,
        default=1234,
        help="Seed used for execution",
    )

    args = parser.parse_args()

    run_leiden(
        args.edgelist,
        args.output,
        args.model,
        args.resolution,
        args.n_iterations,
        args.seed,
    )
