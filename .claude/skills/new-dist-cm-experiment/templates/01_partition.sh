#!/bin/bash
# Phase 1 — partition each (network, clustering) ONCE with 1 MPI task.
# The partition depends only on (network, clustering, min-batch-cost), so every
# compute run in 02_<sweep>.sh reuses it via --partitioned-clusters-dir.
#
# Edit: the networks / clustering lists, DATASET_DIR, mem-per-cpu, batch_size.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Dataset path — override via env var so others can point at their own data.
# Keep this under /scratch (no inode limit; auto-purged ~30 days).
DATASET_DIR="${DATASET_DIR:-/scratch/<netid>/datasets/<study>}"

SLURM_REPORTS="${SCRIPT_DIR}/slurm-reports"; mkdir -p "${SLURM_REPORTS}"

networks=("netA" "netB")
cluster_suffixes=("mod" "0.01")            # filename component of the clustering file
partition_names=("mod" "leiden.01")        # short name used in job ids / dirs
algo_args_list=(
    "--algorithm leiden-mod"
    "--algorithm leiden-cpm --clustering-parameter 0.01"
)
batch_size=1000

for network in "${networks[@]}"; do
    for i in "${!cluster_suffixes[@]}"; do
        suffix="${cluster_suffixes[$i]}"
        pname="${partition_names[$i]}"
        algo_args="${algo_args_list[$i]}"

        job_id="partition.${network}.${pname}.batch${batch_size}"
        echo "Submitting ${job_id}"

        # Partitioning is memory-hungry: raise --mem-per-cpu (32-450GB) for big networks.
        sbatch --ntasks 1 --mem-per-cpu=32GB \
            --job-name="${job_id}" \
            --output="${SLURM_REPORTS}/${job_id}.%j.out" \
            --error="${SLURM_REPORTS}/${job_id}.%j.err" \
            "${SCRIPT_DIR}/dcm.sbatch" CM \
            --edgelist "${DATASET_DIR}/${network}.bedgelist" \
            --existing-clustering "${DATASET_DIR}/${network}.${suffix}.bcluster" \
            --output-file "${SCRIPT_DIR}/${job_id}.out" \
            --work-dir "${SCRIPT_DIR}/work-partition/${job_id}" \
            $algo_args \
            --min-batch-cost ${batch_size} \
            --log-level 2 \
            --partition-only
    done
done
