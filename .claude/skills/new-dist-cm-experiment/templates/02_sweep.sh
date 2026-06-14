#!/bin/bash
# Phase 2 — compute. Reuse the partitions from 01_partition.sh and sweep ONE
# variable. This example sweeps worker count (a scaling study); swap the inner
# loop for a different sweep (see the bottom of this file).
#
# Edit: networks / clustering lists, the swept variable, DATASET_DIR, resources.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET_DIR="${DATASET_DIR:-/scratch/<netid>/datasets/<study>}"

output_dir="${SCRIPT_DIR}/output"
work_dir="${SCRIPT_DIR}/work"
SLURM_REPORTS="${SCRIPT_DIR}/slurm-reports"
mkdir -p "${output_dir}" "${work_dir}" "${SLURM_REPORTS}"

networks=("netA" "netB")
cluster_suffixes=("mod" "0.01")
partition_names=("mod" "leiden.01")
algorithms=("leiden-mod" "leiden-cpm")     # used by CM only
clustering_params=("" "0.01")              # empty for leiden-mod

methods=("CM" "WCC")                        # WCC drops --algorithm/--clustering-parameter
worker_counts=(4 8 16 32 64)                # <-- the swept variable
batch_size=1000
repetitions=1

for network in "${networks[@]}"; do
    for method in "${methods[@]}"; do
        m=$(echo "$method" | tr '[:upper:]' '[:lower:]')
        for i in "${!cluster_suffixes[@]}"; do
            suffix="${cluster_suffixes[$i]}"
            pname="${partition_names[$i]}"

            # CM needs --algorithm (+ --clustering-parameter for cpm); WCC needs neither.
            algo_args=""
            if [[ "$method" == "CM" ]]; then
                algo_args="--algorithm ${algorithms[$i]}"
                [[ -n "${clustering_params[$i]}" ]] && algo_args+=" --clustering-parameter ${clustering_params[$i]}"
            fi

            partitioned_dir="${SCRIPT_DIR}/work-partition/partition.${network}.${pname}.batch${batch_size}/clusters"

            for workers in "${worker_counts[@]}"; do
                for rep in $(seq 1 $repetitions); do
                    job_id="${m}.${network}.${pname}.w${workers}.r${rep}"

                    sbatch --ntasks ${workers} --mem-per-cpu=32GB \
                        --job-name="${job_id}" \
                        --output="${SLURM_REPORTS}/${job_id}.%j.out" \
                        --error="${SLURM_REPORTS}/${job_id}.%j.err" \
                        "${SCRIPT_DIR}/dcm.sbatch" ${method} \
                        --edgelist "${DATASET_DIR}/${network}.bedgelist" \
                        --existing-clustering "${DATASET_DIR}/${network}.${suffix}.bcluster" \
                        --output-file "${output_dir}/${job_id}.out" \
                        --work-dir "${work_dir}/${job_id}" \
                        $algo_args \
                        --partitioned-clusters-dir "${partitioned_dir}" \
                        --min-batch-cost ${batch_size} \
                        --yield-node-threshold 100000 \
                        --mincut-type cactus \
                        --log-level 2
                done
            done
        done
    done
done

# ---------------------------------------------------------------------------
# Swapping the sweep (keep the rest of the structure):
#   batch-size study : loop batch_sizes=(1000 10000 100000); set --min-batch-cost
#                      ${batch_size} AND use a partition built with that batch size.
#   criterion study  : loop "0.2n^0.5" "1log_10(n)" "piecewise"; add
#                      --connectedness-criterion "${criterion}".
#   comparison study : fix workers=64; vary networks/methods (and add baselines).
# ---------------------------------------------------------------------------
