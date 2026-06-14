#!/bin/bash
#SBATCH --job-name=<short_descriptive_name>
#SBATCH --partition=secondary
#SBATCH --time=04:00:00
#SBATCH --mem=16G
#SBATCH --output=<output_dir>/<jobname>_%j.log
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=<netid>@illinois.edu

<command>
