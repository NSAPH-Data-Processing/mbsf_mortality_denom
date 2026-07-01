#!/bin/bash
#SBATCH --job-name=mbsf_mortality_denom
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G    
#SBATCH --time=01:00:00
#SBATCH --partition=large
#SBATCH --output=logs/slurm-%j.out
#SBATCH --error=logs/slurm-%j.err

# Activate environment


# Run Snakemake
snakemake --unlock
snakemake --cores 1 --rerun-incomplete