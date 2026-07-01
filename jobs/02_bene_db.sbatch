#!/bin/bash
#SBATCH -p large # partition (queue)
#SBATCH -c 8 # number of cores per job in the array
#SBATCH --mem 96GB # total memory in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)
#SBATCH -o logs/slurm/02_bene_db_%j.out
#SBATCH -e logs/slurm/02_bene_db_%j.err

# activate environment

mkdir -p logs/slurm logs/hydra/02_bene_db

python src/02_bene_db.py \
  hydra.run.dir=logs/hydra/02_bene_db