#!/bin/bash
#SBATCH -p large # partition (queue)
#SBATCH -c 8 # number of cores per job in the array
#SBATCH --mem 64GB # total memory in the array
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --array=0-21
#SBATCH -o logs/slurm/03_prep_bene_%A_%a.out
#SBATCH -e logs/slurm/03_prep_bene_%A_%a.err

mkdir -p logs/slurm logs/hydra/03_prep_bene

# activate environment

YEARS=($(seq 2000 2021))
YEAR=${YEARS[$SLURM_ARRAY_TASK_ID]}

python src/03_prep_bene.py \
  run.year=${YEAR} \
  hydra.run.dir=logs/hydra/03_prep_bene/${YEAR}
