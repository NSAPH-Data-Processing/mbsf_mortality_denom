#!/bin/bash
#SBATCH -p large # partition (queue)
#SBATCH -c 8 # number of cores per job in the array
#SBATCH --mem 96GB # total memory in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)
#SBATCH --array=0-2
#SBATCH -o logs/slurm/04_fix_enroll_zip_%A_%a.out
#SBATCH -e logs/slurm/04_fix_enroll_zip_%A_%a.err

mkdir -p logs/slurm logs/hydra/04_fix_enroll_zip

# activate environment

YEARS=(2000 2001 2002)
YEAR=${YEARS[$SLURM_ARRAY_TASK_ID]}

python src/04_fix_enroll_zip.py \
  run.year=${YEAR} \
  hydra.run.dir=logs/hydra/04_fix_enroll_zip/${YEAR}
