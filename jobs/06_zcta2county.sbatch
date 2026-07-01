#!/bin/bash
#SBATCH -p large # partition (queue)
#SBATCH -c 4 # number of cores per job in the array
#SBATCH --mem 64GB # total memory in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)
#SBATCH --array=0-21
#SBATCH -o logs/slurm/06_zcta2county_%A_%a.out
#SBATCH -e logs/slurm/06_zcta2county_%A_%a.err

mkdir -p logs/slurm logs/hydra/06_zcta2county

# activate environment

YEARS=($(seq 2000 2021))
YEAR=${YEARS[$SLURM_ARRAY_TASK_ID]}

python src/06_zcta2county.py \
  run.year=${YEAR} \
  hydra.run.dir=logs/hydra/06_zcta2county/${YEAR}

# cleanup DuckDB file
rm -f "./data/output/mbsf_mortality_denom/denom_${YEAR}.db"