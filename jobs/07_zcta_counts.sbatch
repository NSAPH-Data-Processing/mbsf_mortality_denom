#!/bin/bash
#
#SBATCH -p large # partition (queue)
#SBATCH -c 2 # number of cores per job in the array
#SBATCH --mem 24GB # total memory in the array
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --array=0-43
#SBATCH -o logs/slurm/07_zcta_counts_%A_%a.out
#SBATCH -e logs/slurm/07_zcta_counts_%A_%a.err

mkdir -p logs/slurm logs/hydra/07_zcta_counts

YEARS=($(seq 2000 2021))
FREQS=(yearly daily)

# activate environment

YEAR_IDX=$(( SLURM_ARRAY_TASK_ID / 2 ))
FREQ_IDX=$(( SLURM_ARRAY_TASK_ID % 2 ))

YEAR=${YEARS[$YEAR_IDX]}
FREQ=${FREQS[$FREQ_IDX]}

echo "Running year=${YEAR}, freq=${FREQ}"

python src/07_zcta_counts.py \
  run.year=${YEAR} \
  run.temporal_frequency=${FREQ} \
  hydra.run.dir=logs/hydra/07_zcta_counts/${YEAR}_${FREQ}


