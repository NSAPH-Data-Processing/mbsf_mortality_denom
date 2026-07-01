#!/bin/bash
#
#SBATCH -p large # partition (queue)
#SBATCH -c 4 # number of cores per job in the array
#SBATCH --mem 64GB # total memory in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)

# activate environment

# Run your script 
python utils/bene_xwalk.py 