#!/bin/bash
#SBATCH --time=12:00:00
#SBATCH --nodes=20
module load intel/2018.2
module load openmpi/3.1.0
module load anaconda2
python test.py