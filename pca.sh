#!/bin/bash
#SBATCH --time=12:00:00
#SBATCH --nodes=20
#SBATCH --mail-user=jw983@jbs.cam.ac.uk
#SBATCH --mail-type=ALL
module load intel/2018.2
module load openmpi/3.1.0
module load anaconda2
srun python pca.py