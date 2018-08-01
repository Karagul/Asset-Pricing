#!/bin/bash
#SBATCH --time=5:00:00
#SBATCH --ntasks=10
#SBATCH --mail-user=jw983@jbs.cam.ac.uk
#SBATCH --mail-type=ALL
#SBATCH --mem-per-cpu=20G
module load python27-mpi4py/2.0.0
module load miniconda2
mpirun python capm.py