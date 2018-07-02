#!/bin/bash
#SBATCH --time=12:00:00
#SBATCH --nodes=3
#SBATCH --mail-user=jw983@jbs.cam.ac.uk
#SBATCH --mail-type=ALL
#SBATCH --mem=128G
module load python27-mpi4py/2.0.0
module load miniconda2
mpirun python pca.py