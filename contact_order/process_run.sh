#!/bin/bash
#SBATCH --account=plglscclass24-cpu
#SBATCH --partition=plgrid
#SBATCH --job-name=contact_order
#SBATCH --output=contact_order.log
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=4GB
#SBATCH --time=10:00:00

module load python

python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install "dask[distributed]" --upgrade

python contact_order.py