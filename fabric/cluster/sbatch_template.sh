#!/usr/bin/env bash

#SBATCH --job-name={jname}
#SBATCH -d singleton

#SBATCH --partition={partition}
#SBATCH -G {num_gpus}
#SBATCH -c {num_cpus}

#SBATCH --output={log_fname}
#SBATCH --open-mode=append

#SBATCH --export=ALL,IS_REMOTE=1

#SBATCH {extra}

echo --------
echo SLURM NODENAME: $SLURMD_NODENAME
echo --------

cd {task_dirname}

{job_cmd}
