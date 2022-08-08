#!/usr/bin/env bash

#SBATCH --job-name={jname}
#SBATCH -d singleton

#SBATCH --partition={partition}
#SBATCH -c {num_devices}

#SBATCH --output={log_fname}
#SBATCH --open-mode=append

#SBATCH --export=ALL,IS_REMOTE=1

#SBATCH {extra}

cd {task_dirname}

{job_cmd}
