import os
import os.path as path
import argparse
import yaml
import subprocess
from tempfile import NamedTemporaryFile

if 'JCMD' not in os.environ:
    raise ValueError("please specify the job command")
JCMD = os.environ['JCMD']

WHOAMI = os.environ.get('SELF', 'ttic')
if WHOAMI == "ttic":
    DEFAULT_PARTITION = 'greg-gpu,rc-own-gpu'
    GPU_REQ_FLAG = 'c'
    MISC_SBATCH_OPT = '-x gpu-g20,gpu-g26,gpu-g38'
    # EXCLUDE_NODES =
elif WHOAMI == 'autobot':
    DEFAULT_PARTITION = 'long,short'
    GPU_REQ_FLAG = 'G'
    MISC_SBATCH_OPT = ''
else:
    raise ValueError('unregistered machine identifier {}'.format(WHOAMI))

VALID_ACTS = ('run', 'cancel')
CANCEL_CMD = 'scancel -n {name}'
SBATCH_CMD = (
    'sbatch '
    '-d singleton '
    '--job-name={name} '
    '-p {partition} '
    '--nodes=1 -{GPU_REQ_FLAG} {num_cores} '
    '{misc} {extra} '
    '--output="{log}" '
    '--open-mode=append '
    '"{script}" '
)


def temp_sh_exec(command_closure, sh_content, num_runs, dummy=True):
    with NamedTemporaryFile(suffix='.sh') as sbatch_file:
        # this temp sh is compulsory. Write the explanatory comment tmr.
        sbatch_file.file.write(sh_content)
        sbatch_file.file.seek(0)
        sbatch_file_name = sbatch_file.name
        to_exec = command_closure(sbatch_file_name)
        for i in range(num_runs):
            print(to_exec)
            if not dummy:
                subprocess.run(to_exec, shell=True)
        print("")


def task_execute(
    task_dir, action, length, dummy, partition, num_cores, unknown
):
    os.chdir(task_dir)
    task_name = task_dir.split('/')[-1]
    slurm_out = path.join(task_dir, 'slurm.out')
    num_runs = int(length)
    extra = ' '.join(unknown)

    if action == 'run':
        sh_content = b'#!/usr/bin/env bash\n' + str.encode('{}\n'.format(JCMD))
        print(sh_content)
        cmd_closure = lambda sbatch_file_name:\
            SBATCH_CMD.format(
                name=task_name,
                partition=partition,
                GPU_REQ_FLAG=GPU_REQ_FLAG, num_cores=num_cores,
                misc=MISC_SBATCH_OPT, extra=extra,
                log=slurm_out, script=sbatch_file_name,
            )
        temp_sh_exec(cmd_closure, sh_content, num_runs, dummy)
    elif action == 'cancel':
        to_exec = CANCEL_CMD.format(name=task_name)
        print(to_exec)
        print("")
        if not dummy:
            subprocess.run(to_exec, shell=True)


def main():
    parser = argparse.ArgumentParser(description='slurm sbatch submit')
    parser.add_argument(
        '-f', '--file', type=str, required=True,
        help='a yaml containing a list of absolute paths to the job folders')
    parser.add_argument(
        '-a', '--action', default='run',
        help='one of {}, default {}'.format(VALID_ACTS, VALID_ACTS[0]))
    parser.add_argument(
        '-p', '--partition', default=DEFAULT_PARTITION, type=str,
        help='the partition the job is submitted to. default {}'.format(DEFAULT_PARTITION))
    parser.add_argument(
        '-n', '--num-cores', type=int, default=1,
        help='Number of cores to run the job. Overruled if job has a spec')
    parser.add_argument(
        '-l', '--length', type=int, default=1,
        help='the length of a series. Not fully implemented. See doc')
    parser.add_argument(
        '-d', '--dummy', default=False, action='store_true',
        help='in dummy mode the slurm command is printed but not executed')
    args, unknown = parser.parse_known_args()  # pass unknown to runner script

    print("submitting slurm jobs on {}".format(WHOAMI))
    if args.dummy:
        print("WARN: using dummy mode")
    print("Using partition {}".format(args.partition))
    if args.action not in VALID_ACTS:
        raise ValueError(
            "action must be one of {}, but given: {}".format(VALID_ACTS, args.action)
        )
    with open(args.file) as f:
        task_dir_list = yaml.safe_load(f)
    for task_dir in task_dir_list:
        if not path.isdir(task_dir):
            raise ValueError("{} is not a valid directory".format(task_dir))
        else:
            task_execute(
                task_dir, args.action, args.length, args.dummy,
                args.partition, args.num_cores, unknown
            )
