import os.path as path
import argparse
import yaml
from string import Template
import subprocess
from tempfile import NamedTemporaryFile


VALID_CMDS = ('run', 'cancel')
DEFAULT_PARTITION = 'greg-gpu,rc-own-gpu'
CANCEL_CMD = Template('scancel -n $name')
SMIT_CMD = Template(
    'sbatch '
    '-d singleton '
    '--nodes=1 -p $partition -c${num_cores} $feature $machine '
    '--output="$log" '
    '--open-mode=append '
    '--job-name=$name '
    '"$script" '
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
    task_dir, command, length, dummy, partition, num_cores, features, unknown
):
    task_name = task_dir.split('/')[-1]
    slurm_out = path.join(task_dir, 'slurm.out')
    run_script = path.join(task_dir, 'run.py')
    num_runs = int(length)
    # bash: must use single quote to escape input like 1080ti|titanx
    features = '-C \'{}\''.format(features) if features else ''
    machine = '-x gpu-g20,gpu-g26,gpu-g38'  # if partition == 'rc-own-gpu' else ''
    params = '' if unknown is None else ' '.join(unknown)

    if command == 'run':
        if not path.isfile(run_script):
            raise ValueError("{} is not a valid file".format(run_script))

        sh_content = b'#!/usr/bin/env bash\n' \
            + str.encode('python {} {}\n'.format(run_script, params))
        print(sh_content)
        cmd_closure = lambda sbatch_file_name:\
            SMIT_CMD.substitute(
                partition=partition, num_cores=num_cores,
                feature=features, name=task_name,
                log=slurm_out, script=sbatch_file_name,
                machine=machine
            )
        temp_sh_exec(cmd_closure, sh_content, num_runs, dummy)
    elif command == 'cancel':
        to_exec = CANCEL_CMD.substitute(name=task_name)
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
        '-c', '--command', default='run',
        help='one of {}, default {}'.format(VALID_CMDS, VALID_CMDS[0]))
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
        '-C', '--feature-constraints', type=str, default='',
        help='required features, such as highmem or titanx')
    parser.add_argument(
        '-d', '--dummy', default=False, action='store_true',
        help='in dummy mode the slurm command is printed but not executed')
    args, unknown = parser.parse_known_args()  # pass unknown to runner script

    if args.dummy:
        print("WARN: using dummy mode")
    print("Using partition {}".format(args.partition))
    if args.command not in VALID_CMDS:
        raise ValueError(
            "command must be one of {}, but given: {}".format(VALID_CMDS, args.command)
        )
    with open(args.file) as f:
        task_dir_list = yaml.safe_load(f)
    for task_dir in task_dir_list:
        if not path.isdir(task_dir):
            raise ValueError("{} is not a valid directory".format(task_dir))
        else:
            task_execute(
                task_dir, args.command, args.length, args.dummy,
                args.partition, args.num_cores, args.feature_constraints,
                unknown
            )
