import os.path as path
import argparse
import yaml
from string import Template
import subprocess
from tempfile import NamedTemporaryFile


cancel_cmd = Template('scancel -n $name')

clean = Template('rm ${dir_name}/checkpoint/*.ckpt')

list_job_info = Template('squeue -n $name')

submit_cmd = Template(
    'sbatch '
    '-d singleton '
    '--nodes=1 -p $partition -c${num_cores} $feature '
    '--job-name=$name  --output="$log" "$script" '
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


def task_execute(task_dir, action, length, dummy, partition, num_cores, features):
    path_split = task_dir.split('/')
    task_name = path_split[-1]

    task_name
    slurm_out = path.join(task_dir, 'slurm.out')
    run_script = path.join(task_dir, 'run.py')
    eval_notebook = path.join(task_dir, 'eval.ipynb')
    num_runs = int(length)
    # bash: must use single quote to escape input like 1080ti|titanx
    features = '-C \'{}\''.format(features) if features else ''

    with open(path.join(task_dir, 'config.yml'), 'r') as f:
        _task_config = yaml.load(f)
        if 'num_cores' in _task_config:
            num_cores = _task_config['num_cores']

    if action == 'run':
        if not path.isfile(run_script):
            raise ValueError("{} is not a valid file".format(run_script))

        sh_content = b'#!/usr/bin/env bash\n'\
            + str.encode('python "{}"\n'.format(run_script))
            # + str.encode('CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES srun python "{}"\n'.format(run_script))
        cmd_closure = lambda sbatch_file_name:\
            submit_cmd.substitute(
                partition=partition, num_cores=num_cores,
                feature=features, name=task_name,
                log=slurm_out, script=sbatch_file_name
            )
        temp_sh_exec(cmd_closure, sh_content, num_runs, dummy)

    elif action == 'eval':
        if not path.isfile(eval_notebook):
            raise ValueError("{} is not a valid notebook".format(eval_notebook))
        cmd_closure = lambda sbatch_file_name:\
            submit_cmd.substitute(
                partition=partition, num_cores=num_cores,
                feature=features, name=task_name,
                log=slurm_out, script=sbatch_file_name
            )
        sh_content = b'#!/usr/bin/env bash\n' + str.encode(
            'jupyter nbconvert --ExecutePreprocessor.timeout=-1 '
            '--to notebook --execute {nb} --output {nb}'
            .format(nb=eval_notebook)
        )
        temp_sh_exec(cmd_closure, sh_content, num_runs, dummy)

    elif action == 'cancel':
        to_exec = cancel_cmd.substitute(name=task_name)
        print(to_exec)
        print("")
        if not dummy:
            subprocess.run(to_exec, shell=True)


def main():
    allowed_actions = ('run', 'cancel', 'eval')
    default_partition = 'greg-gpu'
    parser = argparse.ArgumentParser(description='TTIC slurm sbatch submit')
    parser.add_argument(
        '-f', '--file', type=str, required=True,
        help='a yaml containing a list of absolute paths to the job folders')
    parser.add_argument(
        '-p', '--partition', default=default_partition, type=str,
        help='the partition the job is submitted to. default {}'.format(default_partition))
    parser.add_argument(
        '-a', '--action', default='run',
        help='one of {}, default {}'.format(allowed_actions, allowed_actions[0]))
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
    args = parser.parse_args()

    file_name = args.file
    action = args.action
    dummy = args.dummy
    partition = args.partition
    length = args.length
    num_cores = args.num_cores
    features = args.feature_constraints
    print("Using partition {}".format(partition))
    if dummy:
        print("Under dummy mode")
    if action not in allowed_actions:
        raise ValueError(
            "action must be one of {}, but given: {}".format(allowed_actions, action))

    with open(file_name) as f:
        task_dir_list = yaml.load(f)
    for task_dir in task_dir_list:
        if not path.isdir(task_dir):
            raise ValueError("{} is not a valid directory".format(task_dir))
        else:
            task_execute(
                task_dir, action, length, dummy, partition, num_cores, features)
