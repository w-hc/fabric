from pathlib import Path
import argparse
import yaml
import subprocess
from tempfile import NamedTemporaryFile
from .watch import Record, ConnWrapper, open_db
from .. import dir_of_this_file


VALID_ACTS = ('run', 'cancel')
DEFAULT_PARTITION = 'greg-gpu'


def load_template():
    template_fname = dir_of_this_file(__file__) / "sbatch_template.sh"
    with template_fname.open("r") as f:
        template = f.read()
    return template


template = load_template()


def sbatch_exec(script):
    with NamedTemporaryFile(suffix='.sh') as sbatch_file:
        # this temp, named buffer is needed. slurm will copy the named shell script
        # to a private location during execution. a name is needed
        script = str.encode(script)
        sbatch_file.file.write(script)
        sbatch_file.file.seek(0)
        sbatch_file_name = sbatch_file.name
        # shell=True is needed; what reason? I can't rmb
        subprocess.run(f"sbatch {sbatch_file_name}", shell=True, check=True)


def make_record(task_dirname, args, extra=[]):
    task_dirname = Path(task_dirname)
    job_name = task_dirname.name
    extra = ' '.join(extra)

    script = template.format(
        jname=job_name, partition=args.partition,
        num_gpus=args.num_gpus, num_cpus=args.num_cpus,
        task_dirname=str(task_dirname), log_fname=str(task_dirname / "slurm.out"),
        job_cmd=args.job, extra=extra
    )

    entry = Record(
        name=job_name, todo=0, path=str(task_dirname), sbatch=script
    )
    return entry


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
        help='the job partition. default {}'.format(DEFAULT_PARTITION))
    parser.add_argument(
        '-G', '--num-gpus', type=int, default=0,
        help='Number of GPUs')
    parser.add_argument(
        '-c', '--num-cpus', type=int, default=0,
        help='Number of CPUs')
    parser.add_argument(
        '-j', '--job', type=str, required=False, default="",
        help='the job command')
    parser.add_argument(
        '-m', '--mock', default=False, action='store_true',
        help='in mock mode the slurm command is printed but not executed')
    parser.add_argument(
        '--nodb', default=False, action='store_true',
        help='do not register jobs to database')

    args, unknown = parser.parse_known_args()  # pass unknown to runner script
    print(args)

    if args.num_cpus == 0:
        args.num_cpus = args.num_gpus * 2

    if args.mock:
        print("WARN: using mock mode")

    if args.action not in VALID_ACTS:
        raise ValueError(
            f"action must be one of {VALID_ACTS}, but given: {args.action}"
        )

    with open(args.file) as f:
        task_dir_list = yaml.safe_load(f)

    records = []
    for task_dir in task_dir_list:
        task_dir = Path(task_dir)
        assert task_dir.is_dir()
        entry = make_record(
            task_dir, args, unknown
        )

        if args.mock:
            print(entry.sbatch)
            break

        if args.action == "run":
            sbatch_exec(entry.sbatch)
        elif args.action == "cancel":
            sbatch_cancel(entry.name)

        records.append(entry)

    # if not args.mock and not args.nodb:
    if False:
        with open_db() as conn:
            conn = ConnWrapper(conn)
            if args.action == "run":
                conn.insert(records)
            elif args.action == "cancel":
                for row in records:
                    conn.drop_row(row.name)
            conn.commit()


def sbatch_cancel(jname):
    subprocess.run(f"scancel -n {jname}", shell=True, check=True)


def period_watch(interval):
    from time import sleep
    from tqdm import tqdm

    with open_db() as conn:
        conn = ConnWrapper(conn)
        pbar = tqdm()

        while True:
            pbar.update()
            jobs = conn.get_all_jobs()
            for j in jobs:
                jname, todo = j.name, j.todo
                if todo != 0:
                    if todo == 1:
                        sbatch_exec(j.sbatch)
                    elif todo == 2:
                        sbatch_cancel(jname)
                        conn.drop_row(jname)
                    conn.update(jname, "todo", 0)
            conn.commit()
            sleep(interval)
