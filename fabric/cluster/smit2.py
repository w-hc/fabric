from pathlib import Path
import argparse
import subprocess
from pprint import pprint
from tempfile import NamedTemporaryFile
from .watch import Record, ConnWrapper, open_db
from .. import dir_of_this_file, yaml_read


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


def make_record(job_name, task_dirname, args, extra=[]):
    task_dirname = Path(task_dirname)
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


def infer_job_names(task_dirs, nj: int):
    assert len(task_dirs) > 0

    # split an abspath by / e.g. aa/bb/c_dd/01 -> [aa, b, c_dd, 01]
    task_dirs = [Path(p).resolve().parts for p in task_dirs]

    if nj is not None:
        task_dirs = [segs[-nj:] for segs in task_dirs]
    else:
        if len(task_dirs) == 1:
            # pick the last seg of the path
            task_dirs = [segs[-1:] for segs in task_dirs]
        else:
            # compare each segment, and find the common prefix.
            # job name starts where the common prefix segments end.
            # this is NOT string common prefix; if a segment is different e.g. c_dd vs c_ee,
            # I want the whole segment in the job name
            inx = 0
            while True:
                if len(set([segs[inx] for segs in task_dirs])) > 1:
                    break
                inx += 1
            task_dirs = [segs[inx:] for segs in task_dirs]

    job_names = ['/'.join(segs) for segs in task_dirs]
    return job_names


def main():
    parser = argparse.ArgumentParser(description='slurm sbatch submit')
    parser.add_argument(
        '-f', '--file', type=str, required=False, default=None,
        help='yaml of a list of abspaths to the job directories, where j_cmd is run.')
    parser.add_argument(
        '--dir', type=str, required=False, default=None,
        help='simply exec the j_cmd in the specified directory.')
    parser.add_argument(
        '--nj', type=int, required=False, default=None,
        help='use the last n segments of each path as job name'
    )

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
    pprint(vars(args))
    print("")

    # slurm is smart enough to handle the error case of both being 0
    if args.num_cpus == 0:
        args.num_cpus = args.num_gpus * 2

    xor = (args.file is not None) ^ (args.dir is not None)
    if not xor:
        raise ValueError("exactly one of [--file] or [--dir] is required for smit")
    task_dir_list = yaml_read(args.file) if args.file else [args.dir]

    job_names = infer_job_names(task_dir_list, args.nj)

    if args.mock:
        print("WARN: using mock mode")

    if args.action not in VALID_ACTS:
        raise ValueError(
            f"action must be one of {VALID_ACTS}, but given: {args.action}"
        )

    records = []
    for i in range(len(task_dir_list)):
        task_dir = Path(task_dir_list[i])
        assert task_dir.is_dir()
        # TODO: load the config.yml and read submit specific options
        # override what is in the args

        entry = make_record(
            job_names[i], task_dir, args, unknown
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
