import os
import os.path as osp
import subprocess
from pathlib import Path


TAG_FNAME = "git_version"


def git_version(src_dir):
    curr = os.getcwd()
    assert osp.isdir(src_dir), '{} is not a valid dir'.format(src_dir)
    os.chdir(src_dir)

    # adapted from https://github.com/numpy/numpy/blob/96103d769301f9c915e23b2a233aa5634008db81/setup.py#L65-L86
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH', 'HOME']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
        return out

    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', 'HEAD'])
        GIT_REVISION = out.strip().decode('ascii')
    except (subprocess.SubprocessError, OSError):
        GIT_REVISION = "Unknown"

    os.chdir(curr)
    return GIT_REVISION


def self_version(fname):
    dirname = Path(fname).resolve().parent
    return git_version(dirname)


def tag_version(fname, output_dir="./"):
    sha = self_version(fname)
    if output_dir is not None:
        output_dir = Path(output_dir)
        if not output_dir.is_dir():
            output_dir.mkdir(parents=True, exist_ok=True)
        v_fname = output_dir / TAG_FNAME
        with open(v_fname, "w") as f:
            f.write(sha)
    return sha


def list_subdirs_versions():
    first_k = 6

    root = Path.cwd()
    data = []
    for p in root.iterdir():
        if p.is_dir():
            v_fname = p / TAG_FNAME
            if v_fname.is_file():
                with v_fname.open("r") as f:
                    tag = f.read().strip()
                    data.append([p.name, tag[:first_k]])

    data = sorted(data)
    # print(tabulate(data, tablefmt="tsv"))
    for (key, tag) in data:
        print(key, tag, sep=",")
