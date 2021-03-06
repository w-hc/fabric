import os
import os.path as osp
import subprocess


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
