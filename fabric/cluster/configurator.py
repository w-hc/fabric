import os
import os.path as osp
import yaml
from fabric.utils.io import save_object, load_object
from fabric.cluster.save_manager import SaveManager
from fabric.utils.logging import setup_logging

logger = setup_logging(__file__)


def symlink_setup(src, dst):
    """
    src is the real save path. If it doens't exist, make it
    dst is the symlink to be made in exp directory
    return the src
    """
    # make the remote save directory if it does not exist
    os.makedirs(src, mode=0o755, exist_ok=True)
    if osp.islink(dst):
        logger.info("sym-link to {} already exists. relinking".format(src))
        os.unlink(dst)
    os.symlink(src, dst)
    return src


class Configurator():
    def __init__(self, caller_file_or_dir_name, save_root=None):
        # always convert to dirname
        caller_dir_path = osp.abspath(caller_file_or_dir_name)
        del caller_file_or_dir_name
        if not osp.isdir(caller_dir_path):
            caller_dir_path = osp.dirname(caller_dir_path)
        assert(osp.isdir(caller_dir_path))
        self.caller_dir_path = caller_dir_path
        logger.info(self.caller_dir_path)

        # use system default save root if not supplied
        self.save_root = save_root
        if not self.save_root:
            with open(
                osp.join( osp.dirname(osp.realpath(__file__)), 'config.yml' )
            ) as cfg:
                self.save_root = yaml.load(cfg)['save_root']

        # store the caller's configurations
        with open(osp.join(caller_dir_path, 'config.yml')) as f:
            self.config = yaml.load(f)
            self.project_name = self.config['_meta']['project']
            self.group_name = self.config['_meta']['group']
            self.index = self.config['_meta']['index']

    def dual_folder_setup(self, service):
        # note that index is buried
        return symlink_setup(
            src=osp.join(
                self.save_root, self.project_name, self.group_name,
                service, self.index
            ),
            dst=osp.join(
                osp.join(self.caller_dir_path, service)
            )
        )

    def get_ckpt_writer(self, save_f, load_f):
        """
        *_f are callables follow the following conventions
        load_f(fname)
        save_f(obj, fname)
        with arbitrary argument name variants. This allows maximum flexibility
        """
        ckpt_path = self.dual_folder_setup('checkpoint')
        ckpt_manager = SaveManager(
            ckpt_path, save_f, load_f, 'ckpt',
            delete_last=True, keep_interval=None
        )  # caller can change keep_interval based on actual needs
        return ckpt_manager

    def get_output_writer(self, save_f=save_object, load_f=load_object):
        """
        *_f are callables follow the following conventions
        load_f(fname)
        save_f(obj, fname)
        with arbitrary argument name variants. This allows maximum flexibility
        """
        output_path = self.dual_folder_setup('output')
        output_writer = SaveManager(
            output_path, save_f, load_f,
            file_suffix='pkl', delete_last=False, keep_interval=None
        )
        return output_writer

    def get_tboard_writer(self):
        from tensorboardX import SummaryWriter
        logging_path = self.dual_folder_setup('tblog')
        writer = SummaryWriter(log_dir=logging_path)
        return writer
