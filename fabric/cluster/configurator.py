import os
import os.path as osp
import yaml
from fabric.utils.io import save_object, load_object
from fabric.cluster.save_manager import SaveManager
from fabric.utils.logging import setup_logging

logger = setup_logging(__file__)


class Configurator():
    def __init__(self, caller_file_or_dir_name):
        # always convert to dirname
        caller_dir_path = osp.abspath(caller_file_or_dir_name)
        del caller_file_or_dir_name
        if not osp.isdir(caller_dir_path):
            caller_dir_path = osp.dirname(caller_dir_path)
        assert(osp.isdir(caller_dir_path))
        self.caller_dir_path = caller_dir_path
        logger.info(self.caller_dir_path)
        with open(osp.join(caller_dir_path, 'config.yml')) as f:
            self.config = yaml.load(f)

    def service_folder_setup(self, service):
        # note that index is buried
        dirname = osp.join(self.caller_dir_path, service)
        if not osp.isdir(dirname):
            os.mkdir(dirname)
            logger.info("making {}".format(service))
        else:
            logger.info("service dir {} exists. skipping".format(service))
        return dirname

    def get_ckpt_writer(self, save_f, load_f):
        """
        *_f are callables follow the following conventions
        load_f(fname)
        save_f(obj, fname)
        with arbitrary argument name variants. This allows maximum flexibility
        """
        ckpt_path = self.service_folder_setup('checkpoint')
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
        output_path = self.service_folder_setup('output')
        output_writer = SaveManager(
            output_path, save_f, load_f,
            file_suffix='pkl', delete_last=False, keep_interval=None
        )
        return output_writer

    def get_tboard_writer(self):
        from tensorboardX import SummaryWriter
        logging_path = self.service_folder_setup('tblog')
        writer = SummaryWriter(log_dir=logging_path)
        return writer
