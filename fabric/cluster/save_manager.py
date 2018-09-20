import os
import os.path as osp
from datetime import datetime
from fabric.algo.sorting import native_argsort

time_format_str = '%y-%m-%d-%H-%M-%S'


def format_save_name(curr_iter, suffix):
    '''
    name will be epoch major, followed by a second level timestamp
    '''
    fname = '{}_iter{}.{}'.format(
        datetime.now().strftime(time_format_str), curr_iter, suffix
    )
    return fname


class SaveManager():
    def __init__(self, output_path, save_f, load_f,
                 file_suffix, delete_last=False, keep_interval=None):
        assert len(file_suffix) > 0
        self.root = output_path
        self.file_suffix = file_suffix
        self.save_f = save_f
        self.load_f = load_f
        self.delete_last = delete_last
        self.keep_interval = keep_interval

    def _list_root_dir(self):
        valid_files = os.listdir(self.root)
        if self.file_suffix is not None:
            valid_files = list(filter(
                lambda x: x.split('.')[-1] == self.file_suffix, valid_files
            ))
        # sort by time str, iter pair. Time takes precedence
        pairs = []
        for fname in valid_files:
            no_suffix = fname.split('.')[0]
            time_str, iter_inx = no_suffix.split('iter')
            pairs.append( (time_str, int(iter_inx)) )
        args_sorted = native_argsort(pairs)
        return [valid_files[i] for i in args_sorted], pairs

    def _get_newest_file(self):
        file_names, pairs = self._list_root_dir()
        if len(file_names) == 0:
            return None
        else:
            return file_names[-1]

    def _delete_last_file(self, curr_iter):
        file_names, pairs = self._list_root_dir()
        if len(file_names) == 0:
            return
        fname, inx = file_names[-1], pairs[-1][1]
        if self.keep_interval is not None and inx % self.keep_interval == 0:
            # assuming keep interval is 10 then do not delete ckpt 20.
            return
        os.remove(osp.join(self.root, fname))

    def save(self, curr_iter, obj):
        if self.delete_last:
            self._delete_last_file(curr_iter)
        fname = format_save_name(curr_iter, self.file_suffix)
        save_path = osp.join(self.root, fname)
        self.save_f(obj, save_path)

    def load(self, fname):
        return self.load_f( osp.join(self.root, fname) )

    def load_latest(self):
        fname = self._get_newest_file()
        if fname is None:
            return None
        else:
            return self.load(fname)

    # def output_exists(self, epoch):
    #     '''
    #     returns true if the output for the specified epoch exists.
    #     '''
    #     look_at_epoch = lambda name: name.split('_')[0]
    #     e_name = look_at_epoch( epoch_save_name(epoch, 'pkl') )
    #     file_e_names = map(look_at_epoch, self.list_ckpt_dir() )
    #     return e_name in file_e_names


def test_save_manager():
    from fabric.utils.io import save_object, load_object
    from torch import load as tload
    from torch import save as tsave
    test_dir_path = '/scratch/test_ckpt_manager'
    # play with some params and test them
    os.makedirs(test_dir_path, exist_ok=True)
    manager = SaveManager(
        test_dir_path, save_f=tsave, load_f=tload,
        file_suffix='pkl', delete_last=True, keep_interval=6
    )
    for i in range(20):
        manager.save(curr_iter=i, obj={'iter': i})
        print(manager.load_latest())


if __name__ == '__main__':
    test_save_manager()
