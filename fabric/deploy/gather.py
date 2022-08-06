import os
import os.path as osp
from collections import OrderedDict

'''
Experiment hierarchy
    exp_store_root -> subroot -> runs/deployments
'''


class Gatherer():
    '''
    a gatherer that expects a sow style experiment folder layout
    '''
    def __init__(self, exp_root):
        '''
        Args:
            exp_root: the root of experiment folder within project folder
        '''
        self.exp_store_root = osp.join(exp_root)
        self.deployed_in = 'runs'

    def path_to_subroot(self, subroot: str):
        return osp.join(self.exp_store_root, subroot)

    def browse_subroot(self, subroot: str):
        subroot_p = self.path_to_subroot(subroot)
        deployed_p = osp.join(subroot_p, self.deployed_in)
        return sorted(os.listdir(deployed_p))

    def path_to_exp(self, subroot, exp):
        path = self.path_to_subroot(subroot)
        # .../ablate_different_arch/runs/resnet34
        return osp.join(path, self.deployed_in, exp)

    def collect(self, subroot: str, exp_name: str, processing_fn):
        path = self.path_to_exp(subroot, exp_name)
        return processing_fn(path)

    def all_collect(self, subroot: str, processing_fn):
        '''
            subroot: initial_trials/haul/
        '''
        exps = self.browse_subroot(subroot)
        accu = OrderedDict()
        for e in exps:
            accu[e] = self.collect(subroot, e, processing_fn)
        return accu
