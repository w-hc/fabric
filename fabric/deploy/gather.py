import os
import os.path as osp
from collections import OrderedDict


class Gatherer():
    '''
    a gatherer that expects a sow style experiment folder layout
    '''
    def __init__(self, research_project_cls, exp_root='exp'):
        '''
        Args:
            research_project_cls: the class of the research project.
                                    Assuming you did editable installation
            exp_root: the root of experiment folder within project folder
        '''
        package_path = research_project_cls.__path__[0]
        # the exp dir is parallel to the package
        basedir = osp.dirname(package_path)
        self.root = osp.join(basedir, exp_root)

    def collect(
        self, exp_name: str, processing_fn,
        deployed_in='runs', where_to_look_within_each_exp='output'
    ):
        '''
            exp_name: initial_trials/haul/
        '''
        subroot = osp.join(self.root, exp_name, deployed_in)
        exps = sorted(os.listdir(subroot))
        accu = OrderedDict()
        for e in exps:
            where2look = osp.join(subroot, e, where_to_look_within_each_exp)
            res = processing_fn(where2look)
            accu[e] = res
        return accu
