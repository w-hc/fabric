'''
This module manages the path for data and experiments
The hope is that the codebase should be easily portable over different clusters

1. Data loading should be machine specific for most optimzied training throughput
and to reduce NFS traffic which might otherwise harm other users.

2. Experiment storage is cluster specific i.e. all the machines will log to a
shared location during training over NFS.
It is okay to generate some light traffic over NFS this way.
The alternative which involves having a machine specific exp root and copying
over exp data at the end of training is too unpalatable.

Hmm... what if I am writing a large amount of video or image visualizations?
Need to think more about this.

In general, exp root resides in the exp/ folder of the project package.
You should set it up manually, sym-link or what not.

Data loading should be programmatically customized with this module.
'''
import os
import os.path as osp
import logging

__all__ = ['get_data_storage_root', ]


def get_data_storage_root(section="staging"):
    registrar = DataStorageRegistrar()
    return registrar.data_root(section)


class DataStorageRegistrar():
    def __init__(self):
        """
        staging refers to volatile data to be sync-ed to node local storage
        for faster access
        """
        self.logger = logging.getLogger(__name__)
        cluster_identifier = os.environ['SELF']
        data_layout_maps = {
            'ttic': {
                "permanent": ["/share/data/vision-greg2/users/whc/audio_visual"],
                "staging": []
            },
            'trinity': {
                "permanent": ["/data2/haochenw/"],
                "staging": [
                    "/ssd0/haochenw/staging",
                    "/ssd1/haochenw/staging",
                    "/data2/haochenw/staging"
                ]
            },
            'autobot': {
                "permanent": ["/project_data/ramanan/haochenw/"],
                "staging": [
                    # "/scratch/haochenw/staging",
                    "/project_data/ramanan/haochenw/staging"
                ]
            }
        }
        assert cluster_identifier in data_layout_maps,\
            "cluster id {} does not have a registered data root".format(cluster_identifier)
        self.path_layout = data_layout_maps[cluster_identifier]

    def data_root(self, section):
        choices = self.path_layout[section]
        # pick the first one that is a dir
        root = None
        for candidate in choices:
            if osp.isdir(candidate):
                root = candidate
                break
        self.logger.info(f"data storage root for {section}: {root}")
        return root
