import os.path as osp
import yaml
import torch
import argparse


class ZooKeeper():
    def __init__(self):
        with open(
            osp.join(osp.dirname(osp.realpath(__file__)), 'config.yml')
        ) as cfg:
            config = yaml.load(cfg)['zoo']
        self.model_root = config['model_root']
        self.models = config['models']

    def fetch(self, category, variant):
        '''
        return the osp to the network checkpoint
        @param category: the category of the network i.e. resnet
        @param variant: the particular network variant chosen i.e. resnet101
        '''
        if category not in self.models:
            print("{} is not among the zoo categories".format(category))
            return None
        if variant not in self.models[category]:
            print("{} is not among  {}".format(variant, category))
        file_name = self.models[category][variant].split('/')[-1]
        path = osp.join(self.model_root, category, file_name)
        if not osp.isfile(path):
            print("{} is not a valid file".format(path))
            return None
        return torch.load(path)


def zoo_inspect_and_populate():
    """
    Inspect the zoo and download missing models
    """
    import wget
    parser = argparse.ArgumentParser(description='PyTorch Models ZooKeeper')
    parser.add_argument('-d', '--download', default=0, type=int,
                        help='whether to download missing models')
    args = parser.parse_args()
    keeper = ZooKeeper()
    model_root = keeper.model_root
    models = keeper.models
    print("Start zoo inspection")
    print('Download missing models: {}'.format(bool(args.download)))

    for category, models_profile in models.items():
        print('>>>>> {}'.format(category))
        for name, url in models_profile.items():
            path = osp.join(model_root, name, url.split('/')[-1])
            exists = osp.exists(path)
            print('{} exists {}'.format(name, exists))

            if not exists and bool(args.download):
                print('downloading {}'.format(name))
                wget.download(url, path)


if __name__ == '__main__':
    zoo_inspect_and_populate()
