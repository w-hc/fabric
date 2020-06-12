from path import Path
import yaml


__all__ = ['EvalCatalogue', ]


class EvalCatalogue():
    '''
    Structure of the catalogue: note that the index is on fname.
    1. Fname is unique in the directory. Good as key
    2. each eval file can be associated with a variety of metadata, not just iter
        fname_a:
            iter: 1
            split: train
        fname_b:
            iter: 1
            split: val
        fname_c:
            iter: 2
            split: train
    '''
    def __init__(self, root):
        root = Path(root)
        assert root.isdir()
        self.root = root
        self.catalogue_fname = root / 'catalogue.yml'

    def write_to_catalogue(self, src_dict):
        with open(self.catalogue_fname, 'w') as f:
            yaml.dump(src_dict, f)

    def read_catalogue(self):
        if not self.catalogue_fname.isfile():
            return {}
        else:
            with self.catalogue_fname.open(mode='r') as f:
                return yaml.safe_load(f)

    def register_eval(self, fname, meta):
        '''make sure that iter is always in the metadata field'''
        assert 'iter' in meta
        catalogue = self.read_catalogue()
        assert fname not in catalogue
        catalogue[fname] = meta
        self.write_to_catalogue(catalogue)
