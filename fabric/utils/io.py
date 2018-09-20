import os.path as osp
import pickle


def save_object(obj, file_name):
    """Save a Python object by pickling it."""
    file_name = osp.abspath(file_name)
    with open(file_name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_object(file_name):
    "load the pickled object"
    file_name = osp.abspath(file_name)
    with open(file_name, 'rb') as f:
        return pickle.load(f)
