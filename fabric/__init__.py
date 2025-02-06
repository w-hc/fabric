from pathlib import Path


def dir_of_this_file(__filevar__):
    '''
    this is not cwd; this is the directory of the __file__
    '''
    curr_file_dir = Path(__filevar__).resolve().parent
    return curr_file_dir
