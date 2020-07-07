from pathlib import Path
import numpy as np
from PIL import Image
from tqdm import tqdm
from fabric.io.lmdb_tools import save_to_lmdb, LMDBData


class DummyStream():
    def __init__(self, num_data_points: int):
        self.length = num_data_points

    def __len__(self):
        return self.length

    def __iter__(self):
        for inx in range(self.length):
            key = f"{inx}.key"  # each key is a mix of int and string
            data = np.random.randint(0, 256, size=(224, 224, 3), dtype=np.uint8)
            yield (key, data)


def get_db_fname():
    root = Path("/scratch/haochenw")
    # root = Path("/compute/autobot-0-9/haochenw/")  # from SSD of another machine
    # root = Path("/projects/haochenw")  # from a shared HDD
    fname = root / "db1.lmdb"
    fname = str(fname)
    return fname


def create_database():
    fname = get_db_fname()
    data_stream = DummyStream(int(1e6))
    save_to_lmdb(fname, data_stream)


def load_database():
    """
    test whether individual read transaction is as cheap as contiguous
    read transactions
    """
    fname = get_db_fname()
    db = LMDBData(fname, readahead=False)
    keys = db.keys()
    np.random.shuffle(keys)
    for k in tqdm(keys):
        val = db[k]


def load_discrete_files():
    class MyDset():
        def __init__(self):
            self.root = Path("/projects/haochenw/av/all_clips/clip_repr_images")

        def __len__(self):
            return int(5 * 1e5)

        def __getitem__(self, index):
            fname = f"{index}.jpg"
            im = np.array(Image.open(self.root / fname))
            return im

    dset = MyDset()
    size = int(5 * 1e5)
    inds = np.random.choice(size, size, replace=False)
    for inx in tqdm(inds):
        im = dset[inx]


if __name__ == "__main__":
    # fname = Path(get_db_fname())
    # fname.unlink(missing_ok=True)
    # create_database()
    load_database()
    # load_discrete_files()
    """
    testing shows that when it comes to loading files from HDD,
    if I disable readahead, lmdb loading has even faster and stabler throughput
    than vanilla file loading.
    The effect of readahead is very pronoucned when I load from SSD of another
    machine. It results in a 10x difference in throughput.
    """
