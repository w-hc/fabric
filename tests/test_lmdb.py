from pathlib import Path
import numpy as np
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
    # root = Path("/projects/haochenw")
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
    db = LMDBData(fname)
    keys = db.query_keys()
    np.random.shuffle(keys)
    for k in tqdm(keys):
        val = db.get(k)
    # for
    # print(val.mean())


if __name__ == "__main__":
    fname = Path(get_db_fname())
    fname.unlink(missing_ok=True)
    create_database()
    load_database()
