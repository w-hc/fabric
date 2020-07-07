"""
The code is adapted from Yuxin Wu's tensorpack library.
1. the default serializer is just Python pickle and it is pretty fast
2. LMDB allows duplicate keys. Check against duplicate keys yourself.
3. In addition to the (k, v) entries, 2 additional items __len__, and __keys__
are stored which corresponds to the size of entries and the sorted list of keys
"""
from pathlib import Path
import platform
import lmdb
from dataflow.utils.serialize import dumps, loads
from dataflow.utils.utils import get_tqdm
from dataflow.utils import logger

__all__ = ['save_to_lmdb', 'LMDBData']


def probe_length(stream):
    try:
        sz = len(stream)
    except (NotImplementedError, TypeError):
        sz = 0
    return sz


def save_to_lmdb(db_fname, stream, write_frequency=5000):
    """
    Adapted from
    https://github.com/tensorpack/dataflow/blob/b1221974eba13619cf9e2fe427b33baedc307c4e/dataflow/dataflow/serialize.py#L30-L105
    Args:
        db_fname: the name of the database file
        stream (iterable): a stream of (key, val) pair
        write_frequency (int): the frequency to write back data to disk.
            A smaller value reduces memory usage.
    """

    db_fname = Path(db_fname).resolve()
    assert not db_fname.exists(), f"LMDB file {db_fname} exists!"
    db_fname = str(db_fname)

    # It's OK to use super large map_size on Linux, but not on other platforms
    # See: https://github.com/NVIDIA/DIGITS/issues/206
    map_size = 1099511627776 * 2 if platform.system() == 'Linux' else 128 * 10**6
    db = lmdb.open(
        db_fname, subdir=False, map_size=map_size,
        readonly=False, meminit=False, map_async=True
    )    # need sync() at the end
    size = probe_length(stream)

    # put data into lmdb, and doubling the size if full.
    # Ref: https://github.com/NVIDIA/DIGITS/pull/209/files
    def put_or_grow(txn, key, value):
        try:
            txn.put(key, value)
            return txn
        except lmdb.MapFullError:
            pass
        txn.abort()
        curr_size = db.info()['map_size']
        new_size = curr_size * 2
        logger.info("Doubling LMDB map_size to {:.2f}GB".format(new_size / 10**9))
        db.set_mapsize(new_size)
        txn = db.begin(write=True)
        txn = put_or_grow(txn, key, value)
        return txn

    with get_tqdm(total=size) as pbar:
        idx = -1
        keys = set([])

        # LMDB transaction is not exception-safe!
        # although it has a context manager interface
        txn = db.begin(write=True)
        for idx, (k, v) in enumerate(stream):

            k = str(k).encode('ascii')
            if k in keys:
                raise ValueError(f"key {k} is already used")
            v = dumps(v)
            txn = put_or_grow(txn, k, v)
            keys.add(k)

            pbar.update()
            if (idx + 1) % write_frequency == 0:
                txn.commit()
                txn = db.begin(write=True)
        txn.commit()

        keys = sorted(keys)
        with db.begin(write=True) as txn:
            txn = put_or_grow(txn, b'__keys__', dumps(keys))
            txn = put_or_grow(txn, b'__len__', dumps(len(keys)))

        logger.info("Flushing database ...")
        db.sync()
    db.close()


class LMDBData():
    def __init__(self, db_fname):
        self.lmdb = lmdb.open(
            db_fname, subdir=False, readonly=True, lock=False,
            readahead=True, map_size=1099511627776 * 2, max_readers=100
        )
        self.length = None

    def get(self, key):
        """
        Args:
            key (str or list of str)
        """
        # if not isinstance(keys, (list, tuple)):
        #     keys = [keys]
        assert isinstance(key, (bytes, str)), f"{key} is not string or bytes"
        if isinstance(key, str):
            key = key.encode("ascii")

        with self.lmdb.begin(write=False) as txn:
            res = txn.get(key)
            res = loads(res)
        return res

    def __len__(self):
        if self.length is None:
            size = self.get(b'__len__')
            self.length = size
        return self.length

    def keys(self):
        return self.get(b'__keys__')
