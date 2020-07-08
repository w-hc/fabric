"""
The code is adapted from Yuxin Wu's tensorpack dataflow library.
1. the default serializer is just Python pickle and according to Yuxin it is as
fast as msgpack.
2. LMDB overwrites values if given duplicate keys. Check against duplicate keys yourself.
3. In addition to the (k, v) entries, 2 additional items __len__, and __keys__
are stored which corresponds to the size of entries and the sorted list of keys

4. Critical for PyTorch Multiprocessing Spawn, lmdb environment object itself
cannot be pickled. Hence a workaround is used here. See __setstate__ and __getstate__
which override the default pickling behavior to recreate a new DB connection upon
pickling.
"""
from pathlib import Path
import io
import platform
import lmdb
import pickle
from PIL import Image
from tqdm import tqdm
from dataflow.utils import logger  # TODO: add a consistent logger for fabric itself

dumps = lambda x: pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads

__all__ = ['save_to_lmdb', 'LMDBData', 'ImageLMDB']


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

    with tqdm(total=size) as pbar:
        idx = -1
        existing_keys = set([])  # for fast membership testing only
        keys = []

        # LMDB transaction is not exception-safe!
        # although it has a context manager interface
        txn = db.begin(write=True)
        for idx, (k, v) in enumerate(stream):
            k = str(k).encode('ascii')
            if k in existing_keys:
                raise ValueError(f"key {k} is already used")

            v = dumps(v)
            txn = put_or_grow(txn, k, v)

            existing_keys.add(k)
            keys.append(k)  # guarantee insertion order.

            pbar.update()
            if (idx + 1) % write_frequency == 0:
                txn.commit()
                txn = db.begin(write=True)
        txn.commit()

        with db.begin(write=True) as txn:
            txn = put_or_grow(txn, b'__keys__', dumps(keys))
            txn = put_or_grow(txn, b'__len__', dumps(len(keys)))

        logger.info("Flushing database ...")
        db.sync()
    db.close()


class LMDBData():
    """
    https://github.com/pytorch/vision/issues/689 provides solution on how to
    deal with the un-picklable db Environment.
    """
    def __init__(self, db_fname, readahead=False):
        self.db_fname = str(db_fname)
        self.readahead = readahead
        # disabling readahead improves random read performance

        self.read_txn = self.make_read_transaction()
        self.length = self._retrieve_item(b'__len__')

    def make_read_transaction(self):
        db = lmdb.open(
            self.db_fname, subdir=False, readonly=True, lock=False,
            readahead=self.readahead, map_size=1099511627776 * 2,
            max_readers=100
        )
        txn = db.begin(write=False)
        return txn

    def __getstate__(self):
        """
        Used only by pickle to customize its behavior so as to ignore the db txn
        """
        state = self.__dict__
        state['read_txn'] = None
        return state

    def __setstate__(self, state):
        """
        Used only by pickle to customize its behavior so as to ignore the db txn
        """
        self.__dict__ = state
        self.read_txn = self.make_read_transaction()

    def __getitem__(self, key):
        """this is public, and can be customzized by children"""
        return self._retrieve_item(key)

    def _retrieve_item(self, key):
        """this method is private and not user-facing, not customizable"""
        assert isinstance(key, (bytes, str)), f"{key} is not string or bytes"
        if isinstance(key, str):
            key = key.encode("ascii")
        res = self.read_txn.get(key)
        res = loads(res)
        return res

    def __len__(self):
        return self.length

    def keys(self):
        return self._retrieve_item(b'__keys__')


class ImageLMDB(LMDBData):
    """
    Images enjoy significant space savings from PNG/JPG format.
    When saving, save the raw file bytes.
    When loading, convert the bytes to Image with a wrapper
    """
    def __getitem__(self, key):
        bytes_data = super().__getitem__(key)
        buf = io.BytesIO()
        buf.write(bytes_data)
        buf.seek(0)
        img = Image.open(buf)
        return img
