from pathlib import Path
import hashlib
from tqdm import tqdm


def get_digest(file_path):
    file_path = Path(file_path)
    size = file_path.stat().st_size

    h = hashlib.sha256()
    chunk_size = h.block_size
    with open(file_path, 'rb') as f, tqdm(total=size) as pbar:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            pbar.update(chunk_size)

    return h.hexdigest()
