"""
Steps taken to produce ImageNet LMDBs:
Define a root directory
1. Download ILSVRC2012_img_train.tar, ILSVRC2012_img_val.tar, ILSVRC2012_devkit_t12.tar.gz to root
2. Use the processing pipeline offered by torchvision at version 0.6.0a0+82fd1c8
    a) torchvision.datasets.ImageNet has built in logic to process tars into images.
3. Now I end up with "meta.bin", "val", "train" directories in the root.
    a) meta.bin is a tuple of (wnid_to_classes, val_wnids)
    b) Both train and val contain 1000 directories named with synnet ID
        train/
            n01440764/
                some_image_name.JPEG
                ...
            ...

        val/
            n01440764/
                some_image_name.JPEG
                ...
            ...
4. This script acts from here.
"""
import json
from pathlib import Path
from tqdm import tqdm
from fabric.io.lmdb_tools import save_to_lmdb, ImageLMDB
from PIL import Image
import numpy as np
import torch

IMAGENET_META_SCHEMA = """
categories: [
    {
        id: 0
        wnid: "01440764-n"
        name: "tench, Tinca tinca",
        uri: "http://wordnet-rdf.princeton.edu/wn30/01440764-n",
    },
    ... # 1000 items
],
train: [
    (fname, id),
    ...
]  # 1281167 items
val: [
    (fname, id)
    ...
]  # 50000 items
"""

ROOT = Path("/scratch/haochenw/ILSVRC2012_tars")


def get_split_dir(split):
    assert split in ('train', 'val')
    return ROOT / split


def iterate_over_fnames(split):
    dirname = get_split_dir(split)
    for cat_id in sorted(dirname.iterdir()):
        # print(cat_id.name)
        wnid = cat_id.name
        for img_fname in sorted(cat_id.iterdir()):
            yield (wnid, img_fname)


def create_json_metadata():
    def create_cat_meta():
        wnids_to_classes = torch.load(ROOT / 'meta.bin')[0]
        accu = []
        seen = set()
        inx = 0
        for (wnid, _) in iterate_over_fnames("val"):
            if wnid not in seen:
                uri_template = "http://wordnet-rdf.princeton.edu/wn30/{}-n"
                accu.append(
                    {
                        "id": inx,
                        "wnid": wnid,
                        "name": wnids_to_classes[wnid],
                        "uri": uri_template.format(wnid[1:])  # remove first n
                    }
                )
                seen.add(wnid)
                inx += 1
        return accu

    raw_cat_meta = create_cat_meta()
    wnid_to_id = { e["wnid"]: e["id"] for e in raw_cat_meta }

    def per_split_meta(split):
        accu = []
        seen = set()
        for (wnid, fname) in iterate_over_fnames(split):
            fname = fname.name
            assert fname not in seen
            cat_id = wnid_to_id[wnid]
            accu.append((fname, cat_id))
            seen.add(fname)
        return accu

    val_meta = per_split_meta("val")
    train_meta = per_split_meta("train")

    meta = {
        "schema": IMAGENET_META_SCHEMA,
        "categories": raw_cat_meta,
        "train": train_meta,
        "val": val_meta
    }
    output_fname = 'meta.json'
    with (ROOT / output_fname).open('w') as f:
        json.dump(meta, f)


class ImageNetImageStream():
    def __init__(self, split, return_bytes=True):
        sizes = {
            "train": 1281167,
            "val": 50000
        }
        self.split = split
        self.size = sizes[split]
        self.return_bytes = return_bytes

    def __len__(self):
        return self.size

    def __iter__(self):
        seen = set()
        for (wnid, fname) in iterate_over_fnames(self.split):
            key = fname.name
            assert key not in seen
            seen.add(key)
            if self.return_bytes:
                with fname.open("rb") as f:
                    img = f.read()
            else:
                img = Image.open(fname)
            yield (key, img)


def main():
    create_json_metadata()

    split = "val"
    stream = ImageNetImageStream(split)
    save_to_lmdb(ROOT / f"{split}.lmdb", stream)

    split = "train"
    stream = ImageNetImageStream(split)
    save_to_lmdb(ROOT / f"{split}.lmdb", stream)

    """
    8197feb9780099f5b66700e74f53ee66  val.lmdb
    9bc4d80b042bc67c881392e9705bc304  train.lmdb
    """


def load_my_imagenet_from_lmdb(split):
    with (ROOT / "meta.json").open("r") as f:
        meta = json.load(f)
    files = meta[split]
    db = ImageLMDB(ROOT / f"{split}.lmdb")
    for (fname, lbl) in tqdm(files):
        k = fname.encode('ascii')
        img = db[k]
        yield img


def test(split="val"):
    offcial_stream = ImageNetImageStream(split, return_bytes=False)
    for other, mine in zip(offcial_stream, load_my_imagenet_from_lmdb(split)):
        other = np.array(other[1])
        mine = np.array(mine)
        assert (other == mine).all()


if __name__ == "__main__":
    # main()
    test("val")
