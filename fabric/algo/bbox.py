import numpy as np


def xywh_bbox_on_mask(mask):
    """
    return the tightest bbox in xywh format for the given binary mask
    """
    xs, ys = np.where(mask > 0)
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    bbox = [x_min, y_min, x_max - x_min + 1, y_max - y_min + 1]
    return bbox
