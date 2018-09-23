from collections import namedtuple

# conv is generic. It can be conv filter, pooling, atrous etc
conv = namedtuple('conv', ['size', 'stride'])


def cal_receptive_field_and_stride(conv_list):
    """
    return receptive field and stride
    """
    if len(conv_list) == 0:
        return 1, 1
    prev_recep, prev_stride = cal_receptive_field_and_stride(conv_list[1:])
    curr_conv = conv_list[0]
    f_l, f_s = curr_conv.size, curr_conv.stride
    stride = f_s * prev_stride
    recep = (f_l - 1) * stride + prev_recep
    return recep, stride


def _test():
    rfield, stride = cal_receptive_field_and_stride([
        conv(size=3, stride=2),
        conv(size=3, stride=1),
        conv(size=3, stride=1),
    ])
    print(rfield, stride)  # 9, 2


if __name__ == '__main__':
    _test()
