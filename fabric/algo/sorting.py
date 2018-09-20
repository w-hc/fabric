def native_argsort(seq):
    return sorted(range(len(seq)), key=seq.__getitem__)
