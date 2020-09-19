def shard_indices(size, num_shards, rank):
    """
    commonly used to divide a task into chunks for execution
    """
    shard_size = (size - 1) // num_shards + 1
    begin = shard_size * rank
    end = min(shard_size * (rank + 1), size)
    return begin, end
