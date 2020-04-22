

def check_start_end_time(start, end, total_duration):
    '''
    start and end can be of any units. Just make sure that the units for
    start, end and duration are consistent
    '''
    # 1. populate start
    if start is None:
        start = 0

    # 2. confirm the end secs
    if end is None:
        end = total_duration
    else:
        if end > total_duration:
            raise ValueError(
                "end exceeds {:.2f} stream of length {:.2f}s".format(
                    end, total_duration
                )
            )

    # 3. make sure the ordering is respected
    if end < start:
        raise ValueError(
            "end should be larger than start, got "
            "start={} and end={}".format(start, end)
        )
    return start, end
