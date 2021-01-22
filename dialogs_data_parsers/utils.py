def iterate_on_parts_by_condition(iterable, condition):
    cur_chunk = []
    for elem in iterable:
        if not condition(elem):
            cur_chunk.append(elem)
        else:
            yield cur_chunk
            cur_chunk = []

    if cur_chunk:
        yield cur_chunk
