import os
from tempfile import gettempdir
from itertools import islice, cycle
from collections import namedtuple
import heapq

# module for sorting the lines in the input file according to a given attribute (in our use it was time)

Keyed = namedtuple("Keyed", ["key", "obj"])


def merge(key=None, *iterables):
    # based on code posted by Scott David Daniels in c.l.p.
    # http://groups.google.com/group/comp.lang.python/msg/484f01f1ea3c832d

    if key is None:
        keyed_iterables = iterables
    else:
        keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable)
                            for iterable in iterables]
    for element in heapq.merge(*keyed_iterables):
        yield element.obj


def batch_sort(input_file, output_file, key, buffer_size=32000, tempdirs=None):
    """
    sorts a batch from the input file
    :param input_file: input file name
    :param output_file: output file name
    :param key: key to sort by
    :param buffer_size: buffer size for the batch
    :param tempdirs: temporary directories to use for sorting, if None then a new one is made by gettempdir
    """
    if tempdirs is None:
        tempdirs = []
    if not tempdirs:
        tempdirs.append(gettempdir())

    chunks = []
    try:
        with open(input_file, 'rb', 64 * 1024) as input_file:
            input_iterator = iter(input_file)
            for tempdir in cycle(tempdirs):
                current_chunk = list(islice(input_iterator, buffer_size))
                if not current_chunk:
                    break
                current_chunk.sort(key=key)
                output_chunk = open(os.path.join(tempdir,'%06i'%len(chunks)), 'w+b',64*1024)
                chunks.append(output_chunk)
                output_chunk.writelines(current_chunk)
                output_chunk.flush()
                output_chunk.seek(0)
        with open(output_file, 'wb', 64 * 1024) as output_file:
            output_file.writelines(merge(key, *chunks))
    finally:
        for chunk in chunks:
            try:
                chunk.close()
                os.remove(chunk.name)
            except Exception:
                pass


def get_key(time_attribute_index):
    return lambda line: int(str(line).split(',')[time_attribute_index])


def sort_file(time_attribute_index, input_file: str, output_file: str):
    batch_sort(input_file, output_file, get_key(time_attribute_index))
