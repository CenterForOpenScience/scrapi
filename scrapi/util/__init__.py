import os
import errno
import importlib
from urllib2 import quote


def import_consumer(consumer_name):
    # TODO Make suer that consumer_name will always import the correct module
    return importlib.import_module('scrapi.consumers.{}'.format(consumer_name))


# :: Str -> Str
def doc_id_to_path(doc_id):
    replacements = [
        ('/', '%2f'),
    ]
    for find, replace in replacements:
        doc_id = doc_id.replace(find, replace)

    return quote(doc_id)


# Thanks to https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def make_dir(dirpath):
    try:
        os.makedirs(dirpath)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirpath):
            raise
