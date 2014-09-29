import os
import errno
import importlib

from scrapi import settings


def import_consumer(consumer_name):
    # TODO Make suer that consumer_name will always import the correct module
    return importlib.import_module('scrapi.consumers.{}'.format(consumer_name))


def build_norm_dir(consumer_name, timestamp, norm_doc):
    pass  # TODO


def build_raw_dir(consumer_name, timestamp, raw_doc):
    manifest = settings.MANIFESTS[consumer_name]
    base = [
        settings.ARCHIVE_DIR,
        manifest['directory'],
        str(raw_doc.get('doc_id')).replace()
    ]


# Thanks to https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def make_dir(dirpath):
    try:
        os.makedirs(dirpath)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirpath):
            raise
