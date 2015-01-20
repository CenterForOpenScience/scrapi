import os
import errno
import importlib
from datetime import datetime

import pytz


def timestamp():
    return pytz.utc.localize(datetime.utcnow()).isoformat().decode('utf-8')


def import_consumer(consumer_name):
    return importlib.import_module('scrapi.consumers.{}'.format(consumer_name))


# Thanks to
# https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def make_dir(dirpath):
    try:
        os.makedirs(dirpath)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirpath):
            raise
