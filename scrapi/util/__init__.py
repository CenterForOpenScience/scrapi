import os
import errno
import importlib
from datetime import datetime

import pytz


def timestamp():
    return pytz.utc.localize(datetime.utcnow()).isoformat().decode('utf-8')


def import_harvester(harvester_name):
    return importlib.import_module('scrapi.harvesters.{}'.format(harvester_name))


# Thanks to
# https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def make_dir(dirpath):
    try:
        os.makedirs(dirpath)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirpath):
            raise


def copy_to_unicode(element, encoding='utf-8'):
    """ used to transform the lxml version of unicode to a
    standard version of unicode that can be pickalable -
    necessary for linting """

    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)
