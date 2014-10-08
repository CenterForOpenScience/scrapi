import os
import errno
import string
import importlib
from datetime import datetime


timestamp = lambda: datetime.utcnow().isoformat().decode('utf-8')


def import_consumer(consumer_name):
    # TODO Make suer that consumer_name will always import the correct module
    return importlib.import_module('scrapi.consumers.{}'.format(consumer_name))


# Thanks to https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def make_dir(dirpath):
    try:
        os.makedirs(dirpath)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirpath):
            raise


# https://gist.github.com/seanh/93666
def safe_filename(s):
    """Take a string and return a valid filename constructed from the string.
Uses a whitelist approach: any characters not present in valid_chars are
removed. Also spaces are replaced with underscores.

Note: this method may produce invalid filenames such as ``, `.` or `..`
When I use this method I prepend a date string like '2009_01_15_19_46_32_'
and append a file extension like '.txt', so I avoid the potential of using
an invalid filename.

"""
    valid_chars = '-_.() %s%s' % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ', '_')  # I don't like spaces in filenames.
    return filename
