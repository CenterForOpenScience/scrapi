import os
import errno
import logging
import importlib
from datetime import datetime
from contextlib import contextmanager

import vcr
import pytz

from scrapi import settings


logger = logging.getLogger(__name__)


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


def stamp_from_raw(raw_doc, **kwargs):
    kwargs['normalizeFinished'] = timestamp()
    stamps = raw_doc['timestamps']
    stamps.update(kwargs)
    return stamps


def build_raw_url(raw, normalized):
    return '{url}/{archive}{source}/{doc_id}/{consumeFinished}/raw.{raw_format}'.format(
        url=settings.SCRAPI_URL,
        source=normalized['source'],
        raw_format=raw['filetype'],
        doc_id=b64encode(raw['docID']),
        archive=settings.ARCHIVE_DIRECTORY,
        consumeFinished=normalized['timestamps']['consumeFinished'],
    )


@contextmanager
def maybe_recorded(file_name):
    # TODO put into cassandra
    if settings.STORE_HTTP_TRANSACTIONS:
        cassette = os.path.join(
            settings.RECORD_DIRECTORY,
            file_name, timestamp() + '.yml'
        )

        logger.info('Recording HTTP request for {} to {}'.format(file_name, cassette))

        with vcr.use_cassette(cassette, record_mode='all'):
            yield
    else:
        yield
