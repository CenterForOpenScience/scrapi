import six
from datetime import datetime

import pytz


def timestamp():
    return pytz.utc.localize(datetime.utcnow()).isoformat().decode('utf-8')


def copy_to_unicode(element):
    """ used to transform the lxml version of unicode to a
    standard version of unicode that can be pickalable -
    necessary for linting """

    if isinstance(element, dict):
        for key, val in element.items():
            element[key] = copy_to_unicode(val)
    elif isinstance(element, list):
        for idx, item in enumerate(element):
            element[idx] = copy_to_unicode(item)
    else:
        element = ''.join(element)
        try:
            element = six.u(element)
        except (TypeError, UnicodeDecodeError):
            pass
    return element


def stamp_from_raw(raw_doc, **kwargs):
    kwargs['normalizeFinished'] = timestamp()
    stamps = raw_doc['timestamps']
    stamps.update(kwargs)
    return stamps
