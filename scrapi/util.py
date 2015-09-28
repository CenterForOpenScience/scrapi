from datetime import datetime

import six
import pytz


def timestamp():
    return pytz.utc.localize(datetime.utcnow()).isoformat()


def copy_to_unicode(element):
    """ used to transform the lxml version of unicode to a
    standard version of unicode that can be pickalable -
    necessary for linting """

    if isinstance(element, dict):
        return {
            key: copy_to_unicode(val)
            for key, val in element.items()
        }
    elif isinstance(element, list):
        return list(map(copy_to_unicode, element))
    else:
        try:
            # A dirty way to convert to unicode in python 2 + 3.3+
            return u''.join(element)
        except TypeError:
            return element


def stamp_from_raw(raw_doc, **kwargs):
    kwargs['normalizeFinished'] = timestamp()
    stamps = raw_doc['timestamps']
    stamps.update(kwargs)
    return stamps


def format_date_with_slashes(date):
    return date.strftime('%m/%d/%Y')


def json_without_bytes(jobj):
    """
        An ugly hack.

        Before we treat a structure as JSON, ensure that bytes are decoded to str.
    """
    # Create a JSON-compatible copy of the attributes for validation
    jobj = jobj.copy()
    for k, v in jobj.items():
        if isinstance(v, six.binary_type):
            jobj[k] = v.decode('utf8')
    return jobj
