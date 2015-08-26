from datetime import datetime

import six
import json
import pytz
import logging

logger = logging.getLogger()


def timestamp():
    return pytz.utc.localize(datetime.utcnow()).isoformat()


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
        try:
            # A dirty way to convert to unicode in python 2 + 3.3+
            element = u''.join(element)
        except TypeError:
            pass
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


def doc_to_normed_dict(doc):
    # make the new dict actually contain real items
    normed = {}
    for key, value in dict(doc).iteritems():
        try:
            normed[key] = json.loads(value)
        except (ValueError, TypeError):
            normed[key] = value

    # Remove empty values and strip down to only normalized fields
    do_not_include = ['docID', 'doc', 'filetype', 'timestamps', 'source']
    for key in normed.keys():
        if not normed[key]:
            del normed[key]

    for key in normed.keys():
        if key in do_not_include:
            del normed[key]

    if normed.get('versions'):
        normed['versions'] = map(str, normed['versions'])

    # No datetime means the document wasn't normalized (probably wasn't on the approved list)
    if normed.get('providerUpdatedDateTime'):
        normed['providerUpdatedDateTime'] = normed['providerUpdatedDateTime'].isoformat()
    # import ipdb; ipdb.set_trace()
    return normed
