from __future__ import unicode_literals

import json
from hashlib import md5

import requests

from scrapi import settings
from scrapi.processing.osf.hashing import REPORT_HASH_FUNCTIONS
from scrapi.processing.osf.hashing import RESOURCE_HASH_FUNCTIONS


def already_processed(raw_doc):
    _md5 = md5(raw_doc['doc']).hexdigest()

    _filter = {
        'term': {
            'docHash': _md5
        }
    }

    return _search(_filter), _md5


def detect_collisions(hashlist, is_resource=False):
    if is_resource:
        _filter = {
            'and': [
                {
                    'terms': {
                        'uids': hashlist
                    }
                },
                {
                    'term': {
                        'isResource': True
                    }
                }
            ]
        }
    else:
        _filter = {
            'and': [
                {
                    'missing': {
                        'field': 'isResource',
                        'existence': True,
                        'null_value': True
                    }
                },
                {
                    'terms': {
                        'uids': hashlist
                    }
                }
            ]
        }
    found = _search(_filter)

    if found:
        return found

    return None


def generate_hash_list(normalized, hashes):
    hashlist = []

    for hashfunc in hashes:
        hashlist.append(hashfunc(normalized))

    return hashlist


def generate_resource_hash_list(normalized):
    return generate_hash_list(normalized, RESOURCE_HASH_FUNCTIONS)


def generate_report_hash_list(normalized):
    return generate_hash_list(normalized, REPORT_HASH_FUNCTIONS)


def _search(_filter):
    query = {
        'query': {
            'filtered': {
                'filter': _filter
            }
        }
    }

    kwargs = {
        'auth': settings.OSF_AUTH,
        'verify': settings.VERIFY_SSL,
        'data': json.dumps(query),
        'headers': {
            'Content-Type': 'application/json'
        }
    }

    ret = requests.post(settings.OSF_APP_URL, **kwargs).json()
    if ret['total'] > 0:
        return ret['results'][0]

    return None
