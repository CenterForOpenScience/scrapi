from __future__ import unicode_literals

import json

import requests

from scrapi import settings
from scrapi.processing.osf.hashing import REPORT_HASH_FUNCTIONS
from scrapi.processing.osf.hashing import RESOURCE_HASH_FUNCTIONS


def detect_collisions(hashlist, is_resource=False):
    if is_resource:
        _filter = {
            'terms': {
                'uuid': hashlist
            }
        }
    else:
        _filter = {
            'and': [
                {
                    'missing': {
                        'field': 'pid',
                        'existence': True,
                        'null_value': True
                    }
                },
                {
                    'terms': {
                        'uuid': hashlist
                    }
                }
            ]
        }

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
        return ret['results'][0]['attached']['nid']

    return None


def generate_hash_list(normalized, hashes):
    hashlist = []

    for hashfunc in hashes:
        hashlist.append(hashfunc(normalized))

    return hashlist


def generate_resource_hash_list(normalized):
    return generate_hash_list(normalized.attributes, RESOURCE_HASH_FUNCTIONS)


def generate_report_hash_list(normalized):
    return generate_hash_list(normalized.attributes, REPORT_HASH_FUNCTIONS)
