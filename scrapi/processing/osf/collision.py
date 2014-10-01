from urllib2 import quote

import requests

from scrapi import settings
from scrapi.processing.osf.hashing import REPORT_HASH_FUNCTIONS
from scrapi.processing.osf.hashing import RESOURCE_HASH_FUNCTIONS


def detect_collisions(hashlist):
    uuids = 'uuid:{}'.format(','.join(hashlist))
    url = '{}?q={}'.format(settings.OSF_APP_URL, quote(uuids))

    ret = requests.get(url, auth=settings.OSF_AUTH).json()

    if ret['total'] > 0:
        return ret['results'][0]['guid']

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
