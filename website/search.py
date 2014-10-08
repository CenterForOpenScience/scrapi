# -*- coding: utf-8 -*-
"""
    Search module for the scrAPI website.
"""
import copy
import json
import logging
import requests
import datetime

from scrapi import settings

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

DEFAULT_PARAMS = {
    'q': '*',
    'start_date': None,
    'end_date': datetime.date.today().isoformat(),
    'sort_field': 'consumeFinished',
    'sort_type': 'desc',
    'from': 0,
    'size': 10,
    'format': 'json'
}


def query_osf(query):
    headers = {'Content-Type': 'application/json'}
    data = json.dumps(query)
    print data
    return requests.post(settings.OSF_APP_URL, auth=settings.OSF_AUTH, headers=headers, data=data).json()


def tutorial():
    return {
        'title': 'string representing title of the resource',
        'contributors': 'a list of dictionaries containing prefix, middle, family, suffix, and ORCID of contributors.',
        'id': 'a dictionary of unique IDs given to the resource based on the particular publication we’re accessing. Should include an entry for a URL that links right to the original resource, a DOI, and a service specific ID',
        'url': 'A url pointing to the resource\' real location',
        'doi': 'The digital object identifier of the resource, if it has one',
        'serviceID': 'A service specific identifier for the resource',
        'description': 'an abstract or general description of the resource',
        'tags': 'a list of tags or keywords identified in the resource itself, normalized to be all lower case',
        'source': 'string identifying where the resource came from',
        'timestamp': 'string indicating when the resource was accessed by scrAPI using the format YYYY-MM-DD h : m : s in iso format',
        'dateCreated': 'string indicating when the resource was first created or published using the format YYYY-MM-DD in iso format',
        'dateUpdated': 'string indicating when the resource was last updated in the home repository using the format YYYY-MM-DD in iso format',
    }


def search(raw_params):
    params = copy.deepcopy(DEFAULT_PARAMS)
    params.update(raw_params)
    for key in params.keys():
        if isinstance(params[key], list) and len(params[key]) == 1:
            params[key] = params[key][0]
    params['from'] = int(params['from'])
    params['size'] = int(params['size'])
    print params
    query = parse_query(params)
    query['format'] = params.get('format')
    return query_osf(query)


def parse_query(params):
    return {
        'query': build_query(
            params.get('q'),
            params.get('start_date'),
            params.get('end_date')
        ),
        'sort': build_sort(params.get('sort_field'), params.get('sort_type')),
        'from': params.get('from'),
        'size': params.get('size'),
    }


def build_query(q, start_date, end_date):
    return {
        'filtered': {
             'query': build_query_string(q),
             'filter': build_date_filter(start_date, end_date),
        }
    }


def build_query_string(q):
    return {
        'query_string': {
            'default_field': '_all',
            'query': q,
            'analyze_wildcard': True,
            'lenient': True  # TODO, may not want to do this
        }
    }


def build_date_filter(start_date, end_date):
    return {
        'range': {
            'consumeFinished': {
                'gte': start_date,  # TODO, can be None, elasticsearch may not like it
                'lte': end_date
            }
        }
    }

def build_sort(sort_field, sort_type):
    print sort_field
    return [{
        sort_field : {
            'order': sort_type
        }
    }]
