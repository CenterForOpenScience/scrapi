import json

import requests

from scrapi import settings


POST_HEADERS = {
    'Content-Type': 'application/json'
}


def create_resource(normalized, hashlist):
    bundle = {
        'systemData': {
            'uuid': hashlist,
            'is_project': True
        },
        'permissions': ['read']
    }

    return _create_node(normalized, bundle)['id']


def create_report(normalized, parent, hashlist):
    bundle = {
        'title': '{}: {}'.format(normalized['source'], normalized['title']),
        'parent': parent,
        'category': 'report',
        'systemData': {
            'uuid': hashlist
        },
    }

    return _create_node(normalized, bundle)['id']


def _create_node(normalized, additional):
    contributors = [
        {
            'name': '{given} {middle} {family}'.format(**x),
            'email': x.get('email')
        }
        for x in normalized['contributors']
    ]

    bundle = {
        'title': normalized['title'],
        'description': normalized.get('description'),
        'contributors': contributors,
        'tags': normalized.get('tags'),
        'metadata': normalized.attributes,
    }

    bundle.update(additional)

    kwargs = {
        'auth': settings.OSF_AUTH,
        'data': json.dumps(bundle),
        'headers': POST_HEADERS
    }
    return requests.post(settings.OSF_NEW_PROJECT, **kwargs).json()


def _get_metadata(id):
    pass


def is_event(normalized):
    pass


def create_event(normalized):
    pass


def is_claimed(resource):
    pass


def update_resource(normalized, resource):
    pass


def update_report(normalized, report):
    kwargs = {
        'data': json.dumps(normalized.attributes),
        'headers': POST_HEADERS
    }
    url = '{}{}/'.format(settings.OSF_APP_URL, report)
    requests.post(url, **kwargs)
