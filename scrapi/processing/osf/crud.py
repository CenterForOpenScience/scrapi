from __future__ import unicode_literals

import json
from copy import deepcopy

import requests

from scrapi import settings


POST_HEADERS = {
    'Content-Type': 'application/json'
}
EVENT_TYPES = ['letter', 'image']


def create_resource(normalized):
    contributors = [
        {
            'name': '{} {} {}'.format(x['given'], x['middle'], x['family']),
            'email': x.get('email')
        }
        for x in
        normalized['contributors']
    ]

    bundle = {
        'permissions': ['read'],
        'contributors': contributors
    }

    return _create_node(normalized, bundle)['id']


def create_report(normalized, parent):
    contributors = [
        {
            'name': '{} {} {}'.format(x['given'], x['middle'], x['family']),
            'email': x.get('email')
        }
        for x in
        normalized['contributors']
    ]

    bundle = {
        'title': '{}: {}'.format(normalized['source'], normalized['title']),
        'parent': parent,
        'category': 'report',
        'contributors': contributors
    }

    return _create_node(normalized, bundle)['id']


def update_node(nid, normalized):
    current = _get_metadata(nid)
    if current['collisionCategory'] > normalized['collisionCategory']:
        new = current.attributes
        new.update(normalized)
    else:
        new = normalized.attributes
        new.update(current)

    if new != current:
        url = '{}{}/{}/'.format(settings.OSF_APP_URL, 'projects', nid)
        kwargs = {
            'auth': settings.OSF_AUTH,
            'data': json.dumps(new),
            'verify': settings.VERIFY_SSL,
            'headers': POST_HEADERS
        }
        return requests.put(url, **kwargs)


def is_event(normalized): # "is event" means "is not project"
    if not normalized.get('contributors'):  # if no contributors, return true
        return True
    if not normalized.get('title'):  # if there's no title, return true
        return True
    # if it's a type we don't want to be a project, return true
    if normalized['properties'].get('type'): # first of all, if there's a type
        dctype = normalized['properties']['type'].lower()
        if dctype in EVENT_TYPES:
            return True
    return False


def is_claimed(resource):
    url = '{}{}/get_contributors/'.format(settings.OSF_APP_URL, resource)

    ret = requests.get(url, auth=settings.OSF_AUTH, verify=settings.VERIFY_SSL).json()
    for contributor in ret['contributors']:
        if contributor['registered']:
            return True
    return False


def get_collision_cat(source):
    return settings.MANIFESTS[source]['collisionCategory']


def clean_report(normalized):
    new = deepcopy(normalized)
    del new['id']
    del new['source']
    del new['meta']['docHash']
    return new


def _create_node(bundle, create_options):
    kwargs = {
        'auth': settings.OSF_AUTH,
        'data': json.dumps(bundle),
        'verify': settings.VERIFY_SSL,
        'headers': POST_HEADERS
    }

    mid = requests.post(settings.OSF_METADATA, **kwargs).json()['id']

    kwargs['data'] = json.dumps(create_options)
    return requests.post(settings.OSF_PROMOTE.format(mid), **kwargs).json()


def _get_metadata(id):
    url = '{}projects/{}/?sort=collisionCategory'.format(settings.OSF_APP_URL, id)
    return requests.get(url, auth=settings.OSF_AUTH, verify=settings.VERIFY_SSL).json()


def dump_metadata(data):
    kwargs = {
        'auth': settings.OSF_AUTH,
        'data': json.dumps(data),
        'headers': POST_HEADERS,
        'verify': settings.VERIFY_SSL
    }
    ret = requests.post(settings.OSF_METADATA, **kwargs)
    if ret.status_code != 201:
        pass  # TODO

    data['_id'] = ret.json()['id']
    return data


def update_metadata(id, data):
    kwargs = {
        'auth': settings.OSF_AUTH,
        'data': json.dumps(data),
        'headers': POST_HEADERS,
        'verify': settings.VERIFY_SSL
    }
    url = '{}{}/'.format(settings.OSF_METADATA, id)
    ret = requests.put(url, **kwargs)

    if ret.status_code != 200:
        pass  # TODO
