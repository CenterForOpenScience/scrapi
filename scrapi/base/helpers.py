from __future__ import unicode_literals

import re
from copy import deepcopy
import functools

from nameparser import HumanName

URL_REGEX = re.compile(ur'(https?://\S*\.\S*)')


def build_properties(*args):
    ret = []
    for arg in args:
        name, expr = arg[0], arg[1]
        kwargs = arg[2] if len(arg) > 2 else {}
        description, uri = kwargs.get('description'), kwargs.get('uri')
        ret.append(build_property(name, expr, description=description, uri=uri))
    return ret


def build_property(name, expr, description=None, uri=None):
    property = {
        'name': name,
        'properties': {
            name: expr
        },
    }
    if description:
        property['description'] = description
    if uri:
        property['uri'] = uri
    return property


def single_result(l, default=''):
    return l[0] if l else default


def compose(*functions):
    '''
    evaluates functions from right to left.
    ex. compose(f, g)(x) = f(g(x))

    credit to sloria
    '''
    def inner(func1, func2):
        return lambda x: func1(func2(x))
    return functools.reduce(inner, functions)


def updated_schema(old, new):
    d = deepcopy(old)
    for key, value in new.items():
        if isinstance(value, dict) and old.get(key) and isinstance(old[key], dict):
            d[key] = updated_schema(old[key], new[key])
        else:
            d[key] = value
    return d


def default_name_parser(names):
    contributor_list = []
    for person in names:
        name = HumanName(person)
        contributor = {
            'name': person,
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last,
            'email': '',
            'sameAs': []
        }
        contributor_list.append(contributor)

    return contributor_list


def format_tags(all_tags, sep=','):
    tags = []
    if isinstance(all_tags, basestring):
        tags = all_tags.split(sep)
    elif isinstance(all_tags, list):
        for tag in all_tags:
            if sep in tag:
                tags.extend(tag.split(sep))
            else:
                tags.append(tag)

    return list(set([unicode(tag.lower().strip()) for tag in tags if tag.strip()]))


def oai_extract_doi(identifiers):
    identifiers = [identifiers] if not isinstance(identifiers, list) else identifiers
    for item in identifiers:
        if 'doi' in item.lower():
            return unicode(item.replace('doi:', '').replace('DOI:', '').replace('http://dx.doi.org/', '').strip())
    return ''


def oai_extract_url(identifiers):
    identifiers = [identifiers] if not isinstance(identifiers, list) else identifiers
    for item in identifiers:
        try:
            found_url = URL_REGEX.search(item).group()
            if 'viewcontent' not in found_url:
                return found_url.decode('utf-8')
        except AttributeError:
            continue


def oai_process_contributors(*args):
    names = []
    for arg in args:
        if isinstance(arg, list):
            for name in arg:
                names.append(name)
        elif arg:
            names.append(arg)
    return default_name_parser(names)


def pack(*args, **kwargs):
    return args, kwargs
