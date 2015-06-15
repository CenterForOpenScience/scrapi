from __future__ import unicode_literals

import re
import functools
from copy import deepcopy

from pycountry import languages
from nameparser import HumanName


URL_REGEX = re.compile(ur'(https?://\S*\.\S*)')

''' Takes a value, returns a function that always returns that value
    Useful inside schemas for defining constants '''
CONSTANT = lambda x: lambda *_, **__: x


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
        'name': CONSTANT(name),
        'properties': {
            name: expr
        },
    }
    if description:
        property['description'] = CONSTANT(description)
    if uri:
        property['uri'] = CONSTANT(uri)
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
    ''' Creates a dictionary resulting from adding all keys/values of the second to the first

    The second dictionary will overwrite the first.'''
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


def oai_extract_dois(*args):
    identifiers = []
    for arg in args:
        if isinstance(arg, list):
            for identifier in arg:
                identifiers.append(identifier)
        elif arg:
            identifiers.append(arg)
    dois = []
    for item in identifiers:
        if 'doi' in item.lower():
            doi = item.replace('doi:', '').replace('DOI:', '').strip()
            if 'http://dx.doi.org/' in doi:
                dois.append(doi)
            else:
                dois.append('http://dx.doi.org/{}'.format(doi))
    return dois


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


def language_code(language):
    try:
        return languages.get(name=language)
    except KeyError:
        return None
