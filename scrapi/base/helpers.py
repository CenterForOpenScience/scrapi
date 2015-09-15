from __future__ import unicode_literals

import re
import logging
import functools
from copy import deepcopy

import six
import pytz
from lxml import etree
from dateutil import parser
from pycountry import languages
from nameparser import HumanName

from scrapi import requests


URL_REGEX = re.compile(r'(https?://\S*\.\S*)')
DOI_REGEX = re.compile(r'(doi:10\.\S*)')


def CONSTANT(x):
    ''' Takes a value, returns a function that always returns that value
        Useful inside schemas for defining constants

        >>> CONSTANT('hello')('my', 'name', verb='is')
        u'hello'
        >>> CONSTANT(['example', 'values'])()
        [u'example', u'values']
    '''
    def inner(*y, **z):
        return x
    return inner


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
    ''' A function that will return the first element of a list if it exists

        >>> single_result(['hello', None])
        u'hello'
        >>> single_result([], default='hello')
        u'hello'
        >>> single_result([])
        u''
    '''
    return l[0] if l else default


def compose(*functions):
    ''' evaluates functions from right to left.

        >>> add = lambda x, y: x + y
        >>> add3 = lambda x: x + 3
        >>> divide2 = lambda x: x/2
        >>> subtract4 = lambda x: x - 4
        >>> subtract1 = compose(add3, subtract4)
        >>> subtract1(1)
        0
        >>> compose(subtract1, add3)(4)
        6
        >>> compose(add3, add3, divide2)(4)
        8
        >>> compose(divide2, add3, add3)(4)
        5
        >>> compose(divide2, compose(add3, add3), add)(7, 3)
        8
    '''
    def inner(func1, func2):
        return lambda *x, **y: func1(func2(*x, **y))
    return functools.reduce(inner, functions)


def updated_schema(old, new):
    ''' Creates a dictionary resulting from adding all keys/values of the second to the first
        The second dictionary will overwrite the first.

        >>> old, new = {'name': 'ric', 'job': None}, {'name': 'Rick'}
        >>> updated = updated_schema(old, new)
        >>> sorted(updated.items(), key=updated.get)  # Dicts are unsorted, need to sort to test
        [(u'job', None), (u'name', u'Rick')]
    '''
    d = deepcopy(old)
    for key, value in new.items():
        if isinstance(value, dict) and old.get(key) and isinstance(old[key], dict):
            d[key] = updated_schema(old[key], new[key])
        else:
            d[key] = value
    return d


def default_name_parser(names):
    ''' Takes a list of names, and attempts to parse them
    '''
    return map(maybe_parse_name, names)


def maybe_parse_name(name):
    ''' Tries to parse a name. If the parsing fails, returns a dictionary
        with just the unparsed name (as per the SHARE schema)
    '''
    return null_on_error(parse_name)(name) or {'name': name}


def parse_name(name):
    ''' Takes a human name, parses it into given/middle/last names
    '''
    person = HumanName(name)
    return {
        'name': name,
        'givenName': person.first,
        'additionalName': person.middle,
        'familyName': person.last
    }


def format_tags(all_tags, sep=','):
    tags = []
    if isinstance(all_tags, six.string_types):
        tags = all_tags.split(sep)
    elif isinstance(all_tags, list):
        for tag in all_tags:
            if sep in tag:
                tags.extend(tag.split(sep))
            else:
                tags.append(tag)

    return list(set([six.text_type(tag.lower().strip()) for tag in tags if tag.strip()]))


def oai_process_uris(*args):
    identifiers = []
    for arg in args:
        if isinstance(arg, list):
            for identifier in arg:
                identifiers.append(identifier)
        elif arg:
            identifiers.append(arg)

    object_uris = []
    provider_uris = []
    for item in identifiers:
        if 'doi' in item.lower():
            doi = item.replace('doi:', '').replace('DOI:', '').strip()
            if 'http://dx.doi.org/' in doi:
                object_uris.append(doi)
            else:
                object_uris.append('http://dx.doi.org/{}'.format(doi))

        try:
            found_url = URL_REGEX.search(item).group()
        except AttributeError:
            found_url = None
        if found_url:
            if 'viewcontent' in found_url:
                object_uris.append(found_url)
            else:
                provider_uris.append(found_url)

    try:
        canonical_uri = (provider_uris + object_uris)[0]
    except IndexError:
        raise ValueError('No Canonical URI was returned for this record.')

    return {
        'canonicalUri': canonical_uri,
        'objectUris': object_uris,
        'providerUris': provider_uris
    }


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


def language_codes(langs):
    '''Given an array of language names, returns an array of ISO 639-3 codes

    e.g. ['English', 'Russian'] -> ['eng', 'rus']
    '''
    return list(filter(lambda x: x, map(get_code, langs)))


def get_code(language):
    try:
        return languages.get(name=language).bibliographic
    except KeyError:
        return None


def oai_get_records_and_token(url, throttle, force, namespaces, verify):
    """ Helper function to get the records and any resumptionToken
    from an OAI request.

    Takes a url and any request parameters and returns the records
    along with the resumptionToken if there is one.
    """
    data = requests.get(url, throttle=throttle, force=force, verify=verify)

    doc = etree.XML(data.content)

    records = doc.xpath(
        '//ns0:record',
        namespaces=namespaces
    )

    token = doc.xpath(
        '//ns0:resumptionToken/node()',
        namespaces=namespaces
    )

    return records, token


def extract_doi_from_text(identifiers):
    identifiers = [identifiers] if not isinstance(identifiers, list) else identifiers
    for item in identifiers:
        try:
            found_url = DOI_REGEX.search(item).group()
            return 'http://dx.doi.org/{}'.format(found_url.replace('doi:', ''))
        except AttributeError:
            continue


def null_on_error(task):
    '''Decorator that makes a function return None on exception'''
    def inner(*args, **kwargs):
        try:
            return task(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger('scrapi.base.helpers.null_on_error')
            logger.warn(e)
            return None
    return inner


def coerce_to_list(thing):
    ''' If a value is not already a list or tuple, puts that value in a length 1 list

        >>> coerce_to_list('hello')
        [u'hello']
        >>> coerce_to_list(['hello'])
        [u'hello']
        >>> coerce_to_list(('hello', 'goodbye'))
        (u'hello', u'goodbye')
    '''
    if not (isinstance(thing, list) or isinstance(thing, tuple)):
        return [thing]
    return thing


def datetime_formatter(datetime_string):
    '''Takes an arbitrary date/time string and parses it, adds time
    zone information and returns a valid ISO-8601 datetime string
    '''
    date_time = parser.parse(datetime_string)
    if not date_time.tzinfo:
        date_time = date_time.replace(tzinfo=pytz.UTC)
    return date_time.isoformat()
