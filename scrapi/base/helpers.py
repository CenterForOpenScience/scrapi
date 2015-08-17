from __future__ import unicode_literals

import re
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
        }
        contributor_list.append(contributor)

    return contributor_list


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
                return found_url
        except AttributeError:
            continue
    raise ValueError('No Canonical URI was returned for this record.')


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


def date_formatter(date_string):
    '''Takes an arbitrary date/time string and parses it, adds time
    zone information and returns a valid ISO-8601 datetime string
    '''
    date_time = parser.parse(date_string)
    if not date_time.tzinfo:
        date_time = date_time.replace(tzinfo=pytz.UTC)
    return date_time.isoformat()
