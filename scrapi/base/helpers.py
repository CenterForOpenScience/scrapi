from __future__ import unicode_literals

import re
import logging
import functools
from copy import deepcopy

import six
import pytz
import xmltodict
from lxml import etree
from dateutil import parser
from pycountry import languages
from nameparser import HumanName

from scrapi import requests


URL_REGEX = re.compile(r'(https?:\/\/\S*\.[^\s\[\]\<\>\}\{\^]*)')
DOI_REGEX = re.compile(r'(doi:10\.\S*)')
DOE_AFFILIATIONS_REGEX = re.compile(r'\s*\[(.*?)\]')
DOE_EMAIL_REGEX = re.compile(r'((?:,? (?:Email|email|E-mail|e-mail):\s*)?(\S*@\S*))')
DOE_ORCID_REGEX = re.compile(r'(\(ORCID:\s*(\S*)\))')


def CONSTANT(x):
    ''' Takes a value, returns a function that always returns that value
        Useful inside schemas for defining constants

        >>> CONSTANT(7)('my', 'name', verb='is')
        7
        >>> CONSTANT([123, 456])()
        [123, 456]
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

        >>> print(single_result(['hello', None]))
        hello
        >>> print(single_result([], default='hello'))
        hello
        >>> print(single_result([]))
        <BLANKLINE>

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
        >>> compose(int, add3, add3, divide2)(4)
        8
        >>> compose(int, divide2, add3, add3)(4)
        5
        >>> compose(int, divide2, compose(add3, add3), add)(7, 3)
        8
    '''
    def inner(func1, func2):
        return lambda *x, **y: func1(func2(*x, **y))
    return functools.reduce(inner, functions)


element_to_dict = compose(xmltodict.parse, etree.tostring)


def non_string(item):
    return not isinstance(item, str)


def updated_schema(old, new):
    ''' Creates a dictionary resulting from adding all keys/values of the second to the first
        The second dictionary will overwrite the first.

        >>> old, new = {'name': 'ric', 'job': None}, {'name': 'Rick'}
        >>> updated = updated_schema(old, new)
        >>> len(updated.keys())
        2
        >>> print(updated['name'])
        Rick
        >>> updated['job'] is None
        True

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
    return list(map(maybe_parse_name, names))


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


def format_doi_as_url(doi):
    if doi:
        plain_doi = doi.replace('doi:', '').replace('DOI:', '').strip()
        return 'http://dx.doi.org/{}'.format(plain_doi)


def gather_identifiers(args):
    identifiers = []
    for arg in args:
        if isinstance(arg, list):
            for identifier in arg:
                identifiers.append(identifier)
        elif arg:
            identifiers.append(arg)

    return identifiers


def maybe_group(match):
    '''
    evaluates an regular expression match object, returns the group or none
    '''
    return match.group() if match else None


def gather_object_uris(identifiers):
    '''
    Gathers object URIs if there are any
    >>> gathered = gather_object_uris(['nopenope', 'doi:10.10.gettables', 'http://dx.doi.org/yep'])
    >>> print(gathered[0])
    http://dx.doi.org/10.10.gettables
    >>> print(gathered[1])
    http://dx.doi.org/yep
    '''
    object_uris = []
    for item in identifiers:
        if 'doi' in item.lower():
            url_doi, just_doi = URL_REGEX.search(item), DOI_REGEX.search(item)
            url_doi = maybe_group(url_doi)
            just_doi = maybe_group(just_doi)

            if url_doi or just_doi:
                object_uris.append(url_doi or format_doi_as_url(just_doi))

    return object_uris


def seperate_provider_object_uris(identifiers):
    object_uris = gather_object_uris(identifiers)
    provider_uris = []
    for item in identifiers:

        found_url = maybe_group(URL_REGEX.search(item))

        if found_url:
            if 'viewcontent' in found_url:
                object_uris.append(found_url)
            else:
                if 'dx.doi.org' not in found_url:
                    provider_uris.append(found_url)

    return provider_uris, object_uris


def oai_process_uris(*args, **kwargs):
    use_doi = kwargs.get('use_doi', False)

    identifiers = gather_identifiers(args)
    provider_uris, object_uris = seperate_provider_object_uris(identifiers)

    potential_uris = (provider_uris + object_uris)
    if use_doi:
        for uri in object_uris:
            if 'dx.doi.org' in uri:
                potential_uris = [uri]
    try:
        canonical_uri = potential_uris[0]
    except IndexError:
        raise ValueError('No Canonical URI was returned for this record.')

    return {
        'canonicalUri': canonical_uri,
        'objectUris': object_uris,
        'providerUris': provider_uris
    }


def oai_extract_dois(*args):
    identifiers = gather_identifiers(args)
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
    names = gather_identifiers(args)
    return default_name_parser(names)


def dif_process_contributors(first_names, last_names):
    raw_names = zip(first_names, last_names)

    return [{'name': ' '.join(map(str, name)),
             'givenName': name[0],
             'familyName': name[1]} for name in raw_names]


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


def null_on_error(task, log=True):
    '''Decorator that makes a function return None on exception'''
    def inner(*args, **kwargs):
        try:
            return task(*args, **kwargs)
        except Exception as e:
            if log:
                logger = logging.getLogger('scrapi.base.helpers.null_on_error')
                logger.warn(e)
            return None
    return inner


def coerce_to_list(thing):
    ''' If a value is not already a list or tuple, puts that value in a length 1 list

        >>> niceties = coerce_to_list('hello')
        >>> len(niceties)
        1
        >>> print(niceties[0])
        hello
        >>> niceties2 = coerce_to_list(['hello'])
        >>> niceties2 == niceties
        True
        >>> niceties3 = (coerce_to_list(('hello', 'goodbye')))
        >>> len(niceties3)
        2
        >>> print(niceties3[0])
        hello
        >>> print(niceties3[1])
        goodbye
    '''
    if not (isinstance(thing, list) or isinstance(thing, tuple)):
        return [thing]
    return list(thing)


def datetime_formatter(datetime_string):
    '''Takes an arbitrary date/time string and parses it, adds time
    zone information and returns a valid ISO-8601 datetime string
    '''
    date_time = parser.parse(datetime_string)
    if not date_time.tzinfo:
        date_time = date_time.replace(tzinfo=pytz.UTC)
    return date_time.isoformat()


def doe_name_parser(name):
    if name.strip() == 'None':
        return {'name': ''}
    name, orcid = extract_and_replace_one(name, DOE_ORCID_REGEX)
    name, email = extract_and_replace_one(name, DOE_EMAIL_REGEX)
    name, affiliations = doe_extract_affiliations(name)

    parsed_name = maybe_parse_name(name)
    if affiliations:
        parsed_name['affiliation'] = list(map(doe_parse_affiliation, affiliations))
    if orcid:
        parsed_name['sameAs'] = ['https://orcid.org/{}'.format(orcid)]
    if email:
        parsed_name['email'] = email
    return parsed_name


def extract_and_replace_one(text, pattern):
    ''' Works with regexes with two matches, where the text of the first match
        is replaced and the text of the second is returned

        In the case where there is a match:
        >>> text = 'I feelvery happy'
        >>> pattern = re.compile(r'.*(very\s*(\S*)).*')
        >>> modified_text, match = extract_and_replace_one(text, pattern)
        >>> print(modified_text)
        I feel
        >>> print(match)
        happy

        In the case where there is not a match:
        >>> text = 'I feel happy'
        >>> modified_text, match = extract_and_replace_one(text, pattern)
        >>> modified_text == text
        True
        >>> match is None
        True
    '''
    matches = pattern.findall(text)
    if matches and len(matches) == 1:
        return text.replace(matches[0][0], ''), matches[0][1]
    return text, None


def doe_extract_affiliations(name):
    affiliations = DOE_AFFILIATIONS_REGEX.findall(name)
    for affiliation in affiliations:
        name = name.replace('[{}]'.format(affiliation), '')
    return name, affiliations


def doe_parse_affiliation(affiliation):
    return {'name': affiliation}  # TODO: Maybe parse out address?


def doe_process_contributors(names):
    return list(map(doe_name_parser, names))


def xml_text_only_list(elems):
    '''Return inner text of all elements in list'''
    return [xml_text_only(elem) for elem in elems]


def xml_text_only(elem):
    '''Return inner text of element with tags stripped'''
    etree.strip_tags(elem, '*')
    inner_text = elem.text
    if inner_text:
        return inner_text.strip()
    return None
