from __future__ import unicode_literals

from copy import deepcopy
from nameparser import HumanName


def update_schema(old, new):
    d = deepcopy(old)
    for key, value in new.items():
        if isinstance(value, dict) and old.get(key) and isinstance(old[key], dict):
            d[key] = update_schema(old[key], new[key])
        else:
            d[key] = value
    return d


def default_name_parser(names):
    contributor_list = []
    for person in names:
        name = HumanName(person)
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': '',
            'ORCID': ''
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

    return list(set([unicode(tag.lower().strip()) for tag in tags if tag.lower().strip()]))


def oai_extract_doi(identifiers):
    identifiers = [identifiers] if isinstance(identifiers, basestring) else identifiers
    for item in identifiers:
        if 'doi' in item.lower():
            return unicode(item.replace('doi:', '').replace('DOI:', '').replace('http://dx.doi.org/', '').strip())
    return ''


def oai_extract_url(identifiers):
    identifiers = [identifiers] if isinstance(identifiers, basestring) else identifiers
    for item in identifiers:
        if 'http://' in item or 'https://' in item and 'viewcontent' not in item:
            return unicode(item)


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
