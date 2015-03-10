from __future__ import unicode_literals

from copy import deepcopy
from dateutil.parser import parse

from nameparser import HumanName


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


def format_tags(all_tags):
    tags = []
    if isinstance(all_tags, basestring):
        for taglist in all_tags.split(' '):
            tags += taglist.split(',')

    return list(set([unicode(tag.lower().strip()) for tag in tags if tag.lower().strip()]))


def update_schema(old, new):
    d = deepcopy(old)
    for key, value in new.items():
        if isinstance(value, dict) and old.get(key) and isinstance(old[key], dict):
            d[key] = update_schema(old[key], new[key])
        else:
            d[key] = value
    return d


BASEXMLSCHEMA = {
    "description": ('//dc:description/node()', lambda x: unicode(x.strip())),
    "contributors": ('//dc:creator/node()', lambda x: default_name_parser(x.split(';'))),
    "title": ('//dc:title/node()', lambda x: unicode(x.strip())),
    "dateUpdated": ('//dc:dateEntry/node()', lambda x: unicode(x.strip())),
    "id": {
        "url": ('//dcq:identifier-citation/node()', lambda x: unicode(x.strip())),
        "serviceID": ('//dc:ostiId/node()', lambda x: unicode(x.strip())),
        "doi": ('//dc:doi/node()', lambda x: unicode(x.strip()))
    },
    "tags": ('//dc:subject/node()', format_tags)
}


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

OAISCHEMA = {
    "contributors": ('//dc:creator/node()', '//dc:contributor/node()', oai_process_contributors),
    'tags': ('//dc:subject/node()', format_tags),
    'id': {
        'doi': ('//dc:identifier/node()', oai_extract_doi),
        'url': ('//dc:identifier/node()', oai_extract_url),
        'serviceID': '//ns0:header/ns0:identifier/node()'
    },
    'dateUpdated': ('//ns0:header/ns0:datestamp/node()', lambda x: unicode(parse(x).isoformat())),
    'title': '//dc:title/node()',
    'description': ('//dc:description/node()', lambda x: x[0] if isinstance(x, list) else x)
}
