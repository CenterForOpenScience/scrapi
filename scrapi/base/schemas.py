from __future__ import unicode_literals

from dateutil.parser import parse

from .helpers import (
    default_name_parser,
    format_tags,
    oai_extract_url,
    oai_extract_doi,
    oai_process_contributors
)

CONSTANT = lambda x: lambda *_, **__: x

BASEXMLSCHEMA = {
    "description": ('//dc:description/node()', lambda x: unicode(x.strip())),
    "contributor": ('//dc:creator/node()', lambda x: default_name_parser(x.split(';'))),
    "title": ('//dc:title/node()', lambda x: unicode(x.strip())),
    "releaseDate": ('//dc:dateEntry/node()', lambda x: unicode(x.strip())),
    "notificationLink": ('//dcq:identifier-citation/node()', lambda x: unicode(x.strip())),
    "resourceIdentifier": ('//dcq:identifier-citation/node()', lambda x: unicode(x.strip())),
    "directLink": ('//dcq:identifier-citation/node()', lambda x: unicode(x.strip())),
    "relation": ('//dc:doi/node()', lambda x: [unicode(x.strip())])
}

OAISCHEMA = {
    "contributor": ('//dc:creator/node()', '//dc:contributor/node()', oai_process_contributors),
    'directLink': ('//dc:identifier/node()', oai_extract_url),
    'relation': ('//dc:identifier/node()', lambda x: [oai_extract_doi(x)]),
    'notificationLink': ('//dc:identifier/node()', oai_extract_url),
    'resourceIdentifier': ('//dc:identifier/node()', oai_extract_url),
    'releaseDate': ('//ns0:header/ns0:datestamp/node()', lambda x: unicode(parse(x).date().isoformat())),
    'title': ('//dc:title/node()', lambda x: x[0] if isinstance(x, list) else x),
    'description': ('//dc:description/node()', lambda x: x[0] if isinstance(x, list) else x)
}
