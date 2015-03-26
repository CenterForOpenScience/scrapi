from __future__ import unicode_literals

from dateutil.parser import parse

from .helpers import (
    default_name_parser,
    format_tags,
    oai_extract_url,
    oai_extract_doi,
    oai_process_contributors
)

CONSTANT = lambda x: lambda doc: x

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

OAISCHEMA = {
    "contributors": ('//dc:creator/node()', '//dc:contributor/node()', oai_process_contributors),
    'tags': ('//dc:subject/node()', format_tags),
    'id': {
        'doi': ('//dc:identifier/node()', oai_extract_doi),
        'url': ('//dc:identifier/node()', oai_extract_url),
        'serviceID': '//ns0:header/ns0:identifier/node()'
    },
    'dateUpdated': ('//ns0:header/ns0:datestamp/node()', lambda x: unicode(parse(x).isoformat())),
    'title': ('//dc:title/node()', lambda x: x[0] if isinstance(x, list) else x),
    'description': ('//dc:description/node()', lambda x: x[0] if isinstance(x, list) else x)
}
