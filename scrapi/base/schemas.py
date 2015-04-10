from __future__ import unicode_literals

from dateutil.parser import parse

from .helpers import (
    default_name_parser,
    oai_extract_url,
    oai_extract_doi,
    oai_process_contributors
)

CONSTANT = lambda x: lambda *_, **__: x

BASEXMLSCHEMA = {
    "description": ('//dc:description/node()', lambda x: unicode(x.strip())),
    "contributors": ('//dc:creator/node()', lambda x: default_name_parser(x.split(';'))),
    "title": ('//dc:title/node()', lambda x: unicode(x.strip())),
    "providerUpdatedDateTime": ('//dc:dateEntry/node()', lambda x: unicode(x.strip())),
    "uris": {
        "canonicalUri": ('//dcq:identifier-citation/node()', lambda x: unicode(x.strip())),
    }
}

OAISCHEMA = {
    "contributors": ('//dc:creator/node()', '//dc:contributor/node()', oai_process_contributors),
    "uris": {
        "canonicalUri": ('//dc:identifier/node()', oai_extract_url)
    },
    'providerUpdatedDateTime': ('//ns0:header/ns0:datestamp/node()', lambda x: unicode(parse(x).replace(tzinfo=None).isoformat())),
    'title': ('//dc:title/node()', lambda x: x[0] if isinstance(x, list) else x),
    'description': ('//dc:description/node()', lambda x: x[0] if isinstance(x, list) else x)
}
