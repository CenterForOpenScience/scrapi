from __future__ import unicode_literals

from dateutil.parser import parse

from .helpers import (
    default_name_parser,
    oai_extract_url,
    # oai_extract_doi,
    oai_process_contributors,
    compose,
    single_result,
    format_tags
)

CONSTANT = lambda x: lambda *_, **__: x


BASEXMLSCHEMA = {
    "description": ('//dc:description/node()', compose(lambda x: unicode(x.strip()), single_result)),
    "contributors": ('//dc:creator/node()', compose(lambda x: default_name_parser(x.split(';')), single_result)),
    "title": ('//dc:title/node()', compose(lambda x: unicode(x.strip()), single_result)),
    "providerUpdatedDateTime": ('//dc:dateEntry/node()', compose(lambda x: unicode(x.strip()), single_result)),
    "uris": {
        "canonicalUri": ('//dcq:identifier-citation/node()', compose(lambda x: unicode(x.strip()), single_result)),
    }
}

OAISCHEMA = {
    "contributors": ('//dc:creator/node()', '//dc:contributor/node()', oai_process_contributors),
    "uris": {
        "canonicalUri": ('//dc:identifier/node()', oai_extract_url)
    },
    'providerUpdatedDateTime': ('//ns0:header/ns0:datestamp/node()', lambda x: unicode(parse(x[0]).replace(tzinfo=None).isoformat())),
    'title': ('//dc:title/node()', lambda x: x[0] if isinstance(x, list) else x),
    'description': ('//dc:description/node()', lambda x: x[0] if isinstance(x, list) else x),
    'tags': ('//dc:subject/node()', format_tags)
}
