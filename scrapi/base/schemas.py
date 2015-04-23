from __future__ import unicode_literals

from dateutil.parser import parse

from .helpers import (
    default_name_parser,
    oai_extract_url,
    # oai_extract_doi,
    oai_process_contributors,
    compose,
    single_result,
    format_tags,
    language_code
)

CONSTANT = lambda x: lambda *_, **__: x


BASEXMLSCHEMA = {
    "description": ('//dc:description/node()', compose(lambda x: x.strip(), single_result)),
    "contributors": ('//dc:creator/node()', compose(default_name_parser, lambda x: x.split(';'), single_result)),
    "title": ('//dc:title/node()', compose(lambda x: x.strip(), single_result)),
    "providerUpdatedDateTime": ('//dc:dateEntry/node()', compose(lambda x: x.strip(), single_result)),
    "uris": {
        "canonicalUri": ('//dcq:identifier-citation/node()', compose(lambda x: x.strip(), single_result)),
    }
}

OAISCHEMA = {
    "contributors": ('//dc:creator/node()', '//dc:contributor/node()', oai_process_contributors),
    "uris": {
        "canonicalUri": ('//dc:identifier/node()', oai_extract_url)
    },
    'providerUpdatedDateTime': ('//ns0:header/ns0:datestamp/node()', lambda x: parse(x[0]).replace(tzinfo=None).isoformat()),
    'title': ('//dc:title/node()', single_result),
    'description': ('//dc:description/node()', single_result),
    'tags': ('//dc:subject/node()', format_tags),
    'publisher': {
        'name': ('//dc:publisher/node()', single_result)
    },
    'languages': ('//dc:language', compose(lambda x: [x], language_code, single_result))
}
