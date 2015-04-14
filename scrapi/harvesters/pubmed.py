"""
Harvester of pubmed for the SHARE notification service
"""


from __future__ import unicode_literals

from scrapi.base import schemas
from scrapi.base import helpers
from scrapi.base import OAIHarvester


def oai_extract_url_pubmed(identifiers):
    identifiers = [identifiers] if not isinstance(identifiers, list) else identifiers
    for item in identifiers:
        try:
            found_url = helpers.URL_REGEX.search(item).group()
            if 'viewcontent' not in found_url and '/pubmed/' in found_url:
                return found_url.decode('utf-8')
        except AttributeError:
            continue


class PubMedHarvester(OAIHarvester):
    short_name = 'pubmed'
    long_name = 'PubMed Central'
    url = 'http://www.ncbi.nlm.nih.gov/pmc/'

    updated = {
        "uris": {
            "canonicalUri": ('//dc:identifier/node()', oai_extract_url_pubmed)
        }
    }

    schema = helpers.updated_schema(schemas.OAISCHEMA, updated)

    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi'
    property_list = [
        'type', 'source', 'publisher', 'rights',
        'format', 'setSpec', 'date', 'identifier'
    ]
