"""
Harvester of PubMed Central for the SHARE notification service

Example API call: http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-13&until=2015-04-14
"""


from __future__ import unicode_literals

from scrapi.base import schemas
from scrapi.base import helpers
from scrapi.base import OAIHarvester


def oai_extract_url_pubmedcentral(header):
    PMC_ID = header.replace('oai:pubmedcentral.nih.gov:', '')
    return 'http://www.ncbi.nlm.nih.gov/pmc/articles/PMC' + PMC_ID


class PubMedCentralHarvester(OAIHarvester):
    short_name = 'pubmedcentral'
    long_name = 'PubMed Central'
    url = 'http://www.ncbi.nlm.nih.gov/pmc/'

    schema = helpers.updated_schema(
        schemas.OAISCHEMA,
        {
            "uris": {
                "canonicalUri": ('//ns0:header/ns0:identifier/node()', oai_extract_url_pubmedcentral)
            }
        }
    )

    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi'
    property_list = [
        'type', 'source', 'publisher', 'rights',
        'format', 'setSpec', 'date', 'identifier'
    ]
