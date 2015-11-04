"""
Harvester of PubMed Central for the SHARE notification service

Example API call: http://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-13&until=2015-04-14
"""

from __future__ import unicode_literals

from scrapi.base import helpers
from scrapi.base import OAIHarvester


def format_uris_pubmedcentral(*args):
    identifiers = helpers.gather_identifiers(args)
    provider_uris, object_uris = helpers.seperate_provider_object_uris(identifiers)

    for arg in args:
        if arg and 'oai:pubmedcentral.nih.gov:' in arg[0]:
            PMC_ID = arg[0].replace('oai:pubmedcentral.nih.gov:', '')
            canonical_uri = 'http://www.ncbi.nlm.nih.gov/pmc/articles/PMC' + PMC_ID

    if not canonical_uri:
        raise ValueError('No Canonical URI was returned for this record.')

    return {
        'canonicalUri': canonical_uri,
        'objectUris': object_uris,
        'providerUris': provider_uris
    }


class PubMedCentralHarvester(OAIHarvester):
    short_name = 'pubmedcentral'
    long_name = 'PubMed Central'
    url = 'http://www.ncbi.nlm.nih.gov/pmc/'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": ('//ns0:header/ns0:identifier/node()', '//dc:identifier/node()', format_uris_pubmedcentral)
        })

    base_url = 'http://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi'
    property_list = [
        'type', 'source', 'rights',
        'format', 'setSpec', 'date', 'identifier'
    ]
