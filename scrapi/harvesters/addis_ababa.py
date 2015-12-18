'''
Harvester for the Addis Ababa University Institutional Repository for the SHARE project

Example API call: http://etd.aau.edu.et/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester
from scrapi.base import helpers


def format_uris_addis_ababa(*args):
    identifiers = helpers.gather_identifiers(args)
    provider_uris, object_uris = helpers.seperate_provider_object_uris(identifiers)

    for arg in args:
        if arg and 'http://hdl.handle.net/123456789/' in arg[0]:
            doc_id = arg[0].replace('http://hdl.handle.net/123456789/', '')
            canonical_uri = 'http://etd.aau.edu.et/handle/123456789/' + doc_id
            provider_uris = ['http://etd.aau.edu.et/handle/123456789/' + doc_id]

    if not canonical_uri:
        raise ValueError('No Canonical URI was returned for this record.')

    return {
        'canonicalUri': canonical_uri,
        'objectUris': object_uris,
        'providerUris': provider_uris
    }


class AauHarvester(OAIHarvester):
    short_name = 'addis_ababa'
    long_name = 'Addis Ababa University Institutional Repository'
    url = 'http://etd.aau.edu.et'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": ('//ns0:header/ns0:identifier/node()', '//dc:identifier/node()', format_uris_addis_ababa)
        })

    base_url = 'http://etd.aau.edu.et/oai/request'
    property_list = ['date', 'type', 'identifier', 'setSpec']
    timezone_granularity = True
