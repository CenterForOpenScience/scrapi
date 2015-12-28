'''
Harvester for the MAXWELL for the SHARE project

Example API call: http://www.maxwell.vrac.puc-rio.br/DC_Todos.php?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester
from scrapi.base import helpers


def oai_process_pcurio(*args):

    identifiers = helpers.gather_identifiers(args)
    provider_uris, object_uris = helpers.seperate_provider_object_uris(identifiers)

    for i, uri in enumerate(provider_uris):
        if 'resultadon' in uri:
            doc_id = provider_uris[i].replace('http://www.maxwell.vrac.puc-rio.br/Busca_etds.php?strSecao=resultadonrSeq=', '')
            provider_uris[i] = 'http://www.maxwell.vrac.puc-rio.br/Busca_etds.php?strSecao=resultado&nrSeq=' + doc_id

    for i, uri in enumerate(object_uris):
        if 'resultadon' in uri:
            doc_id = object_uris[i].replace('http://www.maxwell.vrac.puc-rio.br/Busca_etds.php?strSecao=resultadonrSeq=', '')
            object_uris[i] = 'http://www.maxwell.vrac.puc-rio.br/Busca_etds.php?strSecao=resultado&nrSeq=' + doc_id

    potential_uris = (provider_uris + object_uris)

    try:
        canonical_uri = potential_uris[0]
    except IndexError:
        raise ValueError('No Canonical URI was returned for this record.')

    return {
        'canonicalUri': canonical_uri,
        'objectUris': object_uris,
        'providerUris': provider_uris
    }


class PcurioHarvester(OAIHarvester):
    short_name = 'pcurio'
    long_name = 'Pontifical Catholic University of Rio de Janeiro'
    url = 'http://www.maxwell.vrac.puc-rio.br'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": ('//ns0:header/ns0:identifier/node()', '//dc:identifier/node()', oai_process_pcurio)
        })

    base_url = 'http://www.maxwell.vrac.puc-rio.br/DC_Todos.php'
    property_list = ['date', 'identifier', 'type', 'setSpec']
    timezone_granularity = False
