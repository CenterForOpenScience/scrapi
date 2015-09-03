'''
Harvester for the ScholarsArchive@OSU for the SHARE project

Example API call: http://ir.library.oregonstate.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import helpers
from scrapi.base import OAIHarvester


class ScholarsarchiveosuHarvester(OAIHarvester):
    short_name = 'scholarsarchiveosu'
    long_name = 'ScholarsArchive@OSU'
    url = 'http://ir.library.oregonstate.edu/'

    base_url = 'http://ir.library.oregonstate.edu/oai/request'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": ('//dc:identifier/node()', helpers.oai_process_uris)
        })

    # TODO - return date once we figure out es parsing errors
    property_list = ['relation', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
