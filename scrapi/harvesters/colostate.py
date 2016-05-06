'''
Harvester for the Digital Collections of Colorado for the SHARE project

Example API call: https://dspace.library.colostate.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ColostateHarvester(OAIHarvester):
    short_name = 'colostate'
    long_name = 'Digital Collections of Colorado'
    url = 'https://dspace.library.colostate.edu'

    base_url = 'https://dspace.library.colostate.edu/oai/request'
    property_list = ['source', 'relation', 'coverage', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
