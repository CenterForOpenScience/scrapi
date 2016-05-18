'''
Harvester for the ScholarWorks for the SHARE project

Example API call: http://scholarworks.boisestate.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class Boise_stateHarvester(OAIHarvester):
    short_name = 'boise_state'
    long_name = 'Boise State University ScholarWorks'
    url = 'http://scholarworks.boisestate.edu'

    base_url = 'http://scholarworks.boisestate.edu/do/oai/'
    property_list = ['source', 'identifier', 'type', 'date', 'setSpec', 'publisher', 'rights', 'format']
    timezone_granularity = True
