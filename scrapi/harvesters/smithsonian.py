'''
Harvester for the Smithsonian Digital Repository for the SHARE project

Example API call: http://repository.si.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class SiHarvester(OAIHarvester):
    short_name = 'smithsonian'
    long_name = 'Smithsonian Digital Repository'
    url = 'http://repository.si.edu/oai/request'

    base_url = 'http://repository.si.edu/oai/request'
    property_list = ['date', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
