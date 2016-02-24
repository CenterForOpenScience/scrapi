'''
Harvester for the KU ScholarWorks for the SHARE project

Example API call: https://kuscholarworks.ku.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UkansasHarvester(OAIHarvester):
    short_name = 'ukansas'
    long_name = 'KU ScholarWorks'
    url = 'https://kuscholarworks.ku.edu'

    base_url = 'https://kuscholarworks.ku.edu/oai/request'
    property_list = ['date', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
