'''
Harvester for the MOspace: University of Missouri for the SHARE project

Example API call: https://mospace.umsystem.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class MizzouHarvester(OAIHarvester):
    short_name = 'mizzou'
    long_name = 'MOspace: University of Missouri'
    url = 'https://mospace.umsystem.edu'

    base_url = 'https://mospace.umsystem.edu/oai/request'
    property_list = ['date', 'relation', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
