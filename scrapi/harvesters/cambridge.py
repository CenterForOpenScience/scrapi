'''
Harvester for the DSpace at Cambridge (production) for the SHARE project

Example API call: https://www.repository.cam.ac.uk/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CambridgeHarvester(OAIHarvester):
    short_name = 'cambridge'
    long_name = 'DSpace at Cambridge (production)'
    url = 'https://www.repository.cam.ac.uk'

    base_url = 'https://www.repository.cam.ac.uk/oai/request'
    property_list = ['date', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
