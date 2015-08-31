'''
Harvester for the The TAMU Digital Repository for the SHARE project

Example API call: http://oaktrust.library.tamu.edu/dspace-oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class OaktrustHarvester(OAIHarvester):
    short_name = 'oaktrust'
    long_name = 'The OAKTrust Digital Repository at Texas A&M'
    url = 'http://oaktrust.library.tamu.edu'

    base_url = 'http://oaktrust.library.tamu.edu/dspace-oai/request'
    # TODO -- Add date back once we fix es parsing
    property_list = ['identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
