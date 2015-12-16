'''
Harvester for the Washington State University Research Exchange for the SHARE project

Example API call: http://research.wsulibs.wsu.edu:8080/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class WashuHarvester(OAIHarvester):
    short_name = 'wash_state_u'
    long_name = 'Washington State University Research Exchange'
    url = 'http://research.wsulibs.wsu.edu/xmlui/'

    base_url = 'http://research.wsulibs.wsu.edu:8080/oai/request'
    property_list = ['identifier', 'date', 'format', 'type', 'setSpec']
    timezone_granularity = True
