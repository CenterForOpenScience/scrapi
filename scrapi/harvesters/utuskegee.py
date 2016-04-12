'''
Harvester for the Tuskegee University Archives for the SHARE project

Example API call: http://192.203.127.197/archive/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UtuskegeeHarvester(OAIHarvester):
    short_name = 'utuskegee'
    long_name = 'Tuskegee University'
    url = 'http://192.203.127.197'

    base_url = 'http://192.203.127.197/archive/oai/request'
    property_list = ['date', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
