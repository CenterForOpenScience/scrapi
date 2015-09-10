'''
Harvester for the Trace: Tennessee Research and Creative Exchange for the SHARE project
Example API call: http://trace.tennessee.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UtnknoxvilleHarvester(OAIHarvester):
    short_name = 'utnknoxville'
    long_name = 'Trace: Tennessee Research and Creative Exchange'
    url = 'http://trace.tennessee.edu'

    base_url = 'http://trace.tennessee.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
