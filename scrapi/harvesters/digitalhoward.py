'''
Harvester for the Digital Howard @ Howard University for the SHARE project

Example API call: http://dh.howard.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class HowardHarvester(OAIHarvester):
    short_name = 'digitalhoward'
    long_name = 'Digital Howard @ Howard University'
    url = 'http://dh.howard.edu'

    base_url = 'http://dh.howard.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'rights']
    timezone_granularity = True
