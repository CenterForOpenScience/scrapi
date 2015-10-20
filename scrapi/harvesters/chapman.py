'''
Harvester for the Chapman University Digital Commons for the SHARE project

Example API call: http://digitalcommons.chapman.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ChapmanHarvester(OAIHarvester):
    short_name = 'chapman'
    long_name = 'Chapman University Digital Commons'
    url = 'http://digitalcommons.chapman.edu'

    base_url = 'http://digitalcommons.chapman.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
