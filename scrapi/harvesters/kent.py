'''
Harvester for the Digital Commons @ Kent State University Libraries for the SHARE project

Example API call: http://digitalcommons.kent.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class KentHarvester(OAIHarvester):
    short_name = 'kent'
    long_name = 'Digital Commons @ Kent State University Libraries'
    url = 'http://digitalcommons.kent.edu'

    base_url = 'http://digitalcommons.kent.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
