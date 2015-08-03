'''
Harvester for the Digital Access to Scholarship at Harvard for the SHARE project

Example API call: http://dash.harvard.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''

from __future__ import unicode_literals
from scrapi.base import OAIHarvester


class DashHarvester(OAIHarvester):
    short_name = 'dash'
    long_name = 'Digital Access to Scholarship at Harvard'
    url = 'http://dash.harvard.edu'

    base_url = 'http://dash.harvard.edu/oai/request'
    property_list = ['date', 'relation', 'identifier', 'type', 'rights']
    timezone_granularity = True
