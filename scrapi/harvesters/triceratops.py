'''
Harvester for the Triceratops: Tri-College Digital Repository for the SHARE project

Example API call: http://triceratops.brynmawr.edu/dspace-oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class TriceratopsHarvester(OAIHarvester):
    short_name = 'triceratops'
    long_name = 'Triceratops: Tri-College Digital Repository'
    url = 'http://triceratops.brynmawr.edu'

    base_url = 'http://triceratops.brynmawr.edu/dspace-oai/request'
    property_list = ['date', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
