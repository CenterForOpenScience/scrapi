'''
Harvester for the K-State Research Exchange for the SHARE project

Example API call: http://krex.k-state.edu/dspace-oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class KrexHarvester(OAIHarvester):
    short_name = 'krex'
    long_name = 'K-State Research Exchange'
    url = 'http://krex.k-state.edu'

    base_url = 'http://krex.k-state.edu/dspace-oai/request'
    property_list = ['type', 'date', 'identifier', 'setSpec']
    timezone_granularity = True
