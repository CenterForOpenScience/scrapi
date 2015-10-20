'''
Harvester for the Huskie Commons for the SHARE project

Example API call: http://commons.lib.niu.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class HuskiecommonsHarvester(OAIHarvester):
    short_name = 'huskiecommons'
    long_name = 'Huskie Commons'
    url = 'http://commons.lib.niu.edu'

    base_url = 'http://commons.lib.niu.edu/oai/request'
    property_list = ['date', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
