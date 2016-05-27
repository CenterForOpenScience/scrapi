'''
Harvester for the MLA commons for the SHARE project

Example API call: https://commons.mla.org/deposits/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class MlaHarvester(OAIHarvester):
    short_name = 'mla'
    long_name = 'MLA Commons'
    url = 'https://commons.mla.org'

    base_url = 'https://commons.mla.org/deposits/oai/'
    property_list = ['date', 'identifier', 'setSpec']
    timezone_granularity = True
