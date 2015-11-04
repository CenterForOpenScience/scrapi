'''
Harvester for the UKnowledge for the SHARE project

Example API call: http://uknowledge.uky.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UkyHarvester(OAIHarvester):
    short_name = 'uky'
    long_name = 'UKnowledge @ University of Kentucky'
    url = 'http://uknowledge.uky.edu'

    base_url = 'http://uknowledge.uky.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
