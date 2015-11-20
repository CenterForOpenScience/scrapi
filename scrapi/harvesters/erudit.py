# coding=utf-8
'''
Harvester for the Erudit for the SHARE project

Example API call: http://oai.erudit.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class EruditHarvester(OAIHarvester):
    short_name = 'erudit'
    long_name = 'Érudit'
    url = 'http://erudit.org'

    base_url = 'http://oai.erudit.org/oai/request'
    property_list = ['date', 'type', 'identifier', 'relation', 'rights', 'setSpec']
    timezone_granularity = True
