'''
Harvester for the Purdue University Research Repository for the SHARE project

Example API call: http://purr.purdue.edu/oaipmh?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class PurdueHarvester(OAIHarvester):
    short_name = 'purdue'
    long_name = 'PURR - Purdue University Research Repository'
    url = 'http://purr.purdue.edu'

    base_url = 'http://purr.purdue.edu/oaipmh'
    property_list = ['date', 'relation', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
