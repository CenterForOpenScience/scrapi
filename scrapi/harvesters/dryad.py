'''
Harvester for the ASU Digital Repository for the SHARE project

Example API call: http://www.datadryad.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''

from __future__ import unicode_literals
from scrapi.base import OAIHarvester


class DryadHarvester(OAIHarvester):
    short_name = 'dryad'
    long_name = 'Dryad Data Repository'
    url = 'http://www.datadryad.org/oai/request'

    base_url = 'http://www.datadryad.org/oai/request'
    property_list = ['rights', 'format', 'relation', 'date', 'identifier', 'type']
    timezone_granularity = True
