'''
Harvester for the ASU Digital Repository for the SHARE project

Example API call: https://zenodo.org/oai2d?verb=ListRecords&metadataPrefix=oai_dc
'''

from __future__ import unicode_literals
from scrapi.base import OAIHarvester


class ZenodoHarvester(OAIHarvester):
    short_name = 'zenodo'
    long_name = 'Zenodo'
    url = 'https://zenodo.org/oai2d'

    base_url = 'https://zenodo.org/oai2d'
    property_list = ['language', 'rights', 'source', 'relation', 'date', 'identifier', 'type']
    timezone_granularity = True
