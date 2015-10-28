'''
Harvester for the "CiteSeerX Scientific Literature Digital Library and Search Engine" for the SHARE project

Example API call: http://citeseerx.ist.psu.edu/oai2?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CiteseerxHarvester(OAIHarvester):
    short_name = 'citeseerx'
    long_name = 'CiteSeerX Scientific Literature Digital Library and Search Engine'
    url = 'http://citeseerx.ist.psu.edu'

    base_url = 'http://citeseerx.ist.psu.edu/oai2'
    property_list = ['rights', 'format', 'source', 'date', 'identifier', 'type', 'setSpec']
    timezone_granularity = False
