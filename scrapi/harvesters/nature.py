"""
Harvester for the Nature Repository for the SHARE project

Example API call:http://www.nature.com/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from=2016-03-05&until=2016-03-07"
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class NatureHarvester(OAIHarvester):
    short_name = 'nature'
    long_name = 'Nature Publishing Group'
    url = 'http://www.nature.com/'

    base_url = 'http://www.nature.com/oai/request'
    property_list = ['type', 'format', 'date', 'description', 'relation',
                     'setSpec', 'rights', 'identifier']
    timezone_granularity = False
