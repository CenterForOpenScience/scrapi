"""
Harvester for the SHAREOK Repository Repository for the SHARE project

Example API call: https://shareok.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ShareOKHarvester(OAIHarvester):
    short_name = 'shareok'
    long_name = 'SHAREOK Repository'
    url = 'https://shareok.org'
    timezone_granularity = True

    base_url = 'https://shareok.org/oai/request'
    property_list = [
        'type', 'source', 'format',
        'date', 'description', 'setSpec'
    ]
