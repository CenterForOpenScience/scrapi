'''
Harvester for the Washington University Open Scholarship for the SHARE project

Example API call: http://openscholarship.wustl.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class WustlopenscholarshipHarvester(OAIHarvester):
    short_name = 'wustlopenscholarship'
    long_name = 'Washington University Open Scholarship'
    url = 'http://openscholarship.wustl.edu/do/oai/'

    base_url = 'http://openscholarship.wustl.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format']
    timezone_granularity = True
