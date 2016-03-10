'''
Harvester for the eScholarship@UMMS for the SHARE project

Example API call: http://escholarship.umassmed.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UmassmedHarvester(OAIHarvester):
    short_name = 'umassmed'
    long_name = 'eScholarship@UMMS'
    url = 'http://escholarship.umassmed.edu'

    base_url = 'http://escholarship.umassmed.edu/do/oai/'
    property_list = ['rights', 'source', 'relation', 'date', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
