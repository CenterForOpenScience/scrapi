'''
Harvester for the NIST MaterialsData for the SHARE project

Example API call: https://materialsdata.nist.gov/dspace/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class NistHarvester(OAIHarvester):
    short_name = 'nist'
    long_name = 'NIST MaterialsData'
    url = 'https://materialsdata.nist.gov'

    base_url = 'https://materialsdata.nist.gov/dspace/oai/request'
    property_list = ['relation', 'rights', 'identifier', 'type', 'date', 'setSpec']
    timezone_granularity = True
