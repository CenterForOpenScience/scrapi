'''
Harvester for the CSIR Researchspace for the SHARE project

Example API call: http://researchspace.csir.co.za/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CsirHarvester(OAIHarvester):
    short_name = 'csir'
    long_name = 'CSIR Researchspace'
    url = 'http://researchspace.csir.co.za'

    base_url = 'http://researchspace.csir.co.za/oai/request'
    property_list = ['rights', 'format', 'source', 'date', 'identifier', 'type', 'setSpec']
    timezone_granularity = True
