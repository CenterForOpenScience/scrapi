'''
Harvester for the Inter-University Consortium for Political and Social Research for the SHARE project

Example API call: http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class IcpsrHarvester(OAIHarvester):
    short_name = 'icpsr'
    long_name = 'Inter-University Consortium for Political and Social Research'
    url = 'http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies'

    base_url = 'http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies'
    property_list = ['date', 'identifier', 'type', 'coverage']
    timezone_granularity = False
