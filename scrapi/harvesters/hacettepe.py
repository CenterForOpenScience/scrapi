'''
Harvester for the DSpace on LibLiveCD for the SHARE project

Example API call: http://bbytezarsivi.hacettepe.edu.tr/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class HacettepeHarvester(OAIHarvester):
    short_name = 'hacettepe'
    long_name = 'Hacettepe University DSpace on LibLiveCD'
    url = 'http://bbytezarsivi.hacettepe.edu.tr'

    base_url = 'http://bbytezarsivi.hacettepe.edu.tr/oai/request'
    property_list = ['date', 'identifier', 'type', 'rights']
    timezone_granularity = True
