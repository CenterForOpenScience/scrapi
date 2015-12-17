'''
Harvester for the Addis Ababa University Institutional Repository for the SHARE project

Example API call: http://etd.aau.edu.et/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class AauHarvester(OAIHarvester):
    short_name = 'addis_ababa'
    long_name = 'Addis Ababa University Institutional Repository'
    url = 'http://etd.aau.edu.et'

    base_url = 'http://etd.aau.edu.et/oai/request'
    property_list = ['date', 'type', 'identifier', 'setSpec']
    timezone_granularity = True
