'''
Harvester for the RCAAP - Repositório Científico de Acesso Aberto de Portugal for the SHARE project

Example API call: http://www.rcaap.pt/oai?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class RcaapHarvester(OAIHarvester):
    short_name = 'rcaap'
    long_name = 'RCAAP - Repositório Científico de Acesso Aberto de Portugal'
    url = 'http://www.rcaap.pt'
    approved_sets = [u'portugal']

    base_url = 'http://www.rcaap.pt/oai'
    property_list = ['type', 'rights', 'date', 'format', 'identifier', 'setSpec']
    timezone_granularity = False
