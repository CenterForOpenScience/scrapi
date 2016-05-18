'''
Harvester for the NKU Institutional Repository for the SHARE project

Example API call: https://dspace.nku.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class NkuHarvester(OAIHarvester):
    short_name = 'nku'
    long_name = 'NKU Institutional Repository'
    url = 'https://dspace.nku.edu'

    base_url = 'https://dspace.nku.edu/oai/request'
    property_list = ['date', 'type', 'identifier', 'setSpec', 'publisher', 'language', 'relation']

    approved_sets = [u'com_11216_7', u'com_11216_20', u'col_11216_168']

    timezone_granularity = True
