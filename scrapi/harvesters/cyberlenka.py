'''
Harvester for the CyberLeninka - Russian open access scientific library for the SHARE project

Example API call: http://cyberleninka.ru/oai?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CyberlenkaHarvester(OAIHarvester):
    short_name = 'cyberlenka'
    long_name = 'CyberLeninka - Russian open access scientific library'
    url = 'http://cyberleninka.ru/oai'

    base_url = 'http://cyberleninka.ru/oai'
    property_list = ['isPartOf', 'eissn', 'type', 'format', 'volume', 'issue',
                     'issn', 'pages', 'bibliographicCitation', 'uri', 'date',
                     'identifier', 'type']
    timezone_granularity = True
