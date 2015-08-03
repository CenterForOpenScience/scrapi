'''
Harvester for the CyberLeninka - Russian open access scientific library for the SHARE project

Example API call: http://cyberleninka.ru/oai?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CyberleninkaHarvester(OAIHarvester):
    short_name = 'cyberleninka'
    long_name = 'CyberLeninka - Russian open access scientific library'
    url = 'http://cyberleninka.ru/'
    force_request_update = True

    namespaces = {
        'dc': 'http://purl.org/dc/elements/1.1/',
        'ns0': 'http://www.openarchives.org/OAI/2.0/',
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
        'dcterms': 'http://purl.org/dc/terms/',
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'bibo': 'http://purl.org/ontology/bibo/'
    }

    base_url = 'http://cyberleninka.ru/oai'
    property_list = ['isPartOf', 'type', 'format', 'issue', 'issn', 'pages',
                     'bibliographicCitation', 'uri', 'date', 'identifier', 'type']
    timezone_granularity = True
