'''
Harvester for the CyberLeninka - Russian open access scientific library for the SHARE project

Example API call: http://cyberleninka.ru/oai?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CyberleninkaHarvester(OAIHarvester):
    short_name = 'cyberleninka'
    long_name = 'CyberLeninka - Russian open access scientific library'
    url = 'http://cyberleninka.ru/oai'

    base_url = 'http://cyberleninka.ru/oai'
    property_list = ['{http://purl.org/dc/terms/}isPartOf', '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}type', 'format', '{http://purl.org/ontology/bibo/}issue', '{http://purl.org/ontology/bibo/}issn', '{http://purl.org/ontology/bibo/}pages', '{http://purl.org/dc/terms/}bibliographicCitation', '{http://purl.org/ontology/bibo/}uri', 'date', 'identifier', 'type']
    timezone_granularity = True
