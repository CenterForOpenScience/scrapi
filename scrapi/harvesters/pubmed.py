"""
Harvester of pubmed for the SHARE notification service
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class PubMedHarvester(OAIHarvester):
    short_name = 'pubmed'
    long_name = 'PubMed'
    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi'
    property_list = [
        'type', 'source', 'publisher', 'rights',
        'format', 'setSpec', 'date', 'identifier'
    ]
