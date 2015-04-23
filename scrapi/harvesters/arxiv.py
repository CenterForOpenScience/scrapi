"""
Harvests arXiv metadata for ingestion into the SHARE service

Example API call: http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-10
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ArxivHarvester(OAIHarvester):
    short_name = 'arxiv_oai'
    long_name = 'ArXiv'
    url = 'http://arxiv.org'
    timeout = 30
    base_url = 'http://export.arxiv.org/oai2'
    property_list = [
        'type', 'format', 'date',
        'identifier', 'setSpec', 'description'
    ]
