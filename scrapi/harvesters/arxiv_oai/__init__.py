"""
Harvests arXiv metadata for ingestion into the SHARE service

Example API call: http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-10
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


arxiv_oai = OAIHarvester(
    name='arxiv_oai',
    timeout=30,
    base_url='http://export.arxiv.org/oai2',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec', 'description']
)

harvest = arxiv_oai.harvest
normalize = arxiv_oai.normalize
