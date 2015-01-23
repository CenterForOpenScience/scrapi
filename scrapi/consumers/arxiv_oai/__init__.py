from __future__ import unicode_literals

from scrapi.base import OAIHarvester


arxiv_oai = OAIHarvester(
    name='arxiv_oai',
    timeout=30,
    base_url='http://academiccommons.arxiv_oai.edu/catalog/oai',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec']
)

consume = arxiv_oai.harvest
normalize = arxiv_oai.normalize
