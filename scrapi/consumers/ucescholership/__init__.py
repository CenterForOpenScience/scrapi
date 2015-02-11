from __future__ import unicode_literals

from scrapi.base import OAIHarvester


ucescholarship = OAIHarvester(
    name='ucescholarship',
    base_url='http://www.escholarship.org/uc/oai',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec', 'source', 'coverage',
                   'relation', 'rights']
)

consume = ucescholarship.harvest
normalize = ucescholarship.normalize
