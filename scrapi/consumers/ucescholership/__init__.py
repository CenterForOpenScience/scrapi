from __future__ import unicode_literals

from scrapi.base import OAIHarvester


escholarship = OAIHarvester(
    name='escholarship',
    base_url='http://www.escholarship.org/uc/oai',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec', 'source', 'coverage',
                   'relation', 'rights']
)

consume = escholarship.harvest
normalize = escholarship.normalize
