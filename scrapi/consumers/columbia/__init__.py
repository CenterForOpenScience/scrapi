from __future__ import unicode_literals

from scrapi.base import OAIHarvester


columbia = OAIHarvester(
    name='columbia',
    base_url='http://academiccommons.columbia.edu/catalog/oai',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language']
)

consume = columbia.harvest
normalize = columbia.normalize
