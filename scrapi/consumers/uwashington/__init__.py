from __future__ import unicode_literals

from scrapi.base import OAIHarvester


uwashington = OAIHarvester(
    name='uwashington',
    base_url='http://digital.lib.washington.edu/dspace-oai/',
    property_list=['type', 'source', 'publisher', 'format', 'date',
                   'identifier', 'setSpec', 'rights', 'language']
)

consume = uwashington.harvest
normalize = uwashington.normalize
