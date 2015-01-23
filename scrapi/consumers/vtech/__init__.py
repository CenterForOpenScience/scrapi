from __future__ import unicode_literals

from scrapi.base import OAIHarvester


vtechworks = OAIHarvester(
    name='vtechworks',
    base_url='http://vtechworks.lib.vt.edu/oai/',
    property_list=['type', 'source', 'publisher', 'format', 'date',
                   'identifier', 'setSpec', 'rights', 'language', 'relation']
)

consume = vtechworks.harvest
normalize = vtechworks.normalize
