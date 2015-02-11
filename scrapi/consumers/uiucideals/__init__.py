from __future__ import unicode_literals

from scrapi.base import OAIHarvester


uiucideals = OAIHarvester(
    name='uiucideals',
    base_url='http://ideals.uiuc.edu/dspace-oai/request',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec', 'source', 'coverage',
                   'relation', 'rights']
)

consume = uiucideals.harvest
normalize = uiucideals.normalize
