"""
Harvester for the University of Illinois at Urbana-Champaign, IDEALS

information about UIUC-IDEALS can be found here:
https://github.com/CenterForOpenScience/SHARE/blob/master/providers/UIUC-IDEALS.md
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


uiucideals = OAIHarvester(
    name='uiucideals',
    base_url='http://ideals.uiuc.edu/dspace-oai/request',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec', 'source', 'coverage',
                   'relation', 'rights']
)

harvest = uiucideals.harvest
normalize = uiucideals.normalize
