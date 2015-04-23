"""
Harvester for the University of Illinois at Urbana-Champaign, IDEALS

information about UIUC-IDEALS can be found here:
https://github.com/CenterForOpenScience/SHARE/blob/master/providers/UIUC-IDEALS.md
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UiucIdealsHarvester(OAIHarvester):
    long_name = 'University of Illinois at Urbana-Champaign, IDEALS'
    short_name = 'uiucideals'
    url = 'https://www.ideals.illinois.edu'
    base_url = 'http://ideals.uiuc.edu/dspace-oai/request'
    property_list = [
        'type', 'format', 'date',
        'identifier', 'setSpec', 'source', 'coverage',
        'relation', 'rights'
    ]
