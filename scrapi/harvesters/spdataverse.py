"""
Harvester for the Scholars Portal dataverse.

More information here: https://github.com/CenterForOpenScience/SHARE/blob/master/providers/info.scholarsportal.dataverse.md
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class SPDataverse(OAIHarvester):
    short_name = 'spdataverse'
    long_name = 'Scholars Portal dataverse'
    base_url = 'http://dataverse.scholarsportal.info/dvn/OAIHandler'
    property_list = [
        'type', 'source', 'publisher', 'format', 'relation',
        'description', 'coverage', 'rights', 'setSpec', 'date'
    ]
