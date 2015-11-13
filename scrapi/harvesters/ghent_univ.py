"""
Harvester for Ghent University for the SHARE NS
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class GhentUniversity(OAIHarvester):
    short_name = 'ghent'
    long_name = 'Ghent University'
    url = 'https://biblio.ugent.be/'

    base_url = 'https://biblio.ugent.be/oai'
    timezone_granularity = True

    property_list = [
        'type', 'source', 'setSpec',
        'format', 'identifier'
    ]
