"""
Harvester for the Cognitive Sciences ePrint Archive for the SHARE project

"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CogPrintsHarvester(OAIHarvester):
    short_name = 'cogprints'
    long_name = 'Cognitive Sciences ePrint Archive'
    url = 'http://www.cogprints.org/'

    base_url = 'http://cogprints.org/cgi/oai2'
    property_list = [
        'date', 'type', 'format', 'identifier', 'relation'
    ]
