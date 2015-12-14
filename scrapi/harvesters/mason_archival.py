"""
Harvester for Mason Archival Repository Service for the SHARE NS
"""

from __future__ import unicode_literals
from scrapi.base import OAIHarvester


class MasonArchival(OAIHarvester):
    short_name = 'mason'
    long_name = 'Mason Archival Repository Service'
    url = 'http://mars.gmu.edu/'

    base_url = 'http://mars.gmu.edu/oai/request'
    timezone_granularity = True

    property_list = [
        'type', 'source', 'setSpec',
        'format', 'identifier'
    ]
    approved_sets = [
        'col_1920_6102',
        'col_1920_6039',
        'com_1920_262',
        'com_1920_466',
        'com_1920_1320',
        'com_1920_2852',
        'com_1920_2869',
        'com_1920_2883',
        'com_1920_3011',
        'com_1920_7520',
        'com_1920_8132',
        'com_1920_8138',
        'col_1920_13',
        'com_1920_2811'
    ]
