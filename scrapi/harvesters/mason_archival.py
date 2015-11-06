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
        u'col_1920_6102',
        u'col_1920_6039',
        u'com_1920_262',
        u'com_1920_466',
        u'com_1920_1320',
        u'com_1920_2852',
        u'com_1920_2869',
        u'com_1920_2883',
        u'com_1920_3011',
        u'com_1920_7520',
        u'com_1920_8132',
        u'com_1920_8138',
        u'col_1920_13'
    ]
