'''
Harvester for the UTC Scholar for the SHARE project

Example API call: http://scholar.utc.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class Ut_chattanoogaHarvester(OAIHarvester):
    short_name = 'ut_chattanooga'
    long_name = 'University of Tennessee at Chattanooga'
    url = 'http://scholar.utc.edu'

    base_url = 'http://scholar.utc.edu/do/oai/'
    property_list = ['type', 'format', 'identifier', 'date', 'source', 'setSpec', 'publisher', 'language', 'rights']

    approved_sets = [
        u'honors-theses',
        u'theses',
    ]

    timezone_granularity = True
