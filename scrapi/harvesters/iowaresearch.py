'''
Harvester for the Iowa Research Online for the SHARE project

Example API call: http://ir.uiowa.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''

from __future__ import unicode_literals
from scrapi.base import OAIHarvester


class IowaresearchHarvester(OAIHarvester):
    short_name = 'iowaresearch'
    long_name = 'Iowa Research Online'
    url = 'http://ir.uiowa.edu'

    base_url = 'http://ir.uiowa.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type']
    timezone_granularity = True
