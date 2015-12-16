'''
Harvester for the DigitalCommons@University of Nebraska - Lincoln for the SHARE project

Example API call: http://digitalcommons.unl.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UnlHarvester(OAIHarvester):
    short_name = 'unl'
    long_name = 'DigitalCommons@University of Nebraska - Lincoln'
    url = 'http://digitalcommons.unl.edu'

    base_url = 'http://digitalcommons.unl.edu/do/oai/'
    property_list = ['type', 'identifier', 'format', 'date', 'source', 'setSpec']
    timezone_granularity = True
