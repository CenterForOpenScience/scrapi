'''
Harvester for the ScholarWorks@UMass Amherst for the SHARE project

Example API call: http://scholarworks.umass.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ScholarworksumassHarvester(OAIHarvester):
    short_name = 'scholarworks_umass'
    long_name = 'ScholarWorks@UMass Amherst'
    url = 'http://scholarworks.umass.edu'

    base_url = 'http://scholarworks.umass.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format']
    timezone_granularity = True
