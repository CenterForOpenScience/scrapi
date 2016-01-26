'''
Harvester for the New Prairie Press for the SHARE project

Example API call: http://newprairiepress.org/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class Npp_ksuHarvester(OAIHarvester):
    short_name = 'npp_ksu'
    long_name = 'New Prairie Press at Kansas State University'
    url = 'http://newprairiepress.org'

    base_url = 'http://newprairiepress.org/do/oai/'
    property_list = ['identifier', 'source', 'date', 'type', 'format', 'setSpec']
    timezone_granularity = True
