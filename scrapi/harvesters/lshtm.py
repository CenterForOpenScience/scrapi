'''
Harvester for the LSHTM Research Online for the SHARE project

Example API call: http://researchonline.lshtm.ac.uk/cgi/oai2?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class LshtmHarvester(OAIHarvester):
    short_name = 'lshtm'
    long_name = 'London School of Hygiene and Tropical Medicine Research Online'
    url = 'http://researchonline.lshtm.ac.uk'

    base_url = 'http://researchonline.lshtm.ac.uk/cgi/oai2'
    property_list = ['date', 'type', 'identifier', 'relation', 'setSpec']
    timezone_granularity = True
