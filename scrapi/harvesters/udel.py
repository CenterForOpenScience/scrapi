'''
Harvester for the ASU Digital Repository for the SHARE project

Example API call: http://udspace.udel.edu/dspace-oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''

from __future__ import unicode_literals
from scrapi.base import OAIHarvester


class UdelHarvester(OAIHarvester):
    short_name = 'udel'
    long_name = 'University of Delaware Institutional Repository'
    url = 'http://udspace.udel.edu/dspace-oai/request'

    base_url = 'http://udspace.udel.edu/dspace-oai/request'

    # TODO - add date back to property list - udel has non-date
    # formats in their date field which elasticsearch does not enjoy
    property_list = ['identifier', 'type']
    timezone_granularity = True
