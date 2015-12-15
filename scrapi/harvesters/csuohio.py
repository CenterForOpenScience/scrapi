'''
Harvester for the EngagedScholarship@CSU for the SHARE project

Example API call: http://engagedscholarship.csuohio.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CsuohioHarvester(OAIHarvester):
    short_name = 'csuohio'
    long_name = 'Cleveland State University\'s EngagedScholarship@CSU'
    url = 'http://engagedscholarship.csuohio.edu'

    base_url = 'http://engagedscholarship.csuohio.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
