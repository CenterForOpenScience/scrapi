'''
Harvester for the Washington University Open Scholarship for the SHARE project

Example API call: http://openscholarship.wustl.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class WustlopenscholarshipHarvester(OAIHarvester):
    short_name = 'wustlopenscholarship'
    long_name = 'Washington University Open Scholarship'
    url = 'http://openscholarship.wustl.edu'

    base_url = 'http://openscholarship.wustl.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True

    approved_sets = ['cse_research', 'facpubs', 'art_sci_facpubs',
                     'lib_research', 'artarch_facpubs', 'bio_facpubs', 'brown_facpubs',
                     'cfh_facpubs', 'engl_facpubs', 'hist_facpubs', 'math_facpubs',
                     'psych_facpubs', 'lib_present', 'lib_papers', 'wgssprogram']
