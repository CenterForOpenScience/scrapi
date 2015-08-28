'''
Harvester for the DigitalCommons@PCOM for the SHARE project

Example API call: http://digitalcommons.pcom.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class PcomHarvester(OAIHarvester):
    short_name = 'pcom'
    long_name = 'DigitalCommons@PCOM'
    url = 'http://digitalcommons.pcom.edu'

    base_url = 'http://digitalcommons.pcom.edu/do/oai/'
    property_list = ['date', 'source', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True

    approved_sets = [u'biomed', u'pa_systematic_reviews', u'psychology_dissertations',
                     u'scholarly_papers', u'research_day', u'posters']
