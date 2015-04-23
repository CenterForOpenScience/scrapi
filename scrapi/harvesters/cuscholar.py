"""
Harvester for the CU Scholar University of Colorado Boulder for the SHARE project

Example API call: http://scholar.colorado.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-22T00:00:00Z&to=2015-04-23
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CUScholarHarvester(OAIHarvester):
    short_name = 'cuscholar'
    long_name = 'CU Scholar University of Colorado Boulder'
    url = 'http://scholar.colorado.edu'
    timezone_granularity = True

    base_url = 'http://scholar.colorado.edu/do/oai/'
    property_list = [
        'type', 'source', 'format',
        'date', 'setSpec', 'identifier'
    ]
