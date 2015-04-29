"""
Harvests Carnegie Mellon Research Showcase metadata for ingestion into the SHARE service

More information available at: https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.cmu.md

Example API call: http://repository.cmu.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-14
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CMUHarvester(OAIHarvester):
    short_name = 'cmu'
    long_name = 'Carnegie Mellon University Research Showcase'
    url = 'http://repository.cmu.edu/'
    base_url = 'http://repository.cmu.edu/do/oai/'
    property_list = [
        'type', 'format', 'date', 'identifier',
        'setSpec', 'description'
    ]
