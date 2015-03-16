"""
Harvester for the ASU Digital Repository for the SHARE project

More information at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.asu.md

Example API call: http://repository.asu.edu/oai-pmh?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-05T00:00:00Z
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ASUHarvester(OAIHarvester):
    short_name = 'asu'
    long_name = 'Arizona State University Digital Repository'
    url = 'http://www.asu.edu/'

    base_url = 'http://repository.asu.edu/oai-pmh'
    approved_sets = ['research']
    property_list = [
        'type', 'source', 'publisher', 'format',
        'date', 'description', 'relation', 'setSpec',
        'rights', 'identifier'
    ]
