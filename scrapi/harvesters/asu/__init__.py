"""
Harvester for the ASU Digital Repository for the SHARE project

More information at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.asu.md

Example API call: http://repository.asu.edu/oai-pmh?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-05T00:00:00Z
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


asu = OAIHarvester(
    name='asu',
    base_url='http://repository.asu.edu/oai-pmh',
    property_list=['type', 'source', 'publisher', 'format',
                   'date', 'description', 'relation', 'setSpec',
                   'rights', 'identifier'],
    approved_sets=['research']
)

harvest = asu.harvest
normalize = asu.normalize
