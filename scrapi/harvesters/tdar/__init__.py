"""
Harvester for the The Digital Archaeological Record (tDAR) for the SHARE project

More information at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/org.tdar.md

Example API call: http://core.tdar.org/oai-pmh/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-05
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


tdar = OAIHarvester(
    name='tdar',
    base_url='http://core.tdar.org/oai-pmh/oai',
    property_list=['type', 'date', 'setSpec', 'type', 'publisher', 'coverage']
)

harvest = tdar.harvest
normalize = tdar.normalize
