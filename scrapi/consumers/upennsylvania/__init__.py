"""
Harvests University of Pennsylvania metadata for the SHARE NS

More information: https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.upenn.md

Example API call: http://repository.upenn.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-26
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


upennsylvania = OAIHarvester(
    name='upennsylvania',
    base_url='http://repository.upenn.edu/do/oai/',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'setSpec', 'source', 'rights']
)

consume = upennsylvania.harvest
normalize = upennsylvania.normalize
