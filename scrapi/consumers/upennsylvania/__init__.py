"""
Harvests University of Pennsylvania metadata for the SHARE NS

More information: https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.upenn.md

Example API call: http://repository.upenn.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-26
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


upenn = OAIHarvester(
    name='upenn',
    timeout=30,
    base_url='http://repository.upenn.edu/do/oai/'
)

consume = upenn.harvest
normalize = upenn.normalize
