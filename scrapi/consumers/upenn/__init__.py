from __future__ import unicode_literals

from scrapi.base import OAIHarvester


upenn = OAIHarvester(
    name='upenn',
    timeout=30,
    base_url='http://repository.upenn.edu/do/oai/'
)

consume = upenn.harvest
normalize = upenn.normalize
