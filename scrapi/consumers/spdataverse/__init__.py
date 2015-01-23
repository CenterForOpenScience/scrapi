from __future__ import unicode_literals

from scrapi.base import OAIHarvester


spdataverse = OAIHarvester(
    name='spdataverse',
    base_url='http://dataverse.scholarsportal.info/dvn/OAIHandler',
    property_list=['type', 'source', 'publisher', 'format', 'relation',
                   'description', 'coverage', 'rights', 'setSpec']
)

consume = spdataverse.harvest
normalize = spdataverse.normalize
