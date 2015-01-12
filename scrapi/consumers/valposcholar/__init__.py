from scrapi.base import OAIHarvester


valposcholar = OAIHarvester(
    name='valposcholar',
    base_url='http://scholar.valpo.edu/do/oai/',
    property_list=['type', 'source'],
    approved_sets=True
)

consume = valposcholar.harvest
normalize = valposcholar.normalize
