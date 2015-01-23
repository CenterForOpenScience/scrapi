from __future__ import unicode_literals

from scrapi.base import OAIHarvester


texasstate = OAIHarvester(
    name='texasstate',
    base_url='http://digital.library.txstate.edu/oai/request',
    property_list=['type', 'publisher', 'format', 'date',
                   'identifier', 'language', 'setSpec', 'source', 'coverage',
                   'relation', 'rights'],
    approved_sets=[
        'engine_faculty',
        'env_studocs',
        'geo_faculty',
        'geo_honors',
        'geo_studocs',
        'global-awareness',
        'hca_faculty',
        'hct_honors',
        'hist_faculty',
        'hist_honors',
        'infolit_qep',
        'infolit_usra',
        'lib_digitalcommons',
        'lib_docs',
        'lib_faculty',
        'math_faculty',
        'math_honors',
        'mll_faculty',
        'mll_honors',
        'mono',
        'music_honors',
        'oaweek',
        'phil_faculty',
        'phil_honors',
        'physics_faculty',
        'physics_honors',
        'polysci_faculty',
        'polysci_studocs',
        'psych_faculty',
        'psych_honors',
        'relig_faculty',
        'socanthro_faculty',
        'socanthro_honors',
        'socanthro_studocs',
        'speechdrama_honors',
        'urban_studocs',
        'written-communication'
    ]
)

consume = texasstate.harvest
normalize = texasstate.normalize
