from __future__ import unicode_literals

from scrapi.base import OAIHarvester


stcloud = OAIHarvester(
    name='stcloud',
    base_url='http://repository.stcloudstate.edu/do/oai/',
    property_list=['type', 'source', 'publisher', 'format', 'setSpec', 'date'],
    approved_sets=[
        'ews_facpubs',
        'ews_wps',
        'hist_facwp',
        'comm_facpubs',
        'anth_facpubs',
        'soc_facpubs',
        'soc_ug_research',
        'chem_facpubs',
        'phys_present',
        'lrs_facpubs',
        'cfs_facpubs',
        'hurl_facpubs',
        'ed_facpubs',
        'cpcf_gradresearch',
        'econ_facpubs',
        'econ_wps',
        'econ_seminars',
        'stcloud_ling'
    ]
)

consume = stcloud.harvest
normalize = stcloud.normalize
