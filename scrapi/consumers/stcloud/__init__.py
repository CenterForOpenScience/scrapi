"""
A harvester for the Repository at St Cloud State for the SHARE project

More information at: https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.stcloudstate.md

An example API call: http://repository.stcloudstate.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-26T00:00:00Z
"""


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
