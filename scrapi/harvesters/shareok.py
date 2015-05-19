"""
Harvester for the SHAREOK Repository Repository for the SHARE project

Example API call: https://shareok.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ShareOKHarvester(OAIHarvester):
    short_name = 'shareok'
    long_name = 'SHAREOK Repository'
    url = 'https://shareok.org'
    timezone_granularity = True

    base_url = 'https://shareok.org/oai/request'

    # TODO - add date back in once we fix elasticsearch mapping
    property_list = [
        'type', 'source', 'format',
        'description', 'setSpec'
    ]
    approved_sets = [
        'com_11244_14447',
        'com_11244_1',
        'col_11244_14248',
        'com_11244_6231',
        'col_11244_7929',
        'col_11244_7920',
        'col_11244_10476',
        'com_11244_10465',
        'com_11244_10460',
        'col_11244_10466',
        'col_11244_10464',
        'col_11244_10462'
    ]
