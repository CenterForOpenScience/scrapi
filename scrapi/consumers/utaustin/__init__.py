from __future__ import unicode_literals

from scrapi.base import OAIHarvester


utaustin = OAIHarvester(
    name='utaustin',
    base_url='http://repositories.lib.utexas.edu/oai/',
    property_list=['type', 'source', 'publisher', 'format', 'date', 'identifier', 'setSpec'],
    approved_sets=[
        'hdl_2152_1',
        'hdl_2152_13541',
        'hdl_2152_22957',
        'hdl_2152_13341',
        'hdl_2152_11183',
        'hdl_2152_15554',
        'hdl_2152_21116',
        'hdl_2152_11227',
        'hdl_2152_26',
        'hdl_2152_25673',
        'hdl_2152_21442',
        'hdl_2152_11019',
        'hdl_2152_10079',
        'hdl_2152_23952',
        'hdl_2152_19781',
        'hdl_2152_4',
        'hdl_2152_5',
        'hdl_2152_15265',
        'hdl_2152_20099',
        'hdl_2152_4027',
        'hdl_2152_22392',
        'hdl_2152_24880',
        'hdl_2152_24538',
        'hdl_2152_20329',
        'hdl_2152_14283',
        'hdl_2152_14697',
        'hdl_2152_16482',
        'hdl_2152_24831',
        'hdl_2152_11681',
        'hdl_2152_15722',
        'hdl_2152_7103',
        'hdl_2152_20398',
        'hdl_2152_7100',
        'hdl_2152_7105',
        'hdl_2152_7102',
        'hdl_2152_7101',
        'hdl_2152_17706',
        'hdl_2152_15040',
        'hdl_2152_14309',
        'hdl_2152_18015',
        'hdl_2152_6854',
        'hdl_2152_6851',
        'hdl_2152_15082'
    ]
)

consume = utaustin.harvest
normalize = utaustin.normalize
