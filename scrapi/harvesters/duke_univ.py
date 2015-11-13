"""
harvester for Duke University Libraries for the SHARE NS
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class DukeUniversityLib(OAIHarvester):
    short_name = 'duke'
    long_name = 'Duke University Libraries'
    url = 'http://dukespace.lib.duke.edu'

    base_url = 'http://dukespace.lib.duke.edu/dspace-oai/request'
    timezone_granularity = True

    property_list = [
        'type', 'source', 'setSpec',
        'format', 'identifier'
    ]

    approved_sets = [
        'hdl_10161_5894',
        'hdl_10161_4935',
        'hdl_10161_841',
        'hdl_10161_5738',
        'hdl_10161_7698',
        'hdl_10161_7413',
        'hdl_10161_840',
        'hdl_10161_5893',
        'hdl_10161_31',
        'hdl_10161_5387',
        'hdl_10161_1561',
        'hdl_10161_7406',
        'hdl_10161_7650',
        'hdl_10161_7606',
        'hdl_10161_7637',
        'hdl_10161_7623',
        'hdl_10161_7419',
        'hdl_10161_8357',
        'hdl_10161_8356',
        'hdl_10161_9203',
        'hdl_10161_410',
        'hdl_10161_7666',
        'hdl_10161_1707',
        'hdl_10161_4',
        'hdl_10161_6217',
        'hdl_10161_2840',
        'hdl_10161_4936',
        'hdl_10161_8127',
        'hdl_10161_875',
        'hdl_10161_9149',
        'hdl_10161_8115',
        'hdl_10161_9217',
        'hdl_10161_8914',
        'hdl_10161_9201',
        'hdl_10161_7409',
        'hdl_10161_2658',
        'hdl_10161_2493',
        'hdl_10161_7773',
        'hdl_10161_7682',
        'hdl_10161_3188',
        'hdl_10161_41',
        'hdl_10161_52',
        'hdl_10161_60',
        'hdl_10161_874',
        'hdl_10161_7417',
        'hdl_10161_8989',
        'hdl_10161_9283',
        'hdl_10161_10201',
        'hdl_10161_10200',
        'hdl_10161_2877',
        'hdl_10161_7410',
        'hdl_10161_5885',
        'hdl_10161_5886',
        'hdl_10161_3396',
        'hdl_10161_2841',
        'hdl_10161_1701',
        'hdl_10161_7415',
        'hdl_10161_9219',
        'hdl_10161_460',
        'hdl_10161_7408',
        'hdl_10161_7407',
        'hdl_10161_9218',
        'hdl_10161_8939',
        'hdl_10161_8164',
        'hdl_10161_1',
        'hdl_10161_6',
        'hdl_10161_4937',
        'hdl_10161_5895',
        'hdl_10161_8929',
        'hdl_10161_8108',
        'hdl_10161_5896'
    ]
