"""
A harvester for Calhoun: The NPS Institutional Archive for the SHARE project

An example API call: http://calhoun.nps.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CalhounHarvester(OAIHarvester):
    short_name = 'calhoun'
    long_name = 'Calhoun: Institutional Archive of the Naval Postgraduate School'
    url = 'http://calhoun.nps.edu'
    verify = False

    base_url = 'http://calhoun.nps.edu/oai/request'
    property_list = [
        'type', 'source', 'format', 'setSpec', 'date', 'rights'
    ]
    approved_sets = [
        'com_10945_7075', 'com_10945_6', 'col_10945_17'
    ]
