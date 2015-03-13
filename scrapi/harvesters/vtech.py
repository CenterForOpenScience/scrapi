"""
Harvests Virginia Tech VTechWorks metadata for ingestion into the SHARE service

Information about VTechWorks at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.vt.vtechworks.md

Example API call: http://vtechworks.lib.vt.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-29
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class VTechHarvester(OAIHarvester):
    short_name = 'vtech'
    long_name = 'Virginia Tech VTechWorks'
    url = 'https://vtechworks.lib.vt.edu'

    base_url = 'http://vtechworks.lib.vt.edu/oai/request'
    property_list = [
        'type', 'source', 'publisher', 'format', 'date',
        'identifier', 'setSpec', 'rights', 'language', 'relation'
    ]
