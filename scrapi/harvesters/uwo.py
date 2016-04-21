"""Harvests MIT DSpace metadata for ingestion into the SHARE service

More information available here:
https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.mit.md

Example metadata URL: http://dspace.mit.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-28
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UWOHarvester(OAIHarvester):
    short_name = 'uwo'
    long_name = 'Western University'
    url = 'http://ir.lib.uwo.ca'

    base_url = 'http://ir.lib.uwo.ca/do/oai/'
    property_list = [
        'type', 'source', 'format', 'rights', 'identifier',
        'relation', 'date', 'description', 'setSpec'
    ]
