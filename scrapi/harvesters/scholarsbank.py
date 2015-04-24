"""
Harvester for Scholars Bank University of Oregon for the SHARE project

Example API call: http://scholarsbank.uoregon.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester
from scrapi.base.helpers import updated_schema


class ScholarsbankHarvester(OAIHarvester):
    short_name = 'scholarsbank'
    long_name = 'Scholars Bank University of Oregon'
    url = 'http://scholarsbank.uoregon.edu'
    timezone_granularity = True

    base_url = 'http://scholarsbank.uoregon.edu/oai/request'
    property_list = [
        'type', 'source', 'format', 'relation',
        'date', 'description', 'setSpec', 'identifier'
    ]
