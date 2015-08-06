'''
Harvester for the Smithsonian Digital Repository for the SHARE project

Example API call: http://repository.si.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

import re

from scrapi.base import helpers
from scrapi.base import OAIHarvester


class SiHarvester(OAIHarvester):
    short_name = 'smithsonian'
    long_name = 'Smithsonian Digital Repository'
    url = 'http://repository.si.edu'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": {
                "objectUris": [('//dc:identifier/node()', get_doi_from_identifier)]
            }
        })

    base_url = 'http://repository.si.edu/oai/request'
    property_list = ['date', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True


def get_doi_from_identifier(identifiers):
    doi_re = re.compile(r'10\.\S*\/\S*')
    identifiers = [identifiers] if not isinstance(identifiers, list) else identifiers
    for identifier in identifiers:
        try:
            found_doi = doi_re.search(identifier).group()
            return 'http://dx.doi.org/{}'.format(found_doi)
        except AttributeError:
            continue
