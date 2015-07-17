'''
Harvester for the Inter-University Consortium for Political and Social Research for the SHARE project

Example API call: http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import helpers
from scrapi.base import OAIHarvester


class IcpsrHarvester(OAIHarvester):
    short_name = 'icpsr'
    long_name = 'Inter-University Consortium for Political and Social Research'
    url = 'http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": {
                "canonicalUri": ('//dc:identifier/node()', helpers.compose(create_icpsr_url, helpers.single_result)),
                "objectUris": [('//dc:identifier/node()', icpsr_exttract_doi)]
            }
        })

    base_url = 'http://www.icpsr.umich.edu/icpsrweb/ICPSR/oai/studies'
    property_list = ['date', 'identifier', 'type', 'coverage']
    timezone_granularity = False


def create_icpsr_url(identifier):
    return 'http://www.icpsr.umich.edu/icpsrweb/ICPSR/studies/{}'.format(identifier)


def icpsr_exttract_doi(identifiers):
    return ['http://dx.doi.org/{}'.format(item) for item in identifiers if '10.' in item]
