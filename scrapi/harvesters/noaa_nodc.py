"""Harvester of National Oceanic and Atmosphere Administration's National Oceanographic Data Center

Example API call: http://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.nodc:00001339;view=xml

NODC provides an OAI-PMH interface at
http://data.nodc.noaa.gov/cgi-bin/oai-pmh, but it does not contain the
contributors key mandated by the SHARE-schema spec.  Therefore, we
have to use the XML interface, which contains much more metadata.
However, the XML interface is not searchable, so we still use the
OAI-PMH interface for queries. Fun!

"""

from __future__ import unicode_literals

from scrapi.base import helpers
from scrapi.base import OAIHarvester


def oai_extract_url_noaa_nodc(nodc_id):
    return 'http://data.nodc.noaa.gov/cgi-bin/iso?id=' + nodc_id


class NODCHarvester(OAIHarvester):
    short_name = 'noaa_nodc'
    long_name = 'National Oceanographic Data Center'
    url = 'https://www.nodc.noaa.gov/'

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": {
                "canonicalUri": ('//ns0:header/ns0:identifier/node()', helpers.compose(oai_extract_url_noaa_nodc, helpers.single_result))
            }
        })

    base_url = 'http://data.nodc.noaa.gov/cgi-bin/oai-pmh'
    property_list = [
        'type', 'format', 'coverage', 'date', 'identifier',
    ]
    timezone_granularity = True
