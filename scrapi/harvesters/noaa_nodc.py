"""
Harvester of National Oceanic and Atmosphere Administration's National Oceanographic Data Center

Example API call: http://data.nodc.noaa.gov/cgi-bin/oai-pmh?verb=GetRecord&metadataPrefix=oai_dc&identifier=gov.noaa.nodc:0110317

"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class NODCHarvester(OAIHarvester):
    short_name = 'noaa_nodc'
    long_name = 'National Oceanographic Data Center'
    url = 'https://www.nodc.noaa.gov/'

    base_url = 'http://data.nodc.noaa.gov/cgi-bin/oai-pmh'
    property_list = [
        'type', 'format', 'coverage', 'date', 'identifier',
    ]
    timezone_granularity = True
