"""
A harvester for the Columbia academic commons for the SHARE project

More information available at: https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.columbia.academiccommons.md

Example API call: http://academiccommons.columbia.edu/catalog/oai?verb=ListRecords&from=2014-10-01T00:00:00Z&until=2014-10-02T00:00:00Z&metadataPrefix=oai_dc
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class ColumbiaHarvester(OAIHarvester):
    short_name = 'columbia'
    long_name = 'Columbia Academic Commons'
    url = 'http://academiccommons.columbia.edu/'

    base_url = 'http://academiccommons.columbia.edu/catalog/oai'
    timezone_granularity = True
    property_list = [
        'type', 'format', 'date', 'identifier', 'setSpec'
    ]
