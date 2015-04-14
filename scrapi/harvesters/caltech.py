"""
Harvester for the CaltechAUTHORS repository for the SHARE project

"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class CaltechHarvester(OAIHarvester):
    short_name = 'caltech'
    long_name = 'CaltechAUTHORS'
    url = 'http://authors.library.caltech.edu//'

    base_url = 'http://authors.library.caltech.edu/cgi/oai2'
    property_list = [
        'date', 'resource type', 'format', 'resource identifier', 'relation'
    ]
