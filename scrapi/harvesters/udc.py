"""
A harvester for the Digital Conservancy for the SHARE project

Example API call: http://conservancy.umn.edu/oai/request
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UDCHarvester(OAIHarvester):
    short_name = 'udc'
    long_name = "University of Minnesota, Digital Conservancy"
    url = 'http://conservancy.umn.edu/'

    base_url = 'http://conservancy.umn.edu/oai/request'

    approved_sets = [
        'com_11299_45272', 'com_11299_169792', 'com_11299_166578'
    ]
