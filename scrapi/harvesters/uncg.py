"""
This is a harvester for the Walter Clinton Jackson Library at the University of North Carolina
at Greensboro for the SHARE project

Example API call: http://libres.uncg.edu/ir/oai/oai.aspx?verb=listrecords&set=uncg&metadataprefix=oai_dc
"""


from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UNCGHarvester(OAIHarvester):
    short_name = 'uncg'
    long_name = 'UNC-Greensboro'
    url = 'http://libres.uncg.edu/ir'
    base_url = 'http://libres.uncg.edu/ir/oai/oai.aspx'
    approved_sets = ['UNCG']
