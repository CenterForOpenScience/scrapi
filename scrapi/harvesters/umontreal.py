# -*- coding: utf-8 -*-

'''
Harvester for the PAPYRUS - Depot institutionnel de l'Universite de Montreal for the SHARE project

Example API call: http://papyrus.bib.umontreal.ca/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class UmontrealHarvester(OAIHarvester):
    short_name = 'umontreal'
    long_name = u"PAPYRUS - Dépôt institutionnel de l'Université de Montréal"
    url = 'http://papyrus.bib.umontreal.ca'

    base_url = 'http://papyrus.bib.umontreal.ca/oai/request'
    property_list = ['date', 'identifier', 'type', 'format', 'setSpec']
    timezone_granularity = True
