# -*- coding: utf-8 -*-

'''
Harvester for the PAPYRUS - Depot institutionnel de l'Universite de Montreal for the SHARE project

Example API call: http://papyrus.bib.umontreal.ca/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base.helpers import updated_schema
from scrapi.base import OAIHarvester


def umontreal_language_processor(languages):

    if not languages:
        languages = []

    return languages


class UmontrealHarvester(OAIHarvester):
    short_name = 'umontreal'
    long_name = u"PAPYRUS - Dépôt institutionnel de l'Université de Montréal"
    url = 'http://papyrus.bib.umontreal.ca'

    base_url = 'http://papyrus.bib.umontreal.ca/oai/request'

    @property
    def schema(self):
        return updated_schema(self._schema, {
            'languages': ('//dc:language/node()', umontreal_language_processor)
        })

    property_list = ['identifier', 'type', 'format', 'setSpec']

    timezone_granularity = True
