from __future__ import unicode_literals

from datetime import date, timedelta

from lxml import etree

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.util import copy_to_unicode
from scrapi.base.schemas import DIFSCHEMA


class NCARHarvester(XMLHarvester):
    short_name = 'ncar'
    record_encoding = 'ISO-8859-1'
    long_name = 'Earth System Grid at NCAR'
    url = 'https://www.earthsystemgrid.org/home.html'

    namespaces = {
        'OAI-PMH': 'http://www.openarchives.org/OAI/2.0/',
        'dif': 'http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/'
    }

    schema = DIFSCHEMA

    def harvest(self, start_date=None, end_date=None):

        start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()
        end_date = (end_date or date.today()).isoformat()

        start_date += 'T00:00:00Z'
        end_date += 'T00:00:00Z'

        base_url = 'https://www.earthsystemgrid.org/oai/repository?verb=ListRecords&metadataPrefix=dif&from={}&until={}'

        url = base_url.format(start_date, end_date)

        data = requests.get(url)
        doc = etree.XML(data.content)

        records = doc.xpath('//OAI-PMH:record', namespaces=self.namespaces)

        xml_list = []
        for record in records:
            doc_id = record.xpath('//OAI-PMH:header/OAI-PMH:identifier/node()', namespaces=self.namespaces)[0]
            record = etree.tostring(record)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list
