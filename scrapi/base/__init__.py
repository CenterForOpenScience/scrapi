# Classes for scrAPI Harvesters
from __future__ import unicode_literals

import abc
import logging
from datetime import date, timedelta

from lxml import etree

from scrapi import util
from scrapi import requests
from scrapi.linter import lint
from scrapi.base.schemas import OAISCHEMA
from scrapi.base.helpers import update_schema
from scrapi.base.transformer import XMLTransformer
from scrapi.linter.document import RawDocument, NormalizedDocument

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class BaseHarvester(object):
    """ This is a base class that all harvesters should inheret from

    Defines the copy to unicde method, which is useful for getting standard
    unicode out of xml results.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def harvest(self, days_back=1):
        raise NotImplementedError

    @abc.abstractmethod
    def normalize(self, raw_doc):
        raise NotImplementedError

    def lint(self):
        return lint(self.harvest, self.normalize)


class XMLHarvester(BaseHarvester, XMLTransformer):

    def normalize(self, raw_doc):
        transformed = self.transform(etree.XML(raw_doc['doc']))
        transformed['source'] = self.name
        return NormalizedDocument(transformed)


class OAIHarvester(XMLHarvester):
    """ Create a harvester with a oai_dc namespace, that will harvest
    documents within a certain date range

    Contains functions for harvesting from an OAI provider, normalizing,
    and outputting in a way that scrapi can understand, in the most
    generic terms possible.

    For more information, see the OAI PMH specification:
    http://www.openarchives.org/OAI/openarchivesprotocol.html
    """

    NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
                  'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
                  'ns0': 'http://www.openarchives.org/OAI/2.0/'}

    RECORDS_URL = '?verb=ListRecords'

    META_PREFIX_DATE = '&metadataPrefix=oai_dc&from={}'

    RESUMPTION = '&resumptionToken='

    DEFAULT_ENCODING = 'UTF-8'

    record_encoding = None

    def __init__(self, name, base_url, timezone_granularity=False, timeout=0.5, property_list=None, approved_sets=None):
        self.NAME = name
        self.base_url = base_url
        self.property_list = property_list or ['date', 'language', 'type']
        self.approved_sets = approved_sets
        self.timeout = timeout
        self.timezone_granularity = timezone_granularity

    @property
    def name(self):
        return self.NAME

    @property
    def namespaces(self):
        return self.NAMESPACES

    @property
    def schema(self):
        properties = {
            'properties': {
                item: (
                    '//dc:{}/node()'.format(item),
                    '//ns0:{}/node()'.format(item),
                    self.resolve_property
                ) for item in self.property_list
            }
        }
        return update_schema(OAISCHEMA, properties)

    def resolve_property(self, dc, ns0):
        if isinstance(dc, list) and isinstance(ns0, list):
            ret = dc.extend(ns0)
            return [val for val in ret if ret]
        elif not dc:
            return ns0
        elif not ns0:
            return dc
        else:
            return [dc, ns0]

    def harvest(self, days_back=1):

        start_date = str(date.today() - timedelta(int(days_back)))

        records_url = self.base_url + self.RECORDS_URL
        request_url = records_url + self.META_PREFIX_DATE.format(start_date)

        if self.timezone_granularity:
            request_url += 'T00:00:00Z'

        records = self.get_records(request_url, start_date)

        rawdoc_list = []
        for record in records:
            doc_id = record.xpath(
                'ns0:header/ns0:identifier', namespaces=self.NAMESPACES)[0].text
            record = etree.tostring(record, encoding=self.record_encoding)
            rawdoc_list.append(RawDocument({
                'doc': record,
                'source': util.copy_to_unicode(self.name),
                'docID': util.copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return rawdoc_list

    def get_records(self, url, start_date, resump_token=''):
        data = requests.get(url, throttle=self.timeout)

        doc = etree.XML(data.content)

        records = doc.xpath(
            '//ns0:record',
            namespaces=self.NAMESPACES
        )
        token = doc.xpath(
            '//ns0:resumptionToken/node()',
            namespaces=self.NAMESPACES
        )
        if len(token) == 1:
            base_url = url.replace(
                self.META_PREFIX_DATE.format(start_date), '')
            base_url = base_url.replace(self.RESUMPTION + resump_token, '')
            url = base_url + self.RESUMPTION + token[0]
            records += self.get_records(url, start_date, resump_token=token[0])

        return records

    def normalize(self, raw_doc):
        str_result = raw_doc.get('doc')
        result = etree.XML(str_result)

        if self.approved_sets:
            set_spec = result.xpath(
                'ns0:header/ns0:setSpec/node()',
                namespaces=self.NAMESPACES
            )
            # check if there's an intersection between the approved sets and the
            # setSpec list provided in the record. If there isn't, don't normalize.
            if not {x.replace('publication:', '') for x in set_spec}.intersection(self.approved_sets):
                logger.info('Series {} not in approved list'.format(set_spec))
                return None

        status = result.xpath('ns0:header/@status', namespaces=self.NAMESPACES)
        if status and status[0] == 'deleted':
            logger.info('Deleted record, not normalizing {}'.format(raw_doc['docID']))
            return None

        return super(OAIHarvester, self).normalize(raw_doc)
