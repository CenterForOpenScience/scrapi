# Classes for scrAPI Harvesters
from __future__ import unicode_literals

import abc
import json
import logging
from datetime import timedelta, date

import six
from furl import furl
from lxml import etree

from scrapi import util
from scrapi import registry
from scrapi import settings
from scrapi.base.schemas import OAISCHEMA
from scrapi.linter.document import RawDocument, NormalizedDocument
from scrapi.base.transformer import XMLTransformer, JSONTransformer
from scrapi.base.helpers import updated_schema, build_properties, oai_get_records_and_token

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

etree.set_default_parser(etree.XMLParser(recover=True))


class HarvesterMeta(abc.ABCMeta):
    def __init__(cls, name, bases, dct):
        super(HarvesterMeta, cls).__init__(name, bases, dct)
        if len(cls.__abstractmethods__) == 0 and cls.short_name not in settings.disabled:
            registry[cls.short_name] = cls()
        else:
            logger.info('Class {} not added to registry'.format(cls.__name__))


@six.add_metaclass(HarvesterMeta)
class BaseHarvester(object):
    """ This is a base class that all harvesters should inheret from

    Defines the copy to unicode method, which is useful for getting standard
    unicode out of xml results.
    """

    @abc.abstractproperty
    def short_name(self):
        raise NotImplementedError

    @abc.abstractproperty
    def long_name(self):
        raise NotImplementedError

    @abc.abstractproperty
    def url(self):
        raise NotImplementedError

    @abc.abstractproperty
    def file_format(self):
        raise NotImplementedError

    @abc.abstractmethod
    def harvest(self, start_date=None, end_date=None):
        raise NotImplementedError

    @abc.abstractmethod
    def normalize(self, raw_doc):
        raise NotImplementedError

    @property
    def run_at(self):
        return {
            'hour': 22,
            'minute': 59,
            'day_of_week': 'mon-sun',
        }


class JSONHarvester(BaseHarvester, JSONTransformer):
    file_format = 'json'

    def normalize(self, raw_doc):
        transformed = self.transform(json.loads(raw_doc['doc']), fail=settings.RAISE_IN_TRANSFORMER)
        transformed['shareProperties'] = {
            'source': self.short_name,
            'docID': raw_doc['docID']
        }
        return NormalizedDocument(transformed, clean=True)


class XMLHarvester(BaseHarvester, XMLTransformer):
    file_format = 'xml'

    def normalize(self, raw_doc):
        transformed = self.transform(etree.XML(raw_doc['doc']), fail=settings.RAISE_IN_TRANSFORMER)
        transformed['shareProperties'] = {
            'source': self.short_name,
            'docID': raw_doc['docID']
        }
        return NormalizedDocument(transformed, clean=True)


class OAIHarvester(XMLHarvester):
    """ Create a harvester with a oai_dc namespace, that will harvest
    documents within a certain date range

    Contains functions for harvesting from an OAI provider, normalizing,
    and outputting in a way that scrapi can understand, in the most
    generic terms possible.

    For more information, see the OAI PMH specification:
    http://www.openarchives.org/OAI/openarchivesprotocol.html
    """
    record_encoding = None
    DEFAULT_ENCODING = 'UTF-8'
    RESUMPTION = '&resumptionToken='
    RECORDS_URL = '?verb=ListRecords'
    META_PREFIX_DATE = '&metadataPrefix=oai_dc&from={}&until={}'

    # Override these variable is required
    namespaces = {
        'dc': 'http://purl.org/dc/elements/1.1/',
        'ns0': 'http://www.openarchives.org/OAI/2.0/',
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
    }

    timeout = 0.5
    approved_sets = None
    timezone_granularity = False
    property_list = ['date', 'type']
    force_request_update = False
    verify = True

    @property
    def schema(self):
        return self._schema

    @property
    def _schema(self):
        return updated_schema(OAISCHEMA, self.formatted_properties)

    @property
    def formatted_properties(self):
        return {
            'otherProperties': build_properties(*[(item, (
                '//dc:{}/node()'.format(item),
                '//ns0:{}/node()'.format(item),
                self.resolve_property)
            ) for item in self.property_list])
        }

    def resolve_property(self, dc, ns0):
        ret = dc + ns0
        return ret[0] if len(ret) == 1 else ret

    def harvest(self, start_date=None, end_date=None):

        start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()
        end_date = (end_date or date.today()).isoformat()

        if self.timezone_granularity:
            start_date += 'T00:00:00Z'
            end_date += 'T00:00:00Z'

        url = furl(self.base_url)
        url.args['verb'] = 'ListRecords'
        url.args['metadataPrefix'] = 'oai_dc'
        url.args['from'] = start_date
        url.args['until'] = end_date

        records = self.get_records(url.url, start_date, end_date)

        rawdoc_list = []
        for record in records:
            doc_id = record.xpath(
                'ns0:header/ns0:identifier', namespaces=self.namespaces)[0].text
            record = etree.tostring(record, encoding=self.record_encoding)
            rawdoc_list.append(RawDocument({
                'doc': record,
                'source': util.copy_to_unicode(self.short_name),
                'docID': util.copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return rawdoc_list

    def get_records(self, url, start_date, end_date):
        url = furl(url)
        all_records, token = oai_get_records_and_token(url.url, self.timeout, self.force_request_update, self.namespaces, self.verify)

        while token:
            url.remove('from')
            url.remove('until')
            url.remove('metadataPrefix')
            url.args['resumptionToken'] = token[0]
            records, token = oai_get_records_and_token(url.url, self.timeout, self.force_request_update, self.namespaces, self.verify)
            all_records += records

        return all_records

    def normalize(self, raw_doc):
        str_result = raw_doc.get('doc')
        result = etree.XML(str_result)

        if self.approved_sets:
            set_spec = result.xpath(
                'ns0:header/ns0:setSpec/node()',
                namespaces=self.namespaces
            )
            # check if there's an intersection between the approved sets and the
            # setSpec list provided in the record. If there isn't, don't normalize.
            if not {x.replace('publication:', '') for x in set_spec}.intersection(self.approved_sets):
                logger.info('Series {} not in approved list'.format(set_spec))
                return None

        status = result.xpath('ns0:header/@status', namespaces=self.namespaces)
        if status and status[0] == 'deleted':
            logger.info('Deleted record, not normalizing {}'.format(raw_doc['docID']))
            return None

        return super(OAIHarvester, self).normalize(raw_doc)
