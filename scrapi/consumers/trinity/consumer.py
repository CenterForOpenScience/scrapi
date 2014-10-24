''' Consumer for Trinity '''
from __future__ import unicode_literals

import os
import time
from lxml import etree
from datetime import date, timedelta, datetime

import requests

from dateutil.parser import *

from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'trinity'
OAI_DC_BASE_URL = 'http://digitalcommons.trinity.edu/do/oai/?verb=ListRecords'
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}
DEFAULT = datetime(1970, 01, 01)

DEFAULT_ENCODING = 'UTF-8'

record_encoding = None


def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def consume(days_back=15):
    start_date = date.today() - timedelta(days_back)
    url = OAI_DC_BASE_URL + '&metadataPrefix=oai_dc&from='
    if 'YYYY-MM-DDThh:mm:ssZ' == 'YYYY-MM-DDThh:mm:ssZ':
        url += str(start_date) + 'T00:00:00Z'
    elif 'YYYY-MM-DDThh:mm:ssZ' == 'YYYY-MM-DD hh:mm:ss':
        url += str(start_date) + ' 00:00:00'
    else:
        url += str(start_date)

    record_encoding = requests.get(url).encoding
    records = get_records(url)

    xml_list = []
    for record in records:
        set_spec = record.xpath(
            'ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]
        doc_id = record.xpath(
            'ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record_string = etree.tostring(record, encoding=record_encoding)

        xml_list.append(RawDocument({
            'doc': record_string,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': 'xml'
        }))

    return xml_list


def get_records(url):
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        url = OAI_DC_BASE_URL + '&resumptionToken={}'.format(token[0])
        records += get_records(url)

    return records


def get_contributors(record):
    contributors = record.xpath('//dc:creator/node()', namespaces=NAMESPACES)
    contributor_list = []
    for person in contributors:
        name = HumanName(person)
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': '',
            'ORCID': '',
        }
        contributor_list.append(contributor)
    return contributor_list


def get_tags(record):
    tags = record.xpath('//dc:subject/node()', namespaces=NAMESPACES)
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_ids(record, doc):
    serviceID = doc.get('docID')
    all_urls = record.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for identifier in all_urls:
        if 'viewcontent' not in identifier and 'http://' in identifier:
            url = identifier
        if 'doi' in identifier:
            doi = identifier

    return {'serviceID': serviceID, 'url': copy_to_unicode(url), 'doi': copy_to_unicode(doi)}


def get_properties(record):
    publisher = (record.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    source = (record.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    record_type = (record.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    record_format = (record.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    set_spec = record.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]
    properties = {
        'publisher': publisher,
        'source': source,
        'type': record_type,
        'format': record_format,
        'set_spec': set_spec
    }

    for key, value in properties.iteritems():
        properties[key] = copy_to_unicode(value)

    identifiers = record.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    properties['identifiers'] = [copy_to_unicode(item) for item in identifiers]

    return properties


def get_date_created(record):
    date_created = (
        record.xpath('//dc:date/node()', namespaces=NAMESPACES) or [''])[0]
    date = parse(date_created).isoformat()
    return copy_to_unicode(date)


def get_date_updated(record):
    dateupdated = (record.xpath(
        'ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES) or [''])[0]
    date = parse(dateupdated).isoformat()
    return copy_to_unicode(date)


def normalize(raw_doc):
    doc = raw_doc.get('doc')
    record = etree.XML(doc)

    with open(os.path.join(os.path.dirname(__file__), 'series_names.txt')) as series_names:
        series_name_list = [word.replace('\n', '') for word in series_names]

    set_spec = record.xpath(
        'ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]

    if set_spec.replace('publication:', '') not in series_name_list:
        print('{} not in approved list, not normalizing...'.format(set_spec))
        return None

    title = (
        record.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0]
    description = (record.xpath(
        'ns0:metadata/oai_dc:dc/dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    normalized_dict = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': copy_to_unicode(description),
        'tags': get_tags(record),
        'id': get_ids(record, raw_doc),
        'source': NAME,
        'dateUpdated': get_date_updated(record),
        'dateCreated': get_date_created(record),
    }
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
