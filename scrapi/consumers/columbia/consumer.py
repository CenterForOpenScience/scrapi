# start to a consumer for Columbia's academic commons for the SHARE project
from __future__ import unicode_literals

import time
import requests
from lxml import etree
from datetime import datetime, date, timedelta

from nameparser import HumanName

from dateutil.parser import *

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument


NAME = 'columbia'
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}
OAI_DC_BASE_URL = 'http://academiccommons.columbia.edu/catalog/oai?verb=ListRecords&'
DEFAULT = datetime(2014, 01, 01)
DEFAULT_ENCODING = 'UTF-8'

record_encoding = None

def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)

def consume(days_back=1):
    base_url = OAI_DC_BASE_URL + 'from={}&until={}'
    start_date = str(date.today() - timedelta(days_back)) + 'T00:00:00Z'
    end_date = str(date.today()) + 'T00:00:00Z'
    url = base_url.format(start_date, end_date) + '&metadataPrefix=oai_dc'
    record_encoding = requests.get(url).encoding
    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier',
                              namespaces=NAMESPACES)[0].text
        record = etree.tostring(record, encoding=record_encoding)
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': 'xml'
        }))

    return xml_list


def get_records(url):
    data = requests.get(url)
    try:
        doc = etree.XML(data.content)
    except etree.XMLSyntaxError:
        print('XML error')
        return

    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)
    # TODO - disabling token for now - test periodically
    if len(token) == 1:
        time.sleep(0.5)
        base_url = OAI_DC_BASE_URL + 'resumptionToken='
        url = base_url + token[0]
        records += get_records(url)
    return records


def get_ids(doc, raw_doc):
    service_id = raw_doc.get('docID')
    url = (doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or [''])[0]
    doi = ''
    if 'doi' in url:
        doi = url.replace('http://dx.doi.org/', '')
    if url == '':
        raise Exception('Warning: No url provided!')
    return {'url': copy_to_unicode(url), 'serviceID': service_id, 'doi': copy_to_unicode(doi)}


def get_contributors(doc):
    contributors = doc.xpath('//dc:creator/node()', namespaces=NAMESPACES) or []
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
            'ORCID': ''
        }
        contributor_list.append(contributor)

    return contributor_list


def get_properties(doc):
    language = doc.xpath('//dc:language/node()', namespaces=NAMESPACES) or ['']
    resource_type = doc.xpath(
        '//dc:type/node()', namespaces=NAMESPACES) or ['']
    publisher = doc.xpath(
        '//dc:publisher/node()', namespaces=NAMESPACES) or ['']

    properties = {"language": language, "resource_type": resource_type, "publisher": publisher}

    for key, value in properties.iteritems():
        properties[key] = copy_to_unicode(value)

    return properties

def get_tags(doc):
    tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    return [copy_to_unicode(tag.lower()) for tag in tags]

def get_date_created(doc):
    # TODO - this is almost always just a year - what do? 
    #       right now everyhing just defaults to Jan 1
    date_created = (doc.xpath('//dc:date/node()', namespaces=NAMESPACES) or [''])[0]
    date = parse(date_created, default=DEFAULT).isoformat()
    return copy_to_unicode(date)

def get_date_updated(doc):
    datestamp = doc.xpath('ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    date = parse(datestamp).isoformat()
    return copy_to_unicode(date)

def normalize(raw_doc):
    raw_doc_string = raw_doc.get('doc')
    doc = etree.XML(raw_doc_string)

    title = (doc.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0]
    description = (doc.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    normalized_dict = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(doc),
        'properties': get_properties(doc),
        'description': copy_to_unicode(description),
        'id': get_ids(doc, raw_doc),
        'source': NAME,
        'tags': get_tags(doc),
        'dateCreated': get_date_created(doc),
        'dateUpdated': get_date_updated(doc)
    }

    # import json; print json.dumps(normalized_dict, indent=4)
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
