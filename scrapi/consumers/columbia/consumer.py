# start to a consumer for Columbia's academic commons for the SHARE project
from __future__ import unicode_literals

import time
import requests
from lxml import etree
from datetime import date, timedelta, datetime
from nameparser import HumanName
from dateutil.parser import *
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument


NAME = 'academiccommons'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}
OAI_DC_BASE_URL = 'http://academiccommons.columbia.edu/catalog/oai?verb=ListRecords&'
DEFAULT = datetime(2070, 01, 01)


def consume(days_back=5):
    base_url = OAI_DC_BASE_URL + 'from={}&until={}'
    start_date = str(date.today() - timedelta(days_back)) + 'T00:00:00Z'
    end_date = str(date.today()) + 'T00:00:00Z'
    url = base_url.format(start_date, end_date) + '&metadataPrefix=oai_dc'
    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier',
                              namespaces=NAMESPACES)[0].text
        record = etree.tostring(record, encoding='UTF-8')
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': doc_id,
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

    return {'url': url, 'serviceID': service_id, 'doi': doi}


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

    return {"language": language, "resource_type": resource_type, "publisher": publisher}


def get_tags(doc):
    tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    return [tag.lower() for tag in tags]

def get_date_created(doc):
    # TODO - this is almost always just a year - what do? 
    #       right now everyhing just defaults to Jan 1
    date_created = (doc.xpath('//dc:date/node()', namespaces=NAMESPACES) or [''])[0]
    return parse(date_created, default=DEFAULT).isoformat()


def get_date_updated(doc):
    datestamp = doc.xpath('ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    return parse(datestamp).isoformat()

def normalize(raw_doc, timestamp):
    raw_doc_string = raw_doc.get('doc')
    doc = etree.XML(raw_doc_string)

    normalized_dict = {
        'title': (doc.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0],
        'contributors': get_contributors(doc),
        'properties': get_properties(doc),
        'description': (doc.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0],
        'id': get_ids(doc, raw_doc),
        'source': NAME,
        'tags': get_tags(doc),
        'dateCreated': get_date_created(doc),
        'dateUpdated': get_date_updated(doc),
        'timestamp': str(timestamp)
    }

    #import json
    # print json.dumps(normalized_dict, indent=4)
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
