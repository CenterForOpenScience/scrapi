''' Consumer for ArXiv '''
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

NAME = 'arxiv_oai'
OAI_DC_BASE_URL = 'http://export.arxiv.org/oai2?verb=ListRecords'
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

def consume(days_back=1):
    start_date = date.today() - timedelta(days_back)
    url = OAI_DC_BASE_URL + '&metadataPrefix=oai_dc&from='
    url += str(start_date)
    encoding_url = 'http://export.arxiv.org/oai2?verb=GetRecord&identifier=oai:arXiv.org:0804.2273&metadataPrefix=oai_dc'
    record_encoding = requests.get(encoding_url).encoding
    # TODO - fix these long times to use the  503 Retry-After responses for times
    time.sleep(30)
    records = get_records(url)

    xml_list = []
    # TODO - remove the testing restriction of first 500 documents
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record_string = etree.tostring(record, encoding=record_encoding)

        xml_list.append(RawDocument({
                    'doc': record_string,
                    'source': NAME,
                    'docID': copy_to_unicode(doc_id),
                    'filetype': 'xml'
                }))

    return xml_list

def get_records(url):
    print(url)
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(30)
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
    set_spec = record.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)
    all_tags = tags + set_spec    
    return [copy_to_unicode(tag.lower()) for tag in all_tags]

def get_ids(record, doc):
    serviceID = doc.get('docID')
    all_urls = record.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for identifier in all_urls:
        if 'http://' in identifier:
            url = identifier
        if 'doi' in identifier:
            doi = identifier.replace('doi:', '')
    if url == '':
        raise Exception('Warning: No url provided!')

    return {'serviceID': serviceID, 'url': copy_to_unicode(url), 'doi': copy_to_unicode(doi)}

def get_properties(record):
    record_type = (record.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    record_format = (record.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    set_spec = record.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)
    properties = {
        'type': record_type,
        'format': record_format,
        'set_spec': [copy_to_unicode(spec) for spec in set_spec]
    }

    for key, value in properties.iteritems():
        if not isinstance(value, list):
            properties[key] = copy_to_unicode(value)

    return properties

def get_date_created(record):
    date_created = (record.xpath('//dc:date/node()', namespaces=NAMESPACES) or [''])[0]
    date = parse(date_created).isoformat()
    return copy_to_unicode(date)

def get_date_updated(record):
    dateupdated = (record.xpath('ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES) or [''])[0]
    date = parse(dateupdated).isoformat()
    return copy_to_unicode(date)

def normalize(raw_doc):
    doc = raw_doc.get('doc')
    record = etree.XML(doc)

    title = (record.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0]
    description = (record.xpath('ns0:metadata/oai_dc:dc/dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    normalized_dict = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': copy_to_unicode(description),
        'tags': get_tags(record),
        'id': get_ids(record,raw_doc),
        'source': NAME,
        'dateUpdated': get_date_updated(record),
        'dateCreated': get_date_created(record),
    }

    # import json; print(json.dumps(normalized_dict, indent=4))
    return NormalizedDocument(normalized_dict)
        

if __name__ == '__main__':
    print(lint(consume, normalize))