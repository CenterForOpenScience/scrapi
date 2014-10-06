## Consumer for DOE Pages for SHARE
from __future__ import unicode_literals

import time
import requests
from lxml import etree
from datetime import date, timedelta, datetime

from nameparser import HumanName

from dateutil.parser import *

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'doepages'

NAMESPACES = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcq': 'http://purl.org/dc/terms/'}

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
    base_url = 'http://www.osti.gov/pages/pagesxml?nrows={0}&EntryDateFrom={1}'
    url = base_url.format('1', start_date.strftime('%m/%d/%Y'))
    initial_data = requests.get(url)
    record_encoding = initial_data.encoding
    initial_doc = etree.XML(initial_data.content)

    num_results = int(initial_doc.xpath('//records/@count', namespaces=NAMESPACES)[0])

    url = base_url.format(num_results, start_date.strftime('%m/%d/%Y'))
    data = requests.get(url)
    doc = etree.XML(data.content)

    records = doc.xpath('records/record')

    xml_list = []
    for record in records:
        doc_id = record.xpath('dc:ostiId/node()', namespaces=NAMESPACES)[0]
        record = etree.tostring(record, encoding=record_encoding)
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'docID': copy_to_unicode(doc_id),
                    'filetype': 'xml'
                }))

    return xml_list

def get_ids(doc, raw_doc):
    ids = {}
    ids['doi'] = (doc.xpath('//dc:doi/node()', namespaces=NAMESPACES) or [''])[0]
    ids['serviceID'] = raw_doc.get('docID')
    url = (doc.xpath('//dcq:identifier-citation/node()', namespaces=NAMESPACES) or [''])[0]
    if url == '':
        url = 'http://dx.doi.org/' + ids['doi']
    if url == '':
        raise Exception('Warning: url field is blank!')
    ids['url'] = url

    for key, value in ids.iteritems():
        ids[key] = copy_to_unicode(value)

    return ids

def get_contributors(doc):
    contributor_list = []
    full_contributors = doc.xpath('//dc:creator/node()', namespaces=NAMESPACES)[0].split(';')
    for person in full_contributors:
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
    publisherInfo = {
        'publisher': (doc.xpath('//dcq:publisher/node()', namespaces=NAMESPACES) or [''])[0],
        'publisherSponsor': (doc.xpath('//dcq:publisherSponsor/node()', namespaces=NAMESPACES) or [''])[0],
        'publisherAvailability': (doc.xpath('//dcq:publisherAvailability/node()', namespaces=NAMESPACES) or [''])[0],
        'publisherResearch': (doc.xpath('//dcq:publisherResearch/node()', namespaces=NAMESPACES) or [''])[0],
        'publisherCountry': (doc.xpath('//dcq:publisherCountry/node()', namespaces=NAMESPACES) or [''])[0]
    }

    for key, value in publisherInfo.iteritems():
        publisherInfo[key] = copy_to_unicode(value)

    properties = {
        'language': (doc.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0],
        'type': (doc.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0],
        'typeQualifier': (doc.xpath('//dc:typeQualifier/node()', namespaces=NAMESPACES) or [''])[0],
        'relation': (doc.xpath('//dc:relation/node()', namespaces=NAMESPACES) or [''])[0],
        'coverage': (doc.xpath('//dc:coverage/node()', namespaces=NAMESPACES) or [''])[0],
        'format': (doc.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0],
        'rights': (doc.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])[0],
        # TODO:  parse out some of these identifiers in the strings! 
        'identifier': (doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or [''])[0],
        'identifierReport': (doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or [''])[0],
        'identifierDOEcontract': (doc.xpath('//dcq:identifierDOEcontract/node()', namespaces=NAMESPACES) or [''])[0],
        'identifierOther': (doc.xpath('//dc:identifierOther/node()', namespaces=NAMESPACES) or [''])[0],
        'identifier-purl': (doc.xpath('//dcq:identifier-purl/node()', namespaces=NAMESPACES) or [''])[0]
    }

    for key, value in properties.iteritems():
        properties[key] = copy_to_unicode(value)

    properties['publisherInfo'] = publisherInfo

    return properties

def get_date_created(doc):
    date_created = doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]
    date = parse(date_created).isoformat()
    return copy_to_unicode(date)

def get_date_updated(doc):
    date_updated = doc.xpath('//dc:dateEntry/node()', namespaces=NAMESPACES)[0]
    date = parse(date_updated).isoformat()
    return copy_to_unicode(date)

def get_tags(doc):
    all_tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES) + doc.xpath('//dc:subjectRelated/node()', namespaces=NAMESPACES)
    tags = []
    for taglist in all_tags:
        tags += taglist.split(',')
    return list(set([copy_to_unicode(tag.lower().strip()) for tag in tags]))

def normalize(raw_doc):
    raw_doc_string = raw_doc.get('doc')
    doc = etree.XML(raw_doc_string)

    title = doc.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]
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
        'dateUpdated': get_date_updated(doc),
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
