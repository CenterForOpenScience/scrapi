''' Consumer for St Cloud State University '''

import requests
from datetime import date, timedelta, datetime
from dateutil.parser import *
import time
from lxml import etree
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName
import os

TODAY = date.today()
NAME = "stcloud"
OAI_DC_BASE_URL = 'http://repository.stcloudstate.edu/do/oai/'
DEFAULT = datetime(1970, 01, 01)
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'
              }


def consume(days_back=5):
    start_date = TODAY - timedelta(days_back)
    base_url = OAI_DC_BASE_URL + '?verb=ListRecords&metadataPrefix=oai_dc&from='
    url = base_url + str(start_date) + 'T00:00:00Z'
    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record_string = etree.tostring(record)
        record_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + record_string
        xml_list.append(RawDocument({
            'doc': record_string,
            'source': NAME,
            'docID': doc_id,
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
        base_url = OAI_DC_BASE_URL + '?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url)

    return records


def get_contributors(record):
    all_contributors = record.xpath('//dc:creator/node()', namespaces=NAMESPACES)
    contributor_list = []
    for person in all_contributors:
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
    return [tag.lower() for tag in tags]


def get_ids(record, doc):
    serviceID = doc.get('docID')
    all_ids = record.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    for identifier in all_ids:
        if 'viewcontent' not in identifier and OAI_DC_BASE_URL[:-7] in identifier:
            url = identifier
    if url is '':
        raise Exception('Warning: No url provided!')
    return {'serviceID': serviceID, 'url': url, 'doi': ''}


def get_properties(record):
    publisher = (record.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    source = (record.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    dctype = (record.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    dcformat = (record.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    props = {
        'publisherInfo': {
            'publisher': publisher,
        },
        'source': source,
        'type': dctype,
        'format': dcformat,
    }
    return props


def get_date_created(record):
    date_created = (record.xpath('ns0:metadata/oai_dc:dc/dc:date/node()', namespaces=NAMESPACES) or [''])[0]
    a_date = parse(date_created, yearfirst=True,  default=DEFAULT).isoformat()
    return a_date


def get_date_updated(record):
    dateupdated = (record.xpath('//ns0:datestamp/node()', namespaces=NAMESPACES) or [''])[0]
    date_updated = parse(dateupdated).isoformat()
    return date_updated


def normalize(raw_doc, timestamp):
    doc = raw_doc.get('doc')

    record = etree.XML(doc)

    # load the list of approved series_names as a file - second option
    with open(os.path.join(os.path.dirname(__file__), 'approved_series.txt')) as series_names:
        approved_series = [word.replace('\n', '') for word in series_names]
    set_spec = record.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]
    if set_spec.replace('publication:', '') not in approved_series:
        return None

    title = record.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]
    description = (record.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    payload = {
        'title': title,
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': description,
        'tags': get_tags(record),
        'id': get_ids(record,raw_doc),
        'source': NAME,
        'dateUpdated': get_date_updated(record),
        'dateCreated': get_date_created(record),
        'timestamp': str(timestamp),
    }

    #import json
    #print(json.dumps(payload, indent=4))
    return NormalizedDocument(payload)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
