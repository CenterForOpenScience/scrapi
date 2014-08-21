# start to a consumer for Columbia's academic commons for the SHARE project

import time
import requests
from lxml import etree 
from datetime import date, timedelta

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = 'academiccommons'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}


def consume(days_back=3):
    base_url = 'http://academiccommons.columbia.edu/catalog/oai?verb=ListRecords&from='
    start_date = str(date.today() - timedelta(days_back)) + 'T00:00:00Z'
    url = base_url + str(start_date) + '&metadataPrefix=oai_dc'

    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', 
                                namespaces=NAMESPACES)[0].text
        record = etree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
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
        base_url = 'http://academiccommons.columbia.edu/catalog/oai?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url)

    return records

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    contributors = doc.xpath('//dc:creator', namespaces=NAMESPACES)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email': ''})

    contributor_list = contributor_list or [{'full_name': 'No contributors', 'email': ''}]

    title = doc.xpath('//dc:title/node()', namespaces=NAMESPACES) or ['No title']

    service_id = doc.xpath('ns0:header/ns0:identifier/node()', 
                                    namespaces=NAMESPACES)[0]
    identifier = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or ['no url']

    ids = {'url': identifier[0], 'service_id': service_id, 'doi': 'doi'}

    description = doc.xpath('//dc:description/node()', namespaces=NAMESPACES) or ['']

    date_created = doc.xpath('//dc:date', namespaces=NAMESPACES)[0].text

    tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES)

    normalized_dict = {
            'title': title[0],
            'contributors': contributor_list,
            'properties': {},
            'description': description[0],
            'meta': {},
            'id': ids,
            'source': NAME,
            'tags': tags,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))