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


def consume(days_back=10):
    base_url = 'http://academiccommons.columbia.edu/catalog/oai?verb=ListRecords&from={}&until={}'
    start_date = str(date.today() - timedelta(days_back)) + 'T00:00:00Z'
    end_date = str(date.today()) + 'T00:00:00Z'
    url = base_url.format(start_date, end_date) + '&metadataPrefix=oai_dc'
    print(url)
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

def get_ids(doc, raw_doc):
    service_id = raw_doc.get()
    identifier = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or ['']

    return {'url': identifier[0], 'service_id': service_id, 'doi': ''}

def get_contributors(doc):
    contributors = doc.xpath('//dc:creator', namespaces=NAMESPACES)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email': ''})

    contributor_list = contributor_list or [{'full_name': 'No contributors', 'email': ''}]

    return contributor_list

def get_properties(doc):
    language = doc.xpath('//dc:language/node()', namespaces=NAMESPACES) or ['']
    resource_type = doc.xpath('//dc:type/node()', namespaces=NAMESPACES) or ['']
    publisher = doc.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or ['']

    return {"language": language, "resource_type": resource_type, "publisher": publisher}

def get_tags(doc):
    return doc.xpath('//dc:subject/node()', namespaces=NAMESPACES)

def get_date_created(doc):
    date_created = doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]
    return date_created

def get_date_updated(doc):
    pass

def normalize(raw_doc, timestamp):
    raw_doc_string = raw_doc.get('doc')
    doc = etree.XML(raw_doc_string)

    normalized_dict = {
            "title": (doc.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0],
            "contributors": get_contributors(doc),
            "properties": get_properties(doc),
            "description": (doc.xpath('//dc:description/node()', namespaces=NAMESPACES) or [""])[0],
            "id": get_ids(doc, raw_doc),
            "source": NAME,
            "tags": get_tags(),
            "date_created": get_date_created(),
            "timestamp": str(timestamp)
    }
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))