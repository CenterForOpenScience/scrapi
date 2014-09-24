#!/usr/bin/env python

''' Consumer and Normalizer for the eScholarship Repo 
at the University of California
'''

from lxml import etree
from xml.etree import ElementTree
from datetime import date, timedelta
import requests
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "eScholarship"

NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}

def consume(days_back=10):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.escholarship.org/uc/oai?verb=ListRecords&metadataPrefix=oai_dc&from='
    url = base_url + str(start_date)
    data = requests.get(url)
    doc =  etree.XML(data.content)

    records = doc.xpath('//oai_dc:record', namespaces=NAMESPACES)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', 
                                namespaces=NAMESPACES)[0].text
        record = ElementTree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return xml_list

    ## TODO: fix if there are no records found... what would the XML look like?

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    contributors = doc.findall('ns0:metadata/oai_dc:dc/dc:creator', namespaces=NAMESPACES)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email': ''})
    title = (doc.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0]

    service_id = doc.xpath('ns0:header/ns0:identifier/node()', 
                                    namespaces=NAMESPACES)[0]
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    for item in identifiers:
        if 'escholarship.org' in item:
            url = item

    ids = {'url': url, 'service_id': service_id, 'doi': 'doi'}

    description = doc.xpath('ns0:metadata/oai_dc:dc/dc:description/node()', namespaces=NAMESPACES) or ['']

    date_created = doc.xpath('//dc:date', namespaces=NAMESPACES)[0].text

    tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES)

    normalized_dict = {
            'title': title,
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
