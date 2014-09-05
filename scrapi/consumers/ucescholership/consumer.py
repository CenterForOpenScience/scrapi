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
import json #delete

TODAY = date.today()
NAME = "eScholarship"

def consume(days_back=1):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.escholarship.org/uc/oai?verb=ListRecords&metadataPrefix=oai_dc&from='
    url = base_url + str(start_date)
    data = requests.get(url)
    doc =  etree.XML(data.content)

    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/'}

    records = doc.xpath('//oai_dc:record', namespaces=namespaces)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', 
                                namespaces=namespaces)[0].text
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

    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/'}

    contributors = doc.findall('ns0:metadata/oai_dc:dc/dc:creator', namespaces=namespaces)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email': ''})
    title = doc.findall('ns0:metadata/oai_dc:dc/dc:title', namespaces=namespaces)

    service_id = doc.xpath('ns0:header/ns0:identifier/node()', 
                                    namespaces=namespaces)[0]
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=namespaces)
    for item in identifiers:
        if 'escholarship.org' in item:
            url = item

    ids = {'url': url, 'service_id': service_id, 'doi': 'doi'}

    ## Using this for the abstract for now...
    ## TODO: make this an actual absttract maybe by going to the source...
    description = doc.xpath('ns0:metadata/oai_dc:dc/dc:description/node()', namespaces=namespaces) or ['']

    date_created = doc.xpath('//dc:date', namespaces=namespaces)[0].text

    tags = doc.xpath('//dc:subject/node()', namespaces=namespaces)

    citation = doc.xpath('//dc:source/node()', namespaces=namespaces) or ['']

    dc_type = doc.xpath('//dc:type/node()', namespaces=namespaces) or ['']

    format = doc.xpath('//dc:format/node()', namespaces=namespaces) or ['']

    coverage = doc.xpath('//dc:coverage/node()', namespaces=namespaces) or ['']

    relation = doc.xpath('//dc:relation/node()', namespaces=namespaces) or ['']

    rights = doc.xpath('//dc:rights/node()', namespaces=namespaces) or ['']
    
    normalized_dict = {
            'title': title[0].text,
            'contributors': contributor_list,
            'properties': {
                'type': dc_type[0],
                'format': format[0],
                'published-in': citation[0],
                'coverage': coverage[0],
                'relation': relation[0],
                'rights': rights[0]
            },
            'description': description[0],
            'meta': {},
            'id': ids,
            'source': NAME,
            'tags': tags,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }
    print(json.dumps(normalized_dict, sort_keys=True, indent=4, separators=(',', ': '))) # delete

    return NormalizedDocument(normalized_dict)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
