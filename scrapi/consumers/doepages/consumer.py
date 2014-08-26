## Consumer for DOE Pages for SHARE

import time
import requests
from lxml import etree
from datetime import date, timedelta, datetime

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument


NAME = 'doepages'
TODAY = date.today()

NAMESPACES = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcq': 'http://purl.org/dc/terms/'}

def consume(days_back=30):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.osti.gov/pages/pagesxml?nrows=3000&EntryDateFrom='
    url = base_url + start_date.strftime('%m/%d/%Y')

    data = requests.get(url)
    doc = etree.XML(data.content)

    # NOTE: only grabs the first 3000 results
    num_results = doc.xpath('//records[@count]', namespaces=NAMESPACES)[0].attrib['count']
    if int(num_results) > 3000:
        raise Exception('More results than have been consumed!! There are {} results.'.format(num_results))

    records = doc.xpath('records/record')

    xml_list = []
    for record in records:
        doc_id = record.xpath('dc:ostiId', 
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

def get_ids(doc):
    ids = {}
    ids['doi'] = (doc.xpath('//dc:doi/node()', namespaces=NAMESPACES) or [''])[0]
    ids['service_id'] = (doc.xpath('//dc:ostiId/node()', namespaces=NAMESPACES) or [''])[0]
    url = (doc.xpath('//dcq:identifier-citation/node()', namespaces=NAMESPACES) or [''])[0]
    if url == '':
        url = 'http://dx.doi.org/' + ids['doi']
    if url == '':
        raise Exception('Warning: url field is blank!')
    ids['url'] = url

    return ids

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    ## contributors ##
    contributor_list = []
    full_contributors = doc.xpath('//dc:creator/node()', namespaces=NAMESPACES)[0].split(';')
    for contributor in full_contributors:
        contributor_list.append({'full_name': contributor, 'email': ''})
    normalized_dict = {
        'title': doc.xpath('//dc:title/node()', namespaces=NAMESPACES)[0],
        'contributors': contributor_list,
        'properties': {
                'publisher': (doc.xpath('//dcq:publisher/node()', namespaces=NAMESPACES) or [''])[0],
                'publisher_sponsor': (doc.xpath('//dcq:publisherSponsor/node()', namespaces=NAMESPACES) or [''])[0],
                'language': (doc.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0],
                'type': (doc.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]

        },
        'description': (doc.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0],
        'meta': {},
        'id': get_ids(doc),
        'source': NAME,
        'tags': doc.xpath('//dc:subject/node()', namespaces=NAMESPACES) or [],
        'date_created': doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0],
        'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
