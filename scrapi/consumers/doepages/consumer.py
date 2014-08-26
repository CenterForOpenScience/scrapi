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


def normalize(raw_doc, timestamp):
    pass


consume()

# if __name__ == '__main__':
#     print(lint(consume, normalize))
