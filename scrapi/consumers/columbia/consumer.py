# start to a consumer for Columbia's academic commons for the SHARE project

import requests
from datetime import date, timedelta
from lxml import etree 
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
    # YYYY-MM-DDThh:mm:ssZ
    print start_date
    url = base_url + str(start_date) + '&metadataPrefix=oai_dc'
    data = requests.get(url)
    doc =  etree.XML(data.content)
    #

    records = doc.xpath('//oai_dc:record', namespaces=NAMESPACES)

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

    print record
    return xml_list

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    contributors = doc.findall('ns0:metadata/oai_dc:dc/dc:creator', namespaces=NAMESPACES)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email': ''})
    title = doc.findall('ns0:metadata/oai_dc:dc/dc:title', namespaces=NAMESPACES)

    service_id = doc.xpath('ns0:header/ns0:identifier/node()', 
                                    namespaces=namespaces)[0]
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    for item in identifiers:
        if 'escholarship.org' in item:
            url = item

    ids = {'url': url, 'service_id': service_id, 'doi': 'doi'}

    description = doc.xpath('ns0:metadata/oai_dc:dc/dc:description/node()', namespaces=NAMESPACES) or ['']

    date_created = doc.xpath('//dc:date', namespaces=NAMESPACES)[0].text

    tags = doc.xpath('//dc:subject/node()', namespaces=NAMESPACES)

    normalized_dict = {
            'title': title[0].text,
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

consume()        

# if __name__ == '__main__':
#     print(lint(consume, normalize))