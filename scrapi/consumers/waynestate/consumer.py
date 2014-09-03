''' Consumer for Wayne State University -- Digital Commons '''

from lxml import etree
from datetime import date, timedelta
import requests
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "DigitalCommonsWayneState"

def consume(days_back=5):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://digitalcommons.wayne.edu/do/oai/?verb=ListRecords&metadataPrefix=oai_dc&from='
    url = base_url + str(start_date) + 'T00:00:00Z'
    data = requests.get(url)
    doc =  etree.XML(data.content)
    # import pdb; pdb.set_trace()
    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/'}


    records = doc.xpath('//oai_dc:record', namespaces=namespaces)
    # pdb.set_trace()
    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=namespaces)[0].text
        record = etree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))
    #print(record)
    return xml_list

    ## TODO: fix if there are no records found... what would the XML look like?

def normalize(raw_doc, timestamp):
    doc = raw_doc.get('doc')

    record = etree.XML(doc)

    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/'}

    # contributors #
    contributors = record.findall('ns0:metadata/oai_dc:dc/dc:creator', namespaces=namespaces)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor.text, 'email':''})
    
    # title
    title = record.xpath('//dc:title/node()', namespaces=namespaces)[0]

    # ids
    service_id = record.xpath('ns0:header/ns0:identifier', namespaces=namespaces)[0].text

    all_urls = record.xpath('//dc:identifier/node()', namespaces=namespaces)
    for identifier in all_urls:
        if 'cgi' not in identifier and 'http://digitalcommons.wayne.edu' in identifier:
            url = identifier

    ids = {'url': url, 'service_id': service_id, 'doi': ''}

    try: 
        description = record.xpath('ns0:metadata/oai_dc:dc/dc:description/node()', namespaces=namespaces)[0]
    except IndexError:
        description = "No description available"

    date_created = record.xpath('ns0:metadata/oai_dc:dc/dc:date', namespaces=namespaces)[0].text

    tags = record.xpath('//dc:subject/node()', namespaces=namespaces)

    #properties (publisher and source)
    properties = {}
    properties["publisher"] = record.xpath('//dc:publisher/node()', namespaces=namespaces)
    properties["source"] = record.xpath('//dc:source/node()', namespaces=namespaces)
    properties["type"] = record.xpath('//dc:type/node()', namespaces=namespaces)
    properties["format"] = record.xpath('//dc:format/node()', namespaces=namespaces)

    normalized_dict = {
            'title': title,
            'contributors': contributor_list,
            'properties': properties,
            'description': description,
            'meta': {},
            'id': ids,
            'tags': tags,
            'source': NAME,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }

    print normalized_dict
    return NormalizedDocument(normalized_dict)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
