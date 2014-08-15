## consumer for DataONE SOLR search API

from lxml import etree
from xml.etree import ElementTree
import requests
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = "dataone"

def consume(days_back=1):
    doc =  get_response(1)
    rows = doc.xpath("//result/@numFound")[0]
    doc = get_response(rows)
    records = doc.xpath('//doc')
    
    xml_list = []
    for record in records:
        doc_id = record.xpath("str[@name='id']")[0].text
        record = ElementTree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return xml_list

def get_response(rows):
    ''' helper function to get a response from the DataONE
    API, with the specified number of rows.
    Returns an etree element with results '''
    url = 'https://cn.dataone.org/cn/v1/query/solr/?q=dateModified:[NOW-1DAY TO *]&rows=' + str(rows)
    data = requests.get(url)
    doc =  etree.XML(data.content)
    return doc

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)
    contributors = doc.xpath("arr[@name='origin']/str/node()") or  doc.xpath("str[@name='author']/node()") or ['No contributors.']
    submitters = doc.xpath("str[@name='submitter']/node()")
    email = ''
    for submitter in submitters:
        if '@' in submitter:
            email = submitter
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor, 'email': email})
    #TODO: sometimes email info is in submitter or rights holder fields...
    
    try:
        title = doc.xpath("str[@name='title']/node()")[0]
    except IndexError:
        title = "No title available"
    
    tags = doc.xpath("//arr[@name='keywords']/str/node()")

    doi = ''
    service_id = doc.xpath("str[@name='id']/node()")[0]
    if 'doi' in service_id:
        doi = service_id

    url = doc.xpath('//str[@name="dataUrl"]/node()') or ['']

    ids = {'service_id':service_id, 'doi': doi, 'url':url[0]}

    try: 
        description = doc.xpath("str[@name='abstract']/node()")[0]
    except IndexError:
        description = "No abstract available"

    date_created = doc.xpath("date[@name='dateUploaded']/node()")[0]

    normalized_dict = {
            'title': title,
            'contributors': contributor_list,
            'properties': {},
            'description': description,
            'meta': {},
            'id': ids,
            'tags': tags,
            'source': NAME,
            'date_created': date_created,
            'timestamp': str(timestamp)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
