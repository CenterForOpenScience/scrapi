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
    print url
    data = requests.get(url)
    doc =  etree.XML(data.content)
    return doc

def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    # contributors
    contributors = (doc.xpath("str[@name='author']/node()") + doc.xpath("arr[@name='origin']/str/node()"))  or ['DataONE']
    email = ''
    submitters = doc.xpath("str[@name='submitter']/node()")
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor, 'email': email})
        for submitter in submitters:
            if '@' in submitter:
                contributor_list[0]['email'] = submitter
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

    description = (doc.xpath("str[@name='abstract']/node()") or [''])[0]

    date_created = doc.xpath("date[@name='dateUploaded']/node()")[0]

    ## properties ##
    properties = { 
        'author': (doc.xpath("str[@name='author']/node()") or [''])[0],
        'authorGivenName': (doc.xpath("str[@name='authorGivenName']/node()") or [''])[0],
        'authorSurName': (doc.xpath("str[@name='authorSurName']/node()") or [''])[0],
        'authoritativeMN' : (doc.xpath("str[@name='authoritativeMN']/node()") or [''])[0],
        'checksum' : (doc.xpath("str[@name='checksum']/node()") or [''])[0],
        'checksumAlgorithm' : (doc.xpath("str[@name='checksumAlgorithm']/node()") or [''])[0],
        'dataUrl': (doc.xpath("str[@name='dataUrl']/node()") or [''])[0],
        'datasource': (doc.xpath("str[@name='datasource']/node()") or [''])[0],

        'dateModified': (doc.xpath("date[@name='dateModified']/node()") or [''])[0],
        'datePublished': (doc.xpath("date[@name='datePublished']/node()") or [''])[0],
        'dateUploaded': (doc.xpath("date[@name='dateUploaded']/node()") or [''])[0],
        'pubDate': (doc.xpath("date[@name='pubDate']/node()") or [''])[0],
        'updateDate': (doc.xpath("date[@name='updateDate']/node()") or [''])[0],

        'fileID': (doc.xpath("str[@name='fileID']/node()") or [''])[0],
        'formatId': (doc.xpath("str[@name='formatId']/node()") or [''])[0],
        'formatType': (doc.xpath("str[@name='formatType']/node()") or [''])[0],

        'identifier': (doc.xpath("str[@name='identifier']/node()") or [''])[0],

        'investigator': doc.xpath("arr[@name='investigator']/str/node()"),
        'origin': doc.xpath("arr[@name='origin']/str/node()"),

        'isPublic': (doc.xpath("bool[@name='isPublic']/node()") or [''])[0],
        'readPermission': doc.xpath("arr[@name='readPermission']/str/node()"),
        'replicaMN': doc.xpath("arr[@name='replicaMN']/str/node()"),
        'replicaVerifiedDate': doc.xpath("arr[@name='replicaVerifiedDate']/date/node()"),
        'replicationAllowed': (doc.xpath("bool[@name='replicationAllowed']/node()") or [''])[0],
        'numberReplicas': (doc.xpath("int[@name='numberReplicas']/node()") or [''])[0],
        'preferredReplicationMN': doc.xpath("arr[@name='preferredReplicationMN']/str/node()"),

        'resourceMap': doc.xpath("arr[@name='resourceMap']/str/node()"),
        

        'rightsHolder': (doc.xpath("str[@name='rightsHolder']/node()") or [''])[0],

        'scientificName': doc.xpath("arr[@name='scientificName']/str/node()"),
        'site': doc.xpath("arr[@name='site']/str/node()"),
        'size': (doc.xpath("long[@name='size']/node()") or [''])[0],
        'sku': (doc.xpath("str[@name='sku']/node()") or [''])[0],
        'isDocumentedBy': doc.xpath("arr[@name='isDocumentedBy']/str/node()"),

    }

    # updateDate
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

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
