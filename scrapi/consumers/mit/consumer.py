# Consumer for MIT DSpace

import requests
from datetime import date, timedelta, datetime
from dateutil.parser import *
import time
from lxml import etree 
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument
from nameparser import HumanName

NAME = 'mit'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}
OAI_DC_BASE_URL = 'http://dspace.mit.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from='

def consume(days_back=1):

    start_date = str(date.today() - timedelta(days_back))
    start_date = TODAY - timedelta(days_back)
    # YYYY-MM-DD hh:mm:ss
    url = OAI_DC_BASE_URL + str(start_date)

    records = get_records(url)

    xml_list = []
    json_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text
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
        base_url = 'http://dspace.mit.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&resumptionToken='
        url = base_url + token[0]
        records += get_records(url)

    return records


def getcontributors(result):
    dctype = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    contributors = result.xpath('//dc:contributor/node()', namespaces=NAMESPACES) or []
    creators = result.xpath('//dc:creator/node()', namespaces=NAMESPACES) or []
    if 'hesis' not in dctype and 'issertation' not in dctype:
        all_contributors = contributors + creators
    else:
        all_contributors = creators

    contributor_list = []
    for person in all_contributors:
        name = HumanName(person)
        contributor = {
            "prefix": name.title,
            "given": name.first,
            "middle": name.middle,
            "family": name.last,
            "suffix": name.suffix,
            "email": "",
            "ORCID": "",
            }
        contributor_list.append(contributor)
    return contributor_list

def gettags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    for tag in tags:
        tag = tag.lower()
    return tags

def getabstract(result):
    abstract = result.xpath('//dc:description/node()', namespaces=NAMESPACES) or ['No abstract']
    return abstract[0]

def getids(result):
    service_id = result.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    identifiers += result.xpath('//dc:relation/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for item in identifiers:
        if 'hdl.handle.net' in item:
            url = item
        if 'dx.doi.org' in item:
            doi = item.replace('http://dx.doi.org/', '')
            if url == '':
                url = item

    if url == '':
        raise Exception('Warning: No url provided!')

    return {'service_id': service_id, 'url': url, 'doi': doi}

def get_date_created(result):
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES)
    date_list = []
    DEFAULT = datetime(2010, 01, 01)
    for item in dates:
        try:
            a_date = parse(str(item)[:10], yearfirst=True).isoformat()
        except ValueError:
            try:
                a_date = parse(str(item)[:10], yearfirst=True).isoformat()
            except ValueError:
                try:
                    a_date = parse(str(item)[:10], yearfirst=True).isoformat()
                except ValueError:
                    try:
                        a_date = parse(str(item)[:10], yearfirst=True).isoformat()
                    except ValueError:
                        #import pdb; pdb.set_trace()
                        a_date = parse(str(item)[:10], yearfirst=True,  default=DEFAULT).isoformat()
        date_list.append(a_date)
    min_date = min(date_list)
    return min_date

def normalize(raw_doc, timestamp):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print "Error in namespaces! Skipping this one..."
        return None

    title = result.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]
    rights = result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or ['']
    ids = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or ['']
    ids += result.xpath('//dc:relation/node()', namespaces=NAMESPACES)
    identifiers = []
    for identifier in ids:
        if 'http://' not in identifier:
            identifiers.append(identifier)
    source = (result.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    language = (result.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0]
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    dcformat = result.xpath('//dc:format/node()', namespaces=NAMESPACES) or []
    dateupdated = result.xpath('//ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    dateupdated = parse(dateupdated).isoformat()

    payload = {
        "title": title,
        "contributors": getcontributors(result),
        "properties": {
            "permissions": {
                "copyrightStatement": rights[0],
            },
            "identifiers": identifiers,
            "publisherInfo": {
                "publisher": publisher,
            },
            "format": dcformat,
            "source": source,
            "language": language,
        },
        "description": getabstract(result),
        "tags": gettags(result),
        "id": getids(result),
        "source": NAME,
        "dateUpdated": dateupdated,
        "date_created": get_date_created(result),
        "timestamp": str(timestamp),
    }

    import json
    print(json.dumps(payload, indent=4))
    return NormalizedDocument(payload)

if __name__ == '__main__':
    print(lint(consume, normalize))
