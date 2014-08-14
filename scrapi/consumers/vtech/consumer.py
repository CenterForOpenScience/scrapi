# Refactored consumer for VTechWorks

from sickle import Sickle 
from sickle.oaiexceptions import NoRecordsMatch
# from sickle.models import Record
import xmltodict
import dicttoxml
from datetime import date, timedelta

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = 'vtechworks'

def consume(days_back=3):
    doc_list = []
    start_date = str(date.today() - timedelta(days_back))
    sickle = Sickle('http://vtechworks.lib.vt.edu/oai/request')
    try:
        records = sickle.ListRecords(
            **{ 'metadataPrefix': 'oai_dc',
            'from': start_date,
            'ignore_deleted': 'True'})
    except NoRecordsMatch:
        records = []
        print "No new files or updates!"

    # TODO: This assumes that an empty records collection throws a `KeyError`. This should be verified.
    try:
        for record in records:
            doc = xmltodict.parse(record.raw)

            try:
                doc_list.append(doc['record']) # should grab the header AND metadata, skip XML version
            except KeyError:
                pass
    except KeyError:
            print "No new files/updates!"

    return_list = []
    for x in range(0, len(doc_list)):
        doc_xml = doc_list[x]
        doc_id = doc_xml['header']['identifier']
        return_list.append(RawDocument({
            'doc': dicttoxml.dicttoxml(doc_list[x]), 
            'source': NAME, 
            'doc_id': doc_id, 
            'filetype': 'xml'}))

    return return_list


def getcontributors(result):
    try:
        thelist = result['root']['metadata']['oai_dc:dc']['dc:contributor']['item']
        thelistcomprehended = [ item['#text'] for item in thelist ]
    except KeyError:
        thelistcomprehended = ['no contributors']
    return thelistcomprehended

def gettags(result):
    try:
        tag_list = result['root']['metadata']['oai_dc:dc']['dc:subject']['item']
        tag_text = [tag['#text'] for tag in tag_list]
    except KeyError:
        tag_text = []
    return tag_text

def getabstract(result):
    try:
        abstract = result['root']['metadata']['oai_dc:dc']['key'][7]['#text']
    except KeyError:
        abstract = 'No description available.'
    return abstract

def getids(result):
    service_id = result['root']['header']['identifier']['#text']
    identifiers = result['root']['metadata']['oai_dc:dc']['dc:identifier']['item']
    for item in identifiers:
        if 'http://' in item['#text']:
            url = item['#text']

    url = url or ''

    return {'service_id': service_id, 'doi': '', 'url': url}

def normalize(raw_doc, timestamp):
    result = raw_doc.get('doc')
    result = xmltodict.parse(result)
    date_created = result['root']['metadata']['oai_dc:dc']['dc:date']['item'][2]['#text']
    contributors = [result['root']['metadata']['oai_dc:dc']['key'][6]['#text']] + getcontributors(result)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor, 'email': ''})
    
    payload = {
        'title': result['root']['metadata']['oai_dc:dc']['key'][5]['#text'],
        'contributors': contributor_list,
        'properties': {},
        'description': getabstract(result),
        'tags': gettags(result),
        'meta': {},
        'id': getids(result),
        'source': NAME,
        'date_created': date_created,
        'timestamp': str(timestamp)
    }

    print payload
    return NormalizedDocument(payload)


if __name__ == '__main__':
    print(lint(consume, normalize))
