# Refactored consumer for VTechWorks

from sickle import Sickle 
from sickle.oaiexceptions import NoRecordsMatch
# from sickle.models import Record
import xmltodict
import dicttoxml
from datetime import date, timedelta

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

NAME = 'VTechWorks'

def consume(days_back=2):
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

# UNCOMMENT TO RUN TESTS IN NORMALIZE.PY
#        test_list.append(dicttoxml.dicttoxml(doc_list[x]))

#    with open('output.xml','w') as f:
#        f.write(test_list[1])

    return return_list


def getcontributors(result):
    try:
        thelist = result['root']['metadata']['oai_dc:dc']['dc:contributor']['item']
        thelistcomprehended = [ item['#text'] for item in thelist ]
    except KeyError:
        thelistcomprehended = ['no contributors']
    return thelistcomprehended

def getabstract(result):
    abstract = result['root']['metadata']['oai_dc:dc']['key'][7]['#text']
    return abstract

def getid(result):
    theid = result['root']['header']['identifier']['#text']
    return theid

def normalize(raw_doc, timestamp):
    result = raw_doc.get('doc')
    result = xmltodict.parse(result)
    contributors = [result['root']['metadata']['oai_dc:dc']['key'][6]['#text']] + getcontributors(result)
    contributor_list = []
    for contributor in contributors:
        contributor_list.append({'full_name': contributor, 'email': ''})
    payload = {
        'title': result['root']['metadata']['oai_dc:dc']['key'][5]['#text'],
        'contributors': contributor_list,
        'properties': {
            'abstract': getabstract(result)
        },
        'meta': {},
        'id': getid(result),
        'source': NAME,
        'timestamp': str(timestamp)
    }

    return NormalizedDocument(payload)


if __name__ == '__main__':
    print(lint(consume, normalize))
