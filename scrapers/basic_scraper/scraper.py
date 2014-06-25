import sys

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.insert(1, '/home/fabian/cos/scrapi/')

import xmltodict

import api.process_docs as process_docs


def retrieve():
    with open('10.1371%2Fjournal.pbio.1001356.xml', 'r') as f:
        text = f.read()

    process_docs.process_raw(text, 'basic_scraper', '10.1371%2Fjournal.pbio.1001356', 'xml')
    parse(text)


def parse(text):
    doc = xmltodict.parse(text)
    formatted_doc = {
        'title': doc['doc']['str'][4]['#text'],
        'description': doc['doc']['arr'][1]['str'],
        'contributors': doc['doc']['arr'][0]['str'],
        'id': doc['doc']['str'][0]['#text'],
    }

    process_docs.process(formatted_doc)
retrieve()
