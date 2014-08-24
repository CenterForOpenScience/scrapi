## Consumer for the CrossRef metadata service

import requests
from lxml import etree
from datetime import date, timedelta

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = 'crossref'


def consume(days_back=0):
    base_url = 'http://api.crossref.org/works?filter=from-pub-date:'
    start_date = TODAY - timedelta(days_back)
    url = base_url + str(start_date) + ',until-pub-date:' + str(TODAY) + '&rows=1000'
    print url
    data = requests.get(url)
    doc = data.json()

    records = doc['message']['items']

    # import pdb; pdb.set_trace()

    doc_list = []
    for record in records:
        doc_id = record['DOI'] or record['URL']
        doc_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return doc_list

def normalize(raw_doc, timestamp):
    doc = raw_doc.get('doc')

    # contributors
    contributor_list = []
    # import pdb; pdb.set_trace()
    try:
        contributors = doc['author']
    except KeyError:
        contributors = [{'given': 'no', 'family': 'authors'}]
    for contributor in contributors:
        full_name = (contributor.get('given')) or '' + ' '
        full_name += contributor.get('family') or ''
        contributor_list.append({'full_name': full_name, 'email': ''})

    # ids
    ids = {}
    ids['url'] = doc.get('URL')
    ids['doi'] = doc.get('DOI')
    ids['service_id'] = doc.get('prefix')

    # tags

    # date created

    normalized_dict = {
        'title': doc.get('title'),
        'contributors': contributor_list,
        'properties': {
                'publisher': doc.get('publisher'),
                'type' : doc.get('type'),
                'ISSN': doc.get('ISSN'),
                'ISBN': doc.get('ISBN'),
                'member': doc.get('member'),
                'score': doc.get('score'),
                'issued': doc.get('issued'),
                'deposited': doc.get('deposited'),
                'indexed': doc.get('indexed'),
                'reference-count': doc.get('reference-count') 
        },
        'description': doc.get('subtitle'),
        'meta': {},
        'id': ids,
        'source': NAME,
        'tags': ['some', 'tags'],
        'date_created': 'date_created',
        'timestamp': str(timestamp)
    }

    print normalized_dict
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
