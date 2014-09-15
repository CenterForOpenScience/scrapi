## Consumer for the CrossRef metadata service

import json
import requests
from datetime import date, timedelta

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = 'crossref'


def consume(days_back=0):
    base_url = 'http://api.crossref.org/works?filter=from-pub-date:'
    start_date = TODAY - timedelta(days_back)
    url = base_url + str(start_date) + ',until-pub-date:' + str(TODAY) + '&rows=1000'
    data = requests.get(url)
    doc = data.json()

    records = doc['message']['items']

    doc_list = []
    for record in records:
        doc_id = record['DOI'] or record['URL']
        doc_list.append(RawDocument({
                    'doc': json.dumps(record),
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return doc_list

def normalize(raw_doc, timestamp):
    doc = raw_doc.get('doc')
    doc = json.loads(doc)

    # contributors
    contributor_list = []
    # import pdb; pdb.set_trace()
    try:
        contributors = doc['author']
    except KeyError:
        contributors = [{'given': 'no', 'family': 'authors'}]
    for contributor in contributors:
        try:
            first = contributor.get('given').encode('utf-8') or ''
            last = contributor.get('family').encode('utf-8') or ''
        except AttributeError:
            first = ''
            last = ''
        full_name = '{0} {1}'.format(first, last)
        contributor_list.append({'full_name': full_name, 'email': ''})

    # ids
    ids = {}
    ids['url'] = doc.get('URL')
    if ids['url'] == None:
        raise Exception('Warning: No URL provided...')
    ids['doi'] = doc.get('DOI')
    ids['service_id'] = doc.get('prefix')


    normalized_dict = {
        'title': (doc.get('title') or ['No title'])[0],
        'contributors': contributor_list,
        'properties': {
                'published-in': {
                    'journal-title': doc.get('container-title'),
                    'volume': doc.get('volume'),
                    'issue': doc.get('issue')
                },
                'publisher': doc.get('publisher'),
                'type' : doc.get('type'),
                'ISSN': doc.get('ISSN'),
                'ISBN': doc.get('ISBN'),
                'member': doc.get('member'),
                'score': doc.get('score'),
                'issued': doc.get('issued'),
                'deposited': doc.get('deposited'),
                'indexed': doc.get('indexed'),
                'reference-count': doc.get('reference-count'),
                'update-policy': doc.get('update-policy')
        },
        'description': (doc.get('subtitle') or [''])[0],
        'meta': {},
        'id': ids,
        'source': NAME,
        'tags': doc.get('subject') or [],
        'date_created': 'date_created',
        'timestamp': str(timestamp)
    }
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
