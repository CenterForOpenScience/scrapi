"""PLoS-API-harvester
=================
<p>To run "harvester.py" please follow the instructions:</p>
<ol>

<li>Create an account on <a href="http://register.plos.org/ambra-registration/register.action">PLOS API</a></li>

<li>Sign in <a href="http://alm.plos.org/">here</a> and click on your account name. Retrieve your API key.</li>

<li>Create a new file in the folder named "settings.py". In the file, put<br>
<code>API_KEY = (your API key)</code></li>

</ol>

Sample API query: http://api.plos.org/search?q=publication_date:[2015-01-30T00:00:00Z%20TO%202015-02-02T00:00:00Z]&api_key=ayourapikeyhere&rows=999&start=0
"""


from __future__ import unicode_literals

from datetime import datetime, timedelta

from lxml import etree
from dateutil.parser import *
from nameparser import HumanName

from scrapi import requests
from scrapi.linter.document import RawDocument, NormalizedDocument

try:
    from settings import PLOS_API_KEY
except ImportError:
    from scrapi.settings import PLOS_API_KEY


NAME = 'plos'

DEFAULT_ENCODING = 'UTF-8'

record_encoding = None


class PlosHarvester(object):
    MAX_ROWS_PER_REQUEST = 999
    BASE_URL = 'http://api.plos.org/search?q=publication_date:'

    def __init__(self):
        assert PLOS_API_KEY, 'PLoS requires an API key'

    def build_query(self, days_back):
        to_date = datetime.utcnow()
        from_date = (datetime.utcnow() - timedelta(days=days_back))

        to_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0)
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        return 'publication_date:[{}Z TO {}Z]'.format(from_date.isoformat(), to_date.isoformat())

    def fetch_rows(self, days_back):
        query = self.build_query(days_back)

        resp = requests.get(self.BASE_URL, params={
            'q': query,
            'rows': '0',
            'api_key': PLOS_API_KEY,
        })

        total_rows = int(etree.XML(resp.content).xpath('//result/@numFound')[0])

        current_row = 0
        while current_row < total_rows:
            response = requests.get(self.BASE_URL, throttle=5, params={
                'q': query,
                'start': current_row,
                'api_key': PLOS_API_KEY,
                'rows': self.MAX_ROWS_PER_REQUEST,
            })

            for doc in etree.XML(response.content).xpath('//doc'):
                yield doc

            current_row += self.MAX_ROWS_PER_REQUEST

    def harvest(self, days_back=3):
        return [
            RawDocument({
                'doc': etree.tostring(row),
                'source': NAME,
                'filetype': 'xml',
                'docID': row.xpath("str[@name='id']")[0].text.decode('utf-8'),
            })
            for row in
            self.fetch_rows(days_back)
            if row.xpath("arr[@name='abstract']")
            or row.xpath("str[@name='author_display']")
        ]


def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def get_ids(raw_doc, record):
    doi = record.xpath('//str[@name="id"]/node()')[0]
    ids = {
        'doi': copy_to_unicode(doi),
        'serviceID': raw_doc.get('docID'),
        'url': 'http://dx.doi.org/{}'.format(doi)
    }
    return ids


def get_contributors(record):
    contributor_list = []
    contributors = record.xpath('//arr[@name="author_display"]/str/node()') or ['']
    for person in contributors:
        name = HumanName(person)
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': '',
            'ORCID': ''
        }
        contributor_list.append(contributor)
    return contributor_list


def get_properties(record):
    properties = {
        'journal': (record.xpath('//str[@name="journal"]/node()') or [''])[0],
        'eissn': (record.xpath('//str[@name="eissn"]/node()') or [''])[0],
        'articleType': (record.xpath('//str[@name="article_type"]/node()') or [''])[0],
        'score': (record.xpath('//float[@name="score"]/node()') or [''])[0],
    }

    # ensure everything is in unicode
    for key, value in properties.iteritems():
        properties[key] = copy_to_unicode(value)

    return properties


def get_date_updated(record):
    date_created = (record.xpath('//date[@name="publication_date"]/node()') or [''])[0]
    date = parse(date_created).isoformat()
    return copy_to_unicode(date)


# No tags...
def get_tags(record):
    return []


def normalize(raw_doc):
    raw_doc_string = raw_doc.get('doc')
    record = etree.XML(raw_doc_string)

    title = record.xpath('//str[@name="title_display"]/node()')[0]
    description = (record.xpath('//arr[@name="abstract"]/str/node()') or [''])[0],

    normalized_dict = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(record),
        'description': copy_to_unicode(description),
        'properties': get_properties(record),
        'id': get_ids(raw_doc, record),
        'source': NAME,
        'dateUpdated': get_date_updated(record),
        'tags': get_tags(record)
    }

    # deal with Corrections having "PLoS Staff" listed as contributors
    # fix correction title
    if normalized_dict['properties']['articleType'] == 'Correction':
        normalized_dict['title'] = normalized_dict['title'].replace('Correction: ', '')
        normalized_dict['contributors'] = [{
            'prefix': '',
            'given': '',
            'middle': '',
            'family': '',
            'suffix': '',
            'email': '',
            'ORCID': ''
        }]
    # import json; print(json.dumps(normalized_dict, indent=4))
    return NormalizedDocument(normalized_dict)

harvest = PlosHarvester().harvest
