
import re
import requests
import datetime

from lxml import etree

from nameparser import HumanName

from dateutil.parser import *

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(1)
NAME = 'scitech'
terms_url = 'http://purl.org/dc/terms/'
elements_url = 'http://purl.org/dc/elements/1.1/'


def consume(days_back=1, end_date=None, **kwargs):
    """A function for querying the SciTech Connect database for raw XML. 
    The XML is chunked into smaller pieces, each representing data
    about an article/report. If there are multiple pages of results, 
    this function iterates through all the pages."""

    start_date = (TODAY - datetime.timedelta(days_back)).strftime('%m/%d/%Y')
    base_url = 'http://www.osti.gov/scitech/scitechxml'
    parameters = kwargs
    parameters['EntryDateFrom'] = start_date
    parameters['EntryDateTo'] = end_date
    parameters['page'] = 0
    morepages = 'true'
    xml_list = []
    elements_url = 'http://purl.org/dc/elements/1.1/'

    while morepages == 'true':
        xml = requests.get(base_url, params=parameters)  #.text
        xml = xml.text
        xml_root = etree.XML(xml.encode('utf-8'))
        for record in xml_root.find('records'):
            xml_list.append(RawDocument({
                'doc': etree.tostring(record, encoding='ASCII'),
                'source': NAME,
                'docID': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
                'filetype': 'xml',
            }))
        parameters['page'] += 1
        morepages = xml_root.find('records').attrib['morepages']
    return xml_list

def get_ids(record, raw_doc):
    url = record.find(str(etree.QName(terms_url, 'identifier-citation'))).text or \
        record.find(str(etree.QName(terms_url, 'identifier-purl'))).text or \
        record.find(str(etree.QName(terms_url, 'publisherAvailability'))).text

    ids =  {
        'serviceID': raw_doc.get('docID'),
        'doi': record.find(str(etree.QName(elements_url, 'doi'))).text or '',
        'url': url or '',
    }
    return ids

def get_properties(record):
    # TODO - some of these record.finds return a FutureWarning - should be fixed
    properties = {
        'articleType': record.find(str(etree.QName(elements_url, 'type'))).text or '',
        'dateEntered': record.find(str(etree.QName(elements_url, 'dateEntry'))).text or '',
        'researchOrg': record.find(str(etree.QName(terms_url, 'publisherResearch'))).text or '',
        'researchSponsor': record.find(str(etree.QName(terms_url, 'publisherSponsor'))).text or '',
        'researchCountry': record.find(str(etree.QName(terms_url, 'publisherCountry'))).text or '',
        'identifierInfo': {
            'identifier': record.find(str(etree.QName(elements_url, 'identifier'))).text or "",
            'identifierReport': record.find(str(etree.QName(elements_url, 'identifierReport'))).text or "",
            'identifierContract': record.find(str(etree.QName(terms_url, 'identifierDOEcontract'))) or "",
            'identifierCitation': record.find(str(etree.QName(terms_url, 'identifier-citation'))) or "",
            'identifierOther': record.find(str(etree.QName(elements_url, 'identifierOther'))) or ""
        },
        'relation': record.find(str(etree.QName(elements_url, 'relation'))).text or "",
        'coverage': record.find(str(etree.QName(elements_url, 'coverage'))).text or "",
        'format': record.find(str(etree.QName(elements_url, 'format'))).text or "",
        'language': record.find(str(etree.QName(elements_url, 'language'))).text or ""
    }
    return properties

def get_tags(record):
    # TODO - filter out some of the tags that aren't tags but paragraphs of stuff?
    tags = record.find(str(etree.QName(elements_url, 'subject'))).text
    tags = re.split(',(?!\s\&)|;', tags) if tags is not None else []
    tags = [tag.strip().lower() for tag in tags]
    return tags

def get_contributors(record):
    contributors = record.find(str(etree.QName(elements_url, 'creator'))).text.split(';') or ['']
    # for now, scitech does not grab emails, but it could soon?
    contributor_list = []
    for person in contributors: 
        if person != 'none,' and person != 'None':
            person = person.strip()
            if person[0] in ['/', ',', 'et. al']:
                continue
            if '[' in person:
                person = person[:person.index('[')].strip()
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

def get_date_created(record):
    date_created = record.find(str(etree.QName(elements_url, 'date'))).text
    return parse(date_created).isoformat()

def get_date_updated(record):
    date_updated = record.find(str(etree.QName(elements_url, 'dateEntry'))).text
    return parse(date_updated).isoformat()

def normalize(raw_doc, timestamp):
    """A function for parsing the list of XML objects returned by the 
    consume function.
    Returns a list of Json objects in a format that can be recognized 
    by the OSF scrapi."""
    raw_doc_str = raw_doc.get('doc')
    terms_url = 'http://purl.org/dc/terms/'
    elements_url = 'http://purl.org/dc/elements/1.1/'
    record = etree.XML(raw_doc_str)

    normalized_dict = {
        'title': record.find(str(etree.QName(elements_url, 'title'))).text,
        'description': record.find(str(etree.QName(elements_url, 'description'))).text or '',
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'id': get_ids(record, raw_doc),
        'source': NAME,
        'timestamp': str(timestamp),
        'dateCreated': get_date_created(record),
        'dateUpdated' : get_date_updated(record),
        'tags': get_tags(record),
    }
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
