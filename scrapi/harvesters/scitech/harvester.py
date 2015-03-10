"""
A harvester for the DoE's SciTech Connect Database. Makes use of SciTech's XML Querying Service. Parses the resulting XML for information and outputs the result as Json in a format that works with the OSF scrapi (scraper API).

Example API query: http://www.osti.gov/scitech/scitechxml?EntryDateFrom=02%2F02%2F2015&page=0
"""


from __future__ import unicode_literals

import re
import datetime

from lxml import etree

from nameparser import HumanName

from dateutil.parser import *

from scrapi import requests
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'scitech'
terms_url = 'http://purl.org/dc/terms/'
elements_url = 'http://purl.org/dc/elements/1.1/'

DEFAULT_ENCODING = 'UTF-8'

record_encoding = None


def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def harvest(days_back=1, end_date=None, **kwargs):
    """A function for querying the SciTech Connect database for raw XML.
    The XML is chunked into smaller pieces, each representing data
    about an article/report. If there are multiple pages of results,
    this function iterates through all the pages."""

    TODAY = datetime.date.today()
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
        xml = requests.get(base_url, params=parameters)
        record_encoding = xml.encoding
        xml = xml.text
        xml_root = etree.XML(xml.encode('utf-8'))
        for record in xml_root.find('records'):
            doc_id = record.find(str(etree.QName(elements_url, 'ostiId'))).text,
            xml_list.append(RawDocument({
                'doc': etree.tostring(record, encoding=record_encoding),
                'docID': copy_to_unicode(doc_id),
                'source': NAME,
                'filetype': 'xml'
            }))
        parameters['page'] += 1
        morepages = xml_root.find('records').attrib['morepages']
    return xml_list


def get_ids(record, raw_doc):
    url = record.find(str(etree.QName(terms_url, 'identifier-citation'))).text or \
        record.find(str(etree.QName(terms_url, 'identifier-purl'))).text or \
        record.find(str(etree.QName(terms_url, 'publisherAvailability'))).text or ''
    url = copy_to_unicode(url)
    doi = record.find(str(etree.QName(elements_url, 'doi'))).text or ''

    ids = {
        'serviceID': raw_doc.get('docID'),
        'doi': copy_to_unicode(doi),
        'url': url
    }
    return ids


def get_properties(record):
    # TODO - some of these record.finds return a FutureWarning - should be fixed
    properties = {
        'articleType': record.find(str(etree.QName(elements_url, 'type'))).text or '',
        'dateEntry': record.find(str(etree.QName(elements_url, 'dateEntry'))).text or '',
        'publisherResearch': record.find(str(etree.QName(terms_url, 'publisherResearch'))).text or '',
        'publisherSponsor': record.find(str(etree.QName(terms_url, 'publisherSponsor'))).text or '',
        'publisherCountry': record.find(str(etree.QName(terms_url, 'publisherCountry'))).text or '',
        'identifier': record.find(str(etree.QName(elements_url, 'identifier'))).text or "",
        'identifierReport': record.find(str(etree.QName(elements_url, 'identifierReport'))).text or "",
        'identifierContract': record.find(str(etree.QName(terms_url, 'identifierDOEcontract'))) or "",
        'identifierCitation': record.find(str(etree.QName(terms_url, 'identifier-citation'))) or "",
        'identifierOther': record.find(str(etree.QName(elements_url, 'identifierOther'))) or "",
        'relation': record.find(str(etree.QName(elements_url, 'relation'))).text or "",
        'coverage': record.find(str(etree.QName(elements_url, 'coverage'))).text or "",
        'format': record.find(str(etree.QName(elements_url, 'format'))).text or "",
        'language': record.find(str(etree.QName(elements_url, 'language'))).text or "",
        'date': record.find(str(etree.QName(elements_url, 'date'))).text or "",
        'type': record.find(str(etree.QName(elements_url, 'type'))).text or "",
        'rights': record.find(str(etree.QName(elements_url, 'rights'))).text or "",
        'dateAdded': record.find(str(etree.QName(elements_url, 'dateAdded'))).text or ""

    }
    for key, value in properties.iteritems():
        if isinstance(value, etree._ElementStringResult):
            properties[key] = copy_to_unicode(value)

    return properties


def get_tags(record):
    # TODO - filter out some of the tags that aren't tags but paragraphs of stuff?
    tags = record.find(str(etree.QName(elements_url, 'subject'))).text
    tags = re.split(',(?!\s\&)|;', tags) if tags is not None else []
    return [copy_to_unicode(tag.strip().lower()) for tag in tags]


def get_contributors(record):
    contributors = record.find(str(etree.QName(elements_url, 'creator'))).text.split(';') or ['']
    # TODO for now, scitech does not grab emails, but it could soon?
    # TODO some names grabbed are names of Universities - fix this...
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


def get_date_updated(record):
    date_updated = record.find(str(etree.QName(elements_url, 'dateEntry'))).text
    date = parse(date_updated).isoformat()
    return copy_to_unicode(date)


def normalize(raw_doc):
    """A function for parsing the list of XML objects returned by the
    harvest function.
    Returns a list of Json objects in a format that can be recognized
    by the OSF scrapi."""
    raw_doc_str = raw_doc.get('doc')
    elements_url = 'http://purl.org/dc/elements/1.1/'
    record = etree.XML(raw_doc_str)

    title = record.find(str(etree.QName(elements_url, 'title'))).text
    description = record.find(str(etree.QName(elements_url, 'description'))).text or ''

    normalized_dict = {
        'title': copy_to_unicode(title),
        'description': copy_to_unicode(description),
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'id': get_ids(record, raw_doc),
        'source': NAME,
        'dateUpdated': get_date_updated(record),
        'tags': get_tags(record)
    }

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(harvest, normalize))
