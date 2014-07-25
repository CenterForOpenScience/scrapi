from scrapi_tools.consumer import BaseConsumer, RawFile, NormalizedFile
from lxml import etree
import requests
import datetime
import re

TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(1)


class SciTechConsumer(BaseConsumer):

    def __init__(self):
        # Nothing to see here
        pass

    def consume(self, start_date=YESTERDAY.strftime('%m/%d/%Y'), end_date=None, **kwargs):
        """A function for querying the SciTech Connect database for raw XML. The XML is chunked into smaller pieces, each representing data
        about an article/report. If there are multiple pages of results, this function iterates through all the pages."""

        base_url = 'http://www.osti.gov/scitech/scitechxml'
        parameters = kwargs
        parameters['EntryDateFrom'] = start_date
        parameters['EntryDateTo'] = end_date
        parameters['page'] = 0
        morepages = 'true'
        xml_list = []
        elements_url = 'http://purl.org/dc/elements/1.1/'

        while morepages == 'true':
            xml = requests.get(base_url, params=parameters).text
            xml_root = etree.XML(xml.encode('utf-8'))
            for record in xml_root.find('records'):
                xml_list.append(RawFile({
                    'doc': etree.tostring(record, encoding='ASCII'),
                    'source': 'SciTech',
                    'doc_id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
                    'filetype': 'xml',
                }))
            parameters['page'] += 1
            morepages = xml_root.find('records').attrib['morepages']
        return xml_list

    def normalize(self, raw_doc, timestamp):
        """A function for parsing the list of XML objects returned by the consume function.
        Returns a list of Json objects in a format that can be recognized by the OSF scrapi."""
        raw_doc = raw_doc.get('doc')
        terms_url = 'http://purl.org/dc/terms/'
        elements_url = 'http://purl.org/dc/elements/1.1/'
        record = etree.XML(raw_doc)

        contributor_list = record.find(str(etree.QName(elements_url, 'creator'))).text.split(';')
        # for now, scitech does not grab emails, but it could soon?
        contributors = []
        for name in contributor_list:
            name = name.strip()
            if name[0] in ['/', ',', 'et. al']:
                continue
            if '[' in name:
                name = name[:name.index('[')].strip()
            contributor = {}
            contributor['full_name'] = name
            contributor['email'] = None
            contributors.append(contributor)

            tags = record.find(str(etree.QName(elements_url, 'subject'))).text
            tags = re.split(',(?!\s\&)|;', tags) if tags is not None else []
            tags = [tag.strip() for tag in tags]

        return NormalizedFile({
            'title': record.find(str(etree.QName(elements_url, 'title'))).text,
            'contributors': contributors,
            'properties': {
                'doi': record.find(str(etree.QName(elements_url, 'doi'))).text,
                'description': record.find(str(etree.QName(elements_url, 'description'))).text,
                'article_type': record.find(str(etree.QName(elements_url, 'type'))).text,
                'url': record.find(str(etree.QName(terms_url, 'identifier-purl'))).text,
                'date_entered': record.find(str(etree.QName(elements_url, 'dateEntry'))).text,
                'research_org': record.find(str(etree.QName(terms_url, 'publisherResearch'))).text,
                'research_sponsor': record.find(str(etree.QName(terms_url, 'publisherSponsor'))).text,
                'tags': tags,
                'date_published': record.find(str(etree.QName(elements_url, 'date'))).text
            },
            'meta': {},
            'id': record.find(str(etree.QName(elements_url, 'ostiId'))).text,
            'source': 'SciTech',
            'timestamp': str(timestamp)
        })

if __name__ == '__main__':
    consumer = SciTechConsumer()
    print(consumer.lint())
