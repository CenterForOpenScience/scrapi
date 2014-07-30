## Consumer and Normalizer for the eScholarship Repo 
## at the University of California

# import requests
from lxml import etree, html
from datetime import date, timedelta
from scrapi_tools.consumer import BaseConsumer, RawFile, NormalizedFile


TODAY = date.today()
YESTERDAY = TODAY - timedelta(1)

class EScholarshipConsumer(BaseConsumer):

    def __init__(self):
        # what do?
        pass

    def consume(self):

        base_url = 'http://www.escholarship.org/uc/oai?verb=ListRecords&metadataPrefix=oai_dc&from='
        url = base_url + str(YESTERDAY)
        # response = requests.get(url)
        # xml_text = response.content

        doc = html.parse(url)

        namespaces = {'dc': 'http://purl.org/dc/elements/1.1/'}

        # records = doc.findall('OAI-PMH/ListRecords/record')
        records = doc.findall('//record')
        titles = doc.findall('//title', namespaces=namespaces)

        print 'num records: ' + str(len(records))
        print 'num titles: ' + str(len(titles))

        for record in records:
            print type(record.text)
            print str(record.find('title', namespaces=namespaces))
            print record.text

        # for title in titles:
        #     print title.text

        # for record in records:
        #     print record.text

        # xml_list = []

        # xml_list.append(RawFile({
        #                 'doc': content.text,
        #                 'source': 'eScholarship',
        #                 'doc_id': doc_id,
        #                 'filetype': 'xml',
        #             }))

## OAI-PMH
## ListRecords
## record


    def normalize(self):
        pass

EScholarshipConsumer().consume()

# if __name__ == '__main__':
#     consumer = EScholarshipConsumer()
#     print(consumer.lint())