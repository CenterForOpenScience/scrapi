## Consumer and Normalizer for the eScholarship Repo 
## at the University of California

# import requests
from lxml import etree
from xml.etree import ElementTree
from datetime import date, timedelta
from scrapi_tools.consumer import BaseConsumer, RawFile, NormalizedFile
import requests

TODAY = date.today()
YESTERDAY = TODAY - timedelta(1)


class EScholarshipConsumer(BaseConsumer):

    def __init__(self):
        # what do?
        pass

    def consume(self):

        base_url = 'http://www.escholarship.org/uc/oai?verb=ListRecords&metadataPrefix=oai_dc&from='
        url = base_url + str(YESTERDAY)
        data = requests.get(url)
        doc =  etree.XML(data.content)

        namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                    'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
                    'ns0': 'http://www.openarchives.org/OAI/2.0/'}

        records = doc.xpath('//oai_dc:record', namespaces=namespaces)

        print 'num records: ' + str(len(records))
        # print 'num titles: ' + str(len(titles))

        xml_list = []
        for record in records:
            doc_id = record.xpath('ns0:header/ns0:identifier', 
                                    namespaces=namespaces)[0].text
            record = ElementTree.tostring(record)
            record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
            xml_list.append(RawFile({
                        'doc': record,
                        'source': 'eScholarship',
                        'doc_id': doc_id,
                        'filetype': 'xml'
                    }))

        return xml_list

    def normalize(self):
        pass


# if __name__ == '__main__':
#     consumer = EScholarshipConsumer()
#     print(consumer.lint())